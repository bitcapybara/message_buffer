#![allow(dead_code)]

use std::{collections::VecDeque, future::Future};

use tokio::{
    sync::mpsc::{
        self,
        error::{TryRecvError, TrySendError},
    },
    task::JoinHandle,
    time,
};

enum MessageBufferError {
    QueueFull,
    QueueClosed,
}

impl<T> From<TrySendError<T>> for MessageBufferError {
    fn from(e: TrySendError<T>) -> Self {
        match e {
            TrySendError::Full(_) => Self::QueueFull,
            TrySendError::Closed(_) => Self::QueueClosed,
        }
    }
}

impl From<TryRecvError> for MessageBufferError {
    fn from(e: TryRecvError) -> Self {
        match e {
            TryRecvError::Empty => unreachable!(),
            TryRecvError::Disconnected => Self::QueueClosed,
        }
    }
}

/// backoff setting
/// trait ???
/// fn next_duration()
/// fn reset()
struct BackOff {}

/// batch setting
/// timeout(duration, poll)
/// poll: while res.len() < size {
///        queue.recv().await
/// }
struct Batcher {
    size: usize,
    timeout: time::Duration,
}

struct MessageBuffer<T> {
    main_tx: mpsc::Sender<T>,
    handle: JoinHandle<()>,
}

impl<T: Send + 'static> MessageBuffer<T> {
    fn new<F, Fut, E>(cb: F, backoff: BackOff) -> (Self, mpsc::Receiver<E>)
    where
        F: Fn(Vec<Item<T>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Operation, E>> + Send + 'static,
        E: Send + 'static,
    {
        Self::with_capacity(1024, 50, cb, backoff)
    }

    fn with_capacity<F, Fut, E>(
        cap: usize,
        batch_size: usize,
        cb: F,
        backoff: BackOff,
    ) -> (Self, mpsc::Receiver<E>)
    where
        F: Fn(Vec<Item<T>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Operation, E>> + Send + 'static,
        E: Send + 'static,
    {
        let (main_tx, main_rx) = mpsc::channel(cap);
        let (error_tx, error_rx) = mpsc::channel(1);

        // start worker
        let worker = Worker::new(batch_size, cb, main_rx, error_tx, backoff);
        let handle = tokio::spawn(worker.start());

        (Self { main_tx, handle }, error_rx)
    }

    async fn push(&self, msg: T) -> Result<(), MessageBufferError> {
        Ok(self.main_tx.try_send(msg)?)
    }

    async fn stop(self) {
        drop(self.main_tx);
        let _ = self.handle.await;
    }
}

/// msg and exec count
/// 0 for new msg
struct Item<T>(T, usize);

enum Operation {
    /// send msg to retry queue
    Retry,
    /// drop the msg, no need to retry
    Drop,
}

struct Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Operation, E>> + Send + 'static,
    E: Send + 'static,
{
    cb: F,
    main_rx: mpsc::Receiver<T>,
    retry_q: VecDeque<T>,
    error_tx: mpsc::Sender<E>,
    bsz: usize,
    backoff: BackOff,
}

impl<F, Fut, T, E> Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Operation, E>> + Send + 'static,
    E: Send + 'static,
{
    fn new(
        batch_size: usize,
        cb: F,
        main_rx: mpsc::Receiver<T>,
        error_tx: mpsc::Sender<E>,
        backoff: BackOff,
    ) -> Self {
        Self {
            cb,
            main_rx,
            retry_q: VecDeque::with_capacity(batch_size),
            error_tx,
            bsz: batch_size,
            backoff,
        }
    }

    async fn start(mut self) {
        todo!()
    }

    async fn poll(&mut self, num: usize) -> Result<T, MessageBufferError> {
        let mut main_msgs = Vec::with_capacity(num);
        let mut retry_msgs = Vec::with_capacity(num);
        while retry_msgs.len() < num {
            match self.retry_q.pop_front() {
                Some(msg) => retry_msgs.push(msg),
                None => break,
            }
        }
        while main_msgs.len() + retry_msgs.len() < num {
            match self.main_rx.try_recv() {
                Ok(msg) => main_msgs.push(msg),
                Err(e) => match e {
                    TryRecvError::Empty => break,
                    TryRecvError::Disconnected => return Err(e)?,
                },
            }
        }
        todo!()
    }
}
