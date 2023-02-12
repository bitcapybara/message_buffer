#![allow(dead_code)]

use std::{collections::HashMap, future::Future, time::Duration};

use futures::StreamExt;
use tokio::{
    select,
    sync::mpsc::{
        self,
        error::{TryRecvError, TrySendError},
    },
    task::JoinHandle,
    time::timeout,
};
use tokio_util::time::DelayQueue;

enum MessageBufferError {
    QueueFull,
    Stopped,
}

impl<T> From<TrySendError<T>> for MessageBufferError {
    fn from(e: TrySendError<T>) -> Self {
        match e {
            TrySendError::Full(_) => Self::QueueFull,
            TrySendError::Closed(_) => Self::Stopped,
        }
    }
}

impl From<TryRecvError> for MessageBufferError {
    fn from(e: TryRecvError) -> Self {
        match e {
            TryRecvError::Empty => unreachable!(),
            TryRecvError::Disconnected => Self::Stopped,
        }
    }
}

struct Batch {
    batch_size: usize,
    timeout: Duration,
}

impl Batch {
    fn new(batch_size: usize, timeout: Duration) -> Self {
        Self {
            batch_size,
            timeout,
        }
    }
}

/// batch setting
/// timeout(duration, poll)
/// poll: while res.len() < size {
///        queue.recv().await
/// }
struct Batcher(Option<Batch>);

impl Batcher {
    /// if timeout elapsed, return Ok(Vec), vec may be empty
    /// if queue closed, return Err(Stopped)
    async fn poll<T: Clone>(
        &self,
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut DelayQueue<Item<T>>,
    ) -> Result<Vec<Item<T>>, MessageBufferError> {
        match self.0 {
            Some(Batch {
                batch_size,
                timeout,
            }) => Self::poll_batch(batch_size, timeout, main_rx, retry_q).await,
            None => Self::poll_one(main_rx, retry_q).await.map(|msg| vec![msg]),
        }
    }

    async fn poll_one<T: Clone>(
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut DelayQueue<Item<T>>,
    ) -> Result<Item<T>, MessageBufferError> {
        loop {
            select! {
                msg_res = main_rx.recv() => {
                    match msg_res {
                        Some(msg) => {
                            return Ok(msg);
                        }
                        None => {
                            return Err(MessageBufferError::Stopped);
                        }
                    }
                }
                ex_res = retry_q.next() => {
                    match ex_res {
                        Some(msg) => {
                            return Ok(msg.into_inner());
                        }
                        None => {
                            return Err(MessageBufferError::Stopped);
                        }
                    }
                }
            }
        }
    }

    async fn poll_batch<T: Clone>(
        bsz: usize,
        tmot: Duration,
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut DelayQueue<Item<T>>,
    ) -> Result<Vec<Item<T>>, MessageBufferError> {
        let mut msgs = Vec::with_capacity(bsz);
        match timeout(tmot, async {
            while msgs.len() < bsz {
                let msg = Self::poll_one(main_rx, retry_q).await?;
                msgs.push(msg);
            }
            Ok(())
        })
        .await
        {
            Ok(Ok(())) => Ok(msgs),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(msgs),
        }
    }
}

struct Options {
    batcher: Batcher,
    rcv_err: bool,
    cap: usize,
}

impl Options {
    fn builder() -> OptionsBuilder {
        OptionsBuilder {
            batch: None,
            rcv_err: false,
            cap: 1024,
        }
    }
}

struct OptionsBuilder {
    batch: Option<Batch>,
    rcv_err: bool,
    cap: usize,
}

impl OptionsBuilder {
    fn batch(mut self, batch_size: usize, timeout: Duration) -> Self {
        self.batch = Some(Batch::new(batch_size, timeout));
        self
    }

    fn need_recv_error(mut self) -> Self {
        self.rcv_err = true;
        self
    }

    fn capacity(mut self, cap: usize) -> Self {
        self.cap = cap;
        self
    }

    fn build(self) -> Options {
        Options {
            batcher: Batcher(self.batch),
            rcv_err: self.rcv_err,
            cap: self.cap,
        }
    }
}

struct MessageBuffer<T: Clone, E> {
    id_gen: usize,
    main_tx: mpsc::Sender<Item<T>>,
    error_rx: Option<mpsc::Receiver<E>>,
    handle: JoinHandle<Result<(), MessageBufferError>>,
}

impl<T: Clone + Send + 'static, E: Send + 'static> MessageBuffer<T, E> {
    fn with_capacity<F, Fut>(cb: F, options: Options) -> Self
    where
        F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ItemOp, E>> + Send + 'static,
    {
        let (main_tx, main_rx) = mpsc::channel(options.cap);
        let (mut error_tx, mut error_rx) = (None, None);
        if options.rcv_err {
            let (e_tx, e_rx) = mpsc::channel(1);
            error_tx = Some(e_tx);
            error_rx = Some(e_rx);
        }

        // start worker
        let worker = Worker::new(options.batcher, cb, main_rx, error_tx);
        let handle = tokio::spawn(worker.start());

        Self {
            id_gen: 0,
            main_tx,
            handle,
            error_rx,
        }
    }

    async fn push(&mut self, msg: T) -> Result<(), MessageBufferError> {
        self.id_gen += 1;
        Ok(self.main_tx.try_send(Item {
            id: self.id_gen,
            data: msg,
            trys: 0,
        })?)
    }

    async fn error_receiver(&mut self) -> Option<mpsc::Receiver<E>> {
        self.error_rx.take()
    }

    async fn stop(self) {
        drop(self.main_tx);
        let _ = self.handle.await;
    }
}

/// msg and exec count
/// 0 for new msg
#[derive(Clone)]
struct Item<T: Clone> {
    id: usize,
    data: T,
    trys: usize,
}

enum ItemOp {
    /// send msg to retry queue
    /// duration to delay
    Retry(usize, Duration),
    /// drop the msg, no need to retry
    Drop(usize),
}

struct Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ItemOp, E>> + Send + 'static,
    T: Clone,
    E: Send + 'static,
{
    cb: F,
    main_rx: mpsc::Receiver<Item<T>>,
    error_tx: Option<mpsc::Sender<E>>,
    batcher: Batcher,
    retry_q: DelayQueue<Item<T>>,
}

impl<F, Fut, T, E> Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ItemOp, E>> + Send + 'static,
    T: Clone,
    E: Send + 'static,
{
    fn new(
        batcher: Batcher,
        cb: F,
        main_rx: mpsc::Receiver<Item<T>>,
        error_tx: Option<mpsc::Sender<E>>,
    ) -> Self {
        Self {
            cb,
            main_rx,
            error_tx,
            batcher,
            retry_q: DelayQueue::new(),
        }
    }

    async fn start(mut self) -> Result<(), MessageBufferError> {
        let mut s = HashMap::new();
        loop {
            let msgs = self
                .batcher
                .poll(&mut self.main_rx, &mut self.retry_q)
                .await?;
            for msg in msgs.iter() {
                s.insert(msg.id, msg.clone());
            }

            match (self.cb)(msgs).await {
                Ok(ItemOp::Drop(id)) => {
                    s.remove(&id);
                }
                Ok(ItemOp::Retry(id, duration)) => {
                    if let Some(item) = s.get(&id) {
                        self.retry_q.insert(item.clone(), duration);
                    }
                }
                Err(e) => {
                    if let Some(e_tx) = &self.error_tx {
                        let _ = e_tx.try_send(e);
                    }
                }
            }
        }
    }
}
