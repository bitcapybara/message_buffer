#![allow(dead_code)]

use std::{future::Future, sync::Arc, time::Duration};

use tokio::{
    sync::mpsc::{
        self,
        error::{TryRecvError, TrySendError},
    },
    task::JoinHandle,
    time::timeout,
};

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

/// TODO backoff setting, used in retry
/// fn next_duration()
/// fn reset()
// struct BackOff {}

struct BatchConf {
    batch_size: usize,
    timeout: Duration,
}

impl BatchConf {
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
struct Batcher(Option<BatchConf>);

impl Batcher {
    /// if timeout elapsed, return Ok(Vec), vec may be empty
    /// if queue closed, return Err(Stopped)
    async fn poll<T>(&self, queue: &mut mpsc::Receiver<T>) -> Result<Vec<T>, MessageBufferError> {
        match self.0 {
            Some(BatchConf {
                batch_size,
                timeout,
            }) => Self::poll_batch(batch_size, timeout, queue).await,
            None => todo!(),
        }
    }

    async fn poll_one<T>(queue: &mut mpsc::Receiver<T>) -> Result<T, MessageBufferError> {
        match queue.recv().await {
            Some(msg) => Ok(msg),
            None => Err(MessageBufferError::Stopped),
        }
    }

    async fn poll_batch<T>(
        bsz: usize,
        tmot: Duration,
        queue: &mut mpsc::Receiver<T>,
    ) -> Result<Vec<T>, MessageBufferError> {
        let mut msgs = Vec::with_capacity(bsz);
        match timeout(tmot, async {
            while msgs.len() < bsz {
                match queue.recv().await {
                    Some(msg) => msgs.push(msg),
                    None => return Err(MessageBufferError::Stopped),
                }
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
    batch: Option<BatchConf>,
    rcv_err: bool,
    cap: usize,
}

impl OptionsBuilder {
    fn batch(mut self, batch_size: usize, timeout: Duration) -> Self {
        self.batch = Some(BatchConf::new(batch_size, timeout));
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

struct MessageBuffer<T, E> {
    main_tx: mpsc::Sender<T>,
    error_rx: Option<mpsc::Receiver<E>>,
    handle: JoinHandle<Result<(), MessageBufferError>>,
}

impl<T: Send + 'static, E: Send + 'static> MessageBuffer<T, E> {
    fn with_capacity<F, Fut>(cb: F, options: Options) -> Self
    where
        F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Operation, E>> + Send + 'static,
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
            main_tx,
            handle,
            error_rx,
        }
    }

    async fn push(&self, msg: T) -> Result<(), MessageBufferError> {
        Ok(self.main_tx.try_send(msg)?)
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
struct Item<T>(T, usize);

enum Operation {
    /// send msg to retry queue
    Retry,
    /// drop the msg, no need to retry
    Drop,
}

struct Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Operation, E>> + Send + 'static,
    E: Send + 'static,
{
    cb: Arc<F>,
    main_rx: mpsc::Receiver<T>,
    error_tx: Option<mpsc::Sender<E>>,
    batcher: Batcher,
}

impl<F, Fut, T, E> Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Operation, E>> + Send + 'static,
    E: Send + 'static,
{
    fn new(
        batcher: Batcher,
        cb: F,
        main_rx: mpsc::Receiver<T>,
        error_tx: Option<mpsc::Sender<E>>,
    ) -> Self {
        Self {
            cb: Arc::new(cb),
            main_rx,
            error_tx,
            batcher,
        }
    }

    async fn start(mut self) -> Result<(), MessageBufferError> {
        let msgs = self
            .batcher
            .poll(&mut self.main_rx)
            .await?
            .into_iter()
            .map(|msg| Item(msg, 0))
            .collect::<Vec<Item<T>>>();

        match (self.cb.clone())(msgs).await {
            Ok(Operation::Drop) => {}
            Ok(Operation::Retry) => {}
            Err(e) => {
                if let Some(e_tx) = self.error_tx {
                    let _ = e_tx.try_send(e);
                }
            }
        }

        Ok(())
    }
}
