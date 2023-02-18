#![allow(dead_code)]

use std::{future::Future, mem, time::Duration};

use futures::StreamExt;
use tokio::{
    select,
    sync::mpsc::{self, error::TrySendError},
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
        select! {
            msg_res = main_rx.recv() => {
                msg_res.ok_or(MessageBufferError::Stopped)
            }
            ex_res = retry_q.next() => {
                ex_res.map(|e| e.into_inner()).ok_or(MessageBufferError::Stopped)
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
    fn with_capacity<F, Fut, B>(cb: F, backoff: B, options: Options) -> Self
    where
        F: Fn(&mut Messages<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        B: BackOff + Send + 'static,
    {
        let (main_tx, main_rx) = mpsc::channel(options.cap);
        let (mut error_tx, mut error_rx) = (None, None);
        if options.rcv_err {
            let (e_tx, e_rx) = mpsc::channel(1);
            error_tx = Some(e_tx);
            error_rx = Some(e_rx);
        }

        // start worker
        let worker = Worker::new(options.batcher, cb, backoff, main_rx, error_tx);
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

impl<T: Clone> Item<T> {
    fn retry(self) -> Self {
        Self {
            id: self.id,
            data: self.data.clone(),
            trys: self.trys + 1,
        }
    }
}

/// send msg to retry queue
/// duration to delay
pub struct Retry(usize);

trait BackOff {
    fn next(&mut self) -> Duration;
}

struct ConstantBackOff(Duration);

impl ConstantBackOff {
    fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl BackOff for ConstantBackOff {
    fn next(&mut self) -> Duration {
        self.0
    }
}

struct ExponentBackOff {
    /// used to calculate next duration
    current: Duration,
    /// must greater than 1
    factor: u32,
    /// max duration boundary
    max: Duration,
}

impl ExponentBackOff {
    fn new(init: Duration, factor: u32, max: Duration) -> Self {
        Self {
            current: init,
            factor,
            max,
        }
    }
}

impl BackOff for ExponentBackOff {
    fn next(&mut self) -> Duration {
        let mut next = self.current * self.factor;
        if next > self.max {
            next = self.max;
        }
        self.current = next;
        next
    }
}

struct Messages<T: Clone> {
    msgs: Vec<Item<T>>,
    retries: Vec<Item<T>>,
}

impl<T: Clone> Messages<T> {
    fn all(&mut self) -> Vec<Item<T>> {
        mem::take(&mut self.msgs)
    }
    fn retry(&mut self, msg: &Item<T>) {
        self.retries.push(msg.clone());
    }
}

impl<T: Clone> From<Vec<Item<T>>> for Messages<T> {
    fn from(msgs: Vec<Item<T>>) -> Self {
        Self {
            msgs,
            retries: Vec::new(),
        }
    }
}

struct Worker<F, Fut, T, E, B>
where
    F: Fn(&mut Messages<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    T: Clone,
    E: Send + 'static,
    B: BackOff + Send + 'static,
{
    cb: F,
    backoff: B,
    main_rx: mpsc::Receiver<Item<T>>,
    error_tx: Option<mpsc::Sender<E>>,
    batcher: Batcher,
    retry_q: DelayQueue<Item<T>>,
}

impl<F, Fut, T, E, B> Worker<F, Fut, T, E, B>
where
    F: Fn(&mut Messages<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    T: Clone,
    E: Send + 'static,
    B: BackOff + Send + 'static,
{
    fn new(
        batcher: Batcher,
        cb: F,
        backoff: B,
        main_rx: mpsc::Receiver<Item<T>>,
        error_tx: Option<mpsc::Sender<E>>,
    ) -> Self {
        Self {
            cb,
            backoff,
            main_rx,
            error_tx,
            batcher,
            retry_q: DelayQueue::new(),
        }
    }

    async fn start(mut self) -> Result<(), MessageBufferError> {
        loop {
            let mut msgs: Messages<T> = self
                .batcher
                .poll(&mut self.main_rx, &mut self.retry_q)
                .await?
                .into();

            match (self.cb)(&mut msgs).await {
                Ok(_) => msgs.retries.into_iter().for_each(|item| {
                    self.retry_q.insert(item.retry(), self.backoff.next());
                }),
                Err(e) => {
                    if let Some(e_tx) = &self.error_tx {
                        let _ = e_tx.try_send(e);
                    }
                }
            }
        }
    }
}
