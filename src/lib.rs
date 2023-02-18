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

pub enum MessageBufferError {
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

pub trait Processor<T: Clone, E> {
    type Future: Future<Output = Result<Vec<Item<T>>, E>>;

    fn call(&mut self, msgs: Messages<T>) -> Self::Future;
}

pub fn service_fn<F>(f: F) -> ServiceFn<F> {
    ServiceFn { f }
}

pub struct ServiceFn<F> {
    f: F,
}

impl<T, Fut, F, E> Processor<T, E> for ServiceFn<F>
where
    T: Clone,
    F: Fn(Messages<T>) -> Fut,
    Fut: Future<Output = Result<Vec<Item<T>>, E>>,
{
    type Future = Fut;

    fn call(&mut self, msgs: Messages<T>) -> Self::Future {
        (self.f)(msgs)
    }
}

pub struct Batch {
    batch_size: usize,
    timeout: Duration,
}

impl Batch {
    pub fn new(batch_size: usize, timeout: Duration) -> Self {
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

pub struct Options {
    batcher: Batcher,
    rcv_err: bool,
    cap: usize,
}

impl Options {
    pub fn builder() -> OptionsBuilder {
        OptionsBuilder {
            batch: None,
            rcv_err: false,
            cap: 1024,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            batcher: Batcher(None),
            rcv_err: false,
            cap: 1024,
        }
    }
}

pub struct OptionsBuilder {
    batch: Option<Batch>,
    rcv_err: bool,
    cap: usize,
}

impl OptionsBuilder {
    pub fn batch(mut self, batch_size: usize, timeout: Duration) -> Self {
        self.batch = Some(Batch::new(batch_size, timeout));
        self
    }

    pub fn need_recv_error(mut self) -> Self {
        self.rcv_err = true;
        self
    }

    pub fn capacity(mut self, cap: usize) -> Self {
        self.cap = cap;
        self
    }

    pub fn build(self) -> Options {
        Options {
            batcher: Batcher(self.batch),
            rcv_err: self.rcv_err,
            cap: self.cap,
        }
    }
}

pub struct MessageBuffer<T: Clone, E> {
    id_gen: usize,
    main_tx: mpsc::Sender<Item<T>>,
    error_rx: Option<mpsc::Receiver<E>>,
    handle: JoinHandle<Result<(), MessageBufferError>>,
}

impl<T, E> MessageBuffer<T, E>
where
    T: Clone + Send + 'static,
    E: Send + 'static,
{
    pub fn new<P, B>(processor: P, backoff: B, options: Options) -> Self
    where
        P: Processor<T, E> + Send + Sync + 'static,
        P::Future: Send + 'static,
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
        let worker = Worker::new(processor, options.batcher, backoff, main_rx, error_tx);
        let handle = tokio::spawn(worker.start());

        Self {
            id_gen: 0,
            main_tx,
            handle,
            error_rx,
        }
    }

    pub async fn push(&mut self, msg: T) -> Result<(), MessageBufferError> {
        self.id_gen += 1;
        Ok(self.main_tx.try_send(Item {
            id: self.id_gen,
            data: msg,
            trys: 0,
        })?)
    }

    pub async fn error_receiver(&mut self) -> Option<mpsc::Receiver<E>> {
        self.error_rx.take()
    }

    pub async fn stop(self) {
        drop(self.main_tx);
        let _ = self.handle.await;
    }
}

/// msg and exec count
/// 0 for new msg
#[derive(Clone)]
pub struct Item<T: Clone> {
    id: usize,
    data: T,
    trys: usize,
}

impl<T: Clone> Item<T> {
    fn retry(self) -> Self {
        Self {
            id: self.id,
            data: self.data,
            trys: self.trys + 1,
        }
    }
}

/// send msg to retry queue
/// duration to delay
pub struct Retry(usize);

pub trait BackOff {
    fn next(&mut self) -> Duration;
}

pub struct ConstantBackOff(Duration);

impl ConstantBackOff {
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl BackOff for ConstantBackOff {
    fn next(&mut self) -> Duration {
        self.0
    }
}

pub struct ExponentBackOff {
    /// used to calculate next duration
    current: Duration,
    /// must greater than 1
    factor: u32,
    /// max duration boundary
    max: Duration,
}

impl ExponentBackOff {
    pub fn new(init: Duration, mut factor: u32, max: Duration) -> Self {
        if factor <= 1 {
            factor = 2;
        }
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

pub struct Messages<T: Clone> {
    msgs: Vec<Item<T>>,
    retries: Vec<Item<T>>,
}

impl<T: Clone> Messages<T> {
    pub fn msgs(&mut self) -> Vec<Item<T>> {
        mem::take(&mut self.msgs)
    }
    pub fn retry(&mut self, msg: &Item<T>) {
        self.retries.push(msg.clone());
    }
    pub fn retries(mut self) -> Vec<Item<T>> {
        mem::take(&mut self.retries)
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

struct Worker<P, T: Clone, E, B> {
    processor: P,
    backoff: B,
    main_rx: mpsc::Receiver<Item<T>>,
    error_tx: Option<mpsc::Sender<E>>,
    batcher: Batcher,
    retry_q: DelayQueue<Item<T>>,
}

impl<P, T, E, B> Worker<P, T, E, B>
where
    P: Processor<T, E> + Send + Sync + 'static,
    P::Future: Send + 'static,
    T: Clone + 'static,
    E: Send + 'static,
    B: BackOff + Send + 'static,
{
    fn new(
        processor: P,
        batcher: Batcher,
        backoff: B,
        main_rx: mpsc::Receiver<Item<T>>,
        error_tx: Option<mpsc::Sender<E>>,
    ) -> Self {
        Self {
            processor,
            backoff,
            main_rx,
            error_tx,
            batcher,
            retry_q: DelayQueue::new(),
        }
    }

    async fn start(mut self) -> Result<(), MessageBufferError> {
        loop {
            let msgs = self
                .batcher
                .poll(&mut self.main_rx, &mut self.retry_q)
                .await?
                .into();

            match self.processor.call(msgs).await {
                Ok(retries) => retries.into_iter().for_each(|item| {
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
