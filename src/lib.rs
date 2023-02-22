#![allow(dead_code)]

use std::{error::Error, fmt::Display, future::Future, mem, task::Poll, time::Duration};

use futures::{
    future::{self, RemoteHandle},
    FutureExt, Stream, StreamExt,
};
use tokio::{
    select,
    sync::{
        mpsc::error::SendError,
        mpsc::{
            self,
            error::{TryRecvError::Disconnected, TryRecvError::Empty},
        },
    },
    time::timeout,
};
use tokio_util::time::DelayQueue;

#[derive(Debug)]
pub enum MessageBufferError {
    QueueFull,
    Stopped,
}

impl Error for MessageBufferError {}

impl Display for MessageBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "Message buffer queue is full"),
            Self::Stopped => write!(f, "Message buffer already stopped"),
        }
    }
}

impl<T: Clone + Send> From<SendError<Item<T>>> for MessageBufferError {
    fn from(_: SendError<Item<T>>) -> Self {
        Self::Stopped
    }
}

pub trait Processor<T: Clone + Send, E> {
    type Future: Future<Output = Result<(), E>>;

    fn call(&mut self, msgs: Messages<T>) -> Self::Future;
}

pub fn processor_fn<F>(f: F) -> ProcessorFn<F> {
    ProcessorFn { f }
}

pub struct ProcessorFn<F> {
    f: F,
}

impl<T, Fut, F, E> Processor<T, E> for ProcessorFn<F>
where
    T: Clone + Send,
    F: Fn(Messages<T>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    type Future = Fut;

    fn call(&mut self, msgs: Messages<T>) -> Self::Future {
        (self.f)(msgs)
    }
}

#[derive(Debug)]
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
#[derive(Debug)]
struct Batcher(Option<Batch>);

impl Batcher {
    /// if timeout elapsed, return Ok(Vec), vec may be empty
    /// if queue closed, return Err(Stopped)
    async fn poll<T: Clone + Send>(
        &self,
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut mpsc::Receiver<Item<T>>,
    ) -> Result<Vec<Item<T>>, MessageBufferError> {
        match self.0 {
            Some(Batch {
                batch_size,
                timeout,
            }) => Self::poll_batch(batch_size, timeout, main_rx, retry_q).await,
            None => Self::poll_one(main_rx, retry_q).await.map(|msg| vec![msg]),
        }
    }

    async fn poll_one<T: Clone + Send>(
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut mpsc::Receiver<Item<T>>,
    ) -> Result<Item<T>, MessageBufferError> {
        match retry_q.try_recv() {
            Ok(item) => return Ok(item),
            Err(Disconnected) => return Err(MessageBufferError::Stopped),
            Err(Empty) => {}
        }
        select! {
            msg_res = main_rx.recv() => {
                msg_res.ok_or(MessageBufferError::Stopped)
            }
            ex_res = retry_q.recv() => {
                ex_res.ok_or(MessageBufferError::Stopped)
            }
        }
    }

    async fn poll_batch<T: Clone + Send>(
        bsz: usize,
        tmot: Duration,
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut mpsc::Receiver<Item<T>>,
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

#[derive(Debug)]
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

pub struct MessageBuffer<P, B, T: Clone + Send, E> {
    main_tx: mpsc::Sender<Item<T>>,
    error_rx: Option<mpsc::Receiver<E>>,
    worker: Option<Worker<P, T, E, B>>,
    worker_handle: Option<RemoteHandle<Result<(), MessageBufferError>>>,
}

impl<P, B, T, E> MessageBuffer<P, B, T, E>
where
    T: Clone + Send + 'static,
    E: Send + 'static,
    P: Processor<T, E> + Send + Sync + 'static,
    P::Future: Send + 'static,
    B: BackOff + Send + 'static,
{
    pub fn new(processor: P, backoff: B, options: Options) -> Self {
        let (main_tx, main_rx) = mpsc::channel(options.cap);
        let (mut error_tx, mut error_rx) = (None, None);
        if options.rcv_err {
            let (e_tx, e_rx) = mpsc::channel(1);
            error_tx = Some(e_tx);
            error_rx = Some(e_rx);
        }

        // start worker
        let worker = Worker::new(
            options.cap,
            processor,
            options.batcher,
            backoff,
            main_rx,
            error_tx,
        );

        Self {
            main_tx,
            worker: Some(worker),
            worker_handle: None,
            error_rx,
        }
    }

    pub fn start(&mut self) {
        if let Some(worker) = self.worker.take() {
            let (worker_task, worker_handle) = worker.start().remote_handle();
            tokio::spawn(worker_task);
            self.worker_handle.replace(worker_handle);
        }
    }

    pub async fn push(&mut self, msg: T) -> Result<(), MessageBufferError> {
        Ok(self.main_tx.send(Item::new(msg)).await?)
    }

    pub async fn error_receiver(&mut self) -> Option<mpsc::Receiver<E>> {
        self.error_rx.take()
    }

    pub async fn stop(self) -> Result<(), MessageBufferError> {
        drop(self.main_tx);
        if let Some(worker_handle) = self.worker_handle {
            worker_handle.await?;
        }
        Ok(())
    }
}

/// msg and exec count
/// 0 for new msg
#[derive(Clone)]
pub struct Item<T: Clone + Send> {
    pub data: T,
    duration: Option<Duration>,
}

impl<T: Clone + Send> Item<T> {
    fn new(data: T) -> Self {
        Self {
            data,
            duration: None,
        }
    }
    fn with(self, duration: Duration) -> Self {
        Self {
            data: self.data,
            duration: Some(duration),
        }
    }
}

pub trait BackOff {
    fn next(&self, current: Option<Duration>) -> Duration;
}

pub struct ConstantBackOff(Duration);

impl ConstantBackOff {
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl BackOff for ConstantBackOff {
    fn next(&self, _: Option<Duration>) -> Duration {
        self.0
    }
}

pub struct ExponentBackOff {
    /// used to calculate next duration
    init: Duration,
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
        Self { init, factor, max }
    }
}

impl BackOff for ExponentBackOff {
    fn next(&self, current: Option<Duration>) -> Duration {
        match current {
            Some(current) => {
                let mut next = current * self.factor;
                if next > self.max {
                    next = self.max;
                }
                next
            }
            None => self.init,
        }
    }
}

pub struct Messages<T: Clone + Send> {
    msgs: Vec<Item<T>>,
    retry_tx: mpsc::Sender<Item<T>>,
}

impl<T: Clone + Send> Messages<T> {
    fn new(msgs: Vec<Item<T>>, retry_tx: mpsc::Sender<Item<T>>) -> Self {
        Self { msgs, retry_tx }
    }
    pub fn msgs(&mut self) -> Vec<Item<T>> {
        mem::take(&mut self.msgs)
    }
    pub async fn add_retry(&mut self, msg: &Item<T>) -> Result<(), MessageBufferError> {
        Ok(self.retry_tx.send(msg.clone()).await?)
    }
}

struct RetryQueue<T: Clone + Send>(DelayQueue<Item<T>>);

impl<T: Clone + Send> RetryQueue<T> {
    fn insert(&mut self, item: Item<T>) {
        if let Some(duration) = item.duration {
            self.0.insert(item, duration);
        }
    }
}

impl<T: Clone + Send> Stream for RetryQueue<T> {
    type Item = Item<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match DelayQueue::poll_expired(&mut self.get_mut().0, cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item.into_inner())),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

struct Worker<P, T: Clone + Send, E, B> {
    retry_cap: usize,
    processor: P,
    backoff: B,
    main_rx: mpsc::Receiver<Item<T>>,
    error_tx: Option<mpsc::Sender<E>>,
    batcher: Batcher,
    retry_q: RetryQueue<T>,
}

impl<P, T, E, B> Worker<P, T, E, B>
where
    P: Processor<T, E> + Send + Sync + 'static,
    P::Future: Send + 'static,
    T: Clone + Send + Send + 'static,
    E: Send + 'static,
    B: BackOff + Send + 'static,
{
    fn new(
        retry_cap: usize,
        processor: P,
        batcher: Batcher,
        backoff: B,
        main_rx: mpsc::Receiver<Item<T>>,
        error_tx: Option<mpsc::Sender<E>>,
    ) -> Self {
        Self {
            retry_cap,
            processor,
            backoff,
            main_rx,
            error_tx,
            batcher,
            retry_q: RetryQueue(DelayQueue::new()),
        }
    }

    async fn start(self) -> Result<(), MessageBufferError> {
        // user -> retry_tx, retry_rx -> retry_q
        let (retry_tx, retry_rx) = mpsc::channel::<Item<T>>(1);
        // retry_q -> retry_q_tx, retry_q_rx -> poll
        let (retry_q_tx, retry_q_rx) = mpsc::channel::<Item<T>>(self.retry_cap);

        // retry loop task
        let (retry_task, retry_handle) =
            Self::retry_loop(self.retry_q, self.backoff, retry_q_tx, retry_rx).remote_handle();
        tokio::spawn(retry_task);

        // process loop task
        let (process_task, process_handle) = Self::process_loop(
            self.batcher,
            self.processor,
            self.error_tx,
            self.main_rx,
            retry_tx,
            retry_q_rx,
        )
        .remote_handle();
        tokio::spawn(process_task);

        future::try_join(retry_handle, process_handle).await?;
        Ok(())
    }

    async fn retry_loop(
        mut retry_q: RetryQueue<T>,
        backoff: B,
        retry_q_tx: mpsc::Sender<Item<T>>,
        mut retry_rx: mpsc::Receiver<Item<T>>,
    ) -> Result<(), MessageBufferError> {
        loop {
            select! {
                retry_q_res = retry_q.next() => {
                    if let  Some(item) = retry_q_res {
                        retry_q_tx.send(item).await?;
                    }
                }
                retry_rx_res = retry_rx.recv() => {
                    match retry_rx_res {
                        Some(item) => {
                            let duration = backoff.next(item.duration);
                            retry_q.insert(item.with(duration));
                        }
                        None => {
                            return Err(MessageBufferError::Stopped);
                        },
                    }
                }
            }
        }
    }

    async fn process_loop(
        batcher: Batcher,
        mut processor: P,
        error_tx: Option<mpsc::Sender<E>>,
        mut main_rx: mpsc::Receiver<Item<T>>,
        retry_tx: mpsc::Sender<Item<T>>,
        mut retry_q_rx: mpsc::Receiver<Item<T>>,
    ) -> Result<(), MessageBufferError> {
        loop {
            let poll_msgs = batcher.poll(&mut main_rx, &mut retry_q_rx).await?;
            let msgs = Messages::new(poll_msgs, retry_tx.clone());
            if let Err(e) = processor.call(msgs).await {
                if let Some(e_tx) = &error_tx {
                    let _ = e_tx.try_send(e);
                }
            }
        }
    }
}
