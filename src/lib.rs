#![allow(dead_code)]

use std::{
    cmp,
    future::Future,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use futures::{future, FutureExt, Stream, StreamExt};
use tokio::{
    select,
    sync::{
        mpsc::error::SendError,
        mpsc::{
            self,
            error::{TryRecvError::Disconnected, TryRecvError::Empty},
        },
        Mutex,
    },
    time::timeout,
};
use tokio_util::{sync::CancellationToken, time::DelayQueue};

#[derive(Debug, thiserror::Error)]
pub enum MessageBufferError {
    #[error("Message buffer queue is full")]
    QueueFull,
    #[error("Message buffer already stopped")]
    Stopped,
}

impl<T: Clone + Send> From<SendError<Item<T>>> for MessageBufferError {
    fn from(_: SendError<Item<T>>) -> Self {
        Self::Stopped
    }
}

#[async_trait]
pub trait Processor<T: Clone + Send, E>: Clone {
    async fn call(&mut self, msgs: Messages<T>) -> Result<(), E>;
}

pub fn processor_fn<F>(f: F) -> ProcessorFn<F> {
    ProcessorFn { f }
}

#[derive(Clone)]
pub struct ProcessorFn<F> {
    f: F,
}

#[async_trait]
impl<T, Fut, F, E> Processor<T, E> for ProcessorFn<F>
where
    T: Clone + Send + 'static,
    F: Fn(Messages<T>) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), E>> + Send,
{
    async fn call(&mut self, msgs: Messages<T>) -> Result<(), E> {
        (self.f)(msgs).await
    }
}

#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
            None => Self::poll_one(main_rx, retry_q).await.map(|msg| match msg {
                Some(item) => vec![item],
                None => vec![],
            }),
        }
    }

    async fn poll_one<T: Clone + Send>(
        main_rx: &mut mpsc::Receiver<Item<T>>,
        retry_q: &mut mpsc::Receiver<Item<T>>,
    ) -> Result<Option<Item<T>>, MessageBufferError> {
        match retry_q.try_recv() {
            Ok(item) => return Ok(Some(item)),
            Err(Disconnected) => return Ok(None),
            Err(Empty) => {}
        }
        select! {
            msg_res = main_rx.recv() => {
                Ok(msg_res)
            }
            ex_res = retry_q.recv() => {
                Ok(ex_res)
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
                if let Some(item) = Self::poll_one(main_rx, retry_q).await? {
                    msgs.push(item)
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

#[derive(Debug, Clone)]
pub struct Options {
    batcher: Batcher,
    rcv_err: bool,
    worker_cap: usize,
    pool_size: usize,
}

impl Options {
    pub fn builder() -> OptionsBuilder {
        OptionsBuilder {
            batch: None,
            rcv_err: false,
            cap: 1024,
            pool_size: num_cpus::get() * 2,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            batcher: Batcher(None),
            rcv_err: false,
            worker_cap: 1024,
            pool_size: num_cpus::get() * 2,
        }
    }
}

pub struct OptionsBuilder {
    batch: Option<Batch>,
    rcv_err: bool,
    cap: usize,
    pool_size: usize,
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

    pub fn worker_capacity(mut self, cap: usize) -> Self {
        self.cap = cmp::max(1, cap);
        self
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        if pool_size == 0 {
            self.pool_size = num_cpus::get();
        } else {
            self.pool_size = pool_size;
        }
        self
    }

    pub fn build(self) -> Options {
        Options {
            batcher: Batcher(self.batch),
            rcv_err: self.rcv_err,
            worker_cap: self.cap,
            pool_size: self.pool_size,
        }
    }
}

struct ErrorChan<E> {
    tx: Option<mpsc::Sender<E>>,
    rx: Arc<Mutex<Option<mpsc::Receiver<E>>>>,
}

pub struct MessageBuffer<P, B, T, E> {
    processor: P,
    backoff: B,
    error_queue: ErrorChan<E>,
    worker_index: AtomicUsize,
    worker_queue: Arc<Mutex<Vec<mpsc::Sender<Item<T>>>>>,
    token: CancellationToken,
    options: Options,
}

impl<P, B, T, E> MessageBuffer<P, B, T, E>
where
    T: Clone + Send + 'static,
    E: Send + 'static,
    P: Processor<T, E> + Clone + Send + Sync + 'static,
    B: BackOff + Send + 'static,
{
    pub fn new(processor: P, backoff: B, options: Options) -> Self {
        let (mut error_tx, mut error_rx) = (None, None);

        if options.rcv_err {
            let (e_tx, e_rx) = mpsc::channel(1);
            error_tx = Some(e_tx);
            error_rx = Some(e_rx);
        }

        Self {
            error_queue: ErrorChan {
                tx: error_tx,
                rx: Arc::new(Mutex::new(error_rx)),
            },
            token: CancellationToken::new(),
            worker_queue: Arc::new(Mutex::new(vec![])),
            options,
            processor,
            backoff,
            worker_index: AtomicUsize::new(0),
        }
    }

    pub async fn try_push(&self, msg: T) -> Result<(), MessageBufferError> {
        let workers = self.get_worker_txs().await;
        for (index, queue) in workers.iter().enumerate() {
            if queue.try_send(Item::new(msg.clone())).is_ok() {
                self.worker_index.store(index + 1, Ordering::Relaxed);
                return Ok(());
            }
        }
        if workers.len() >= self.options.pool_size {
            return Err(MessageBufferError::QueueFull);
        }
        self.new_worker(msg).await;
        Ok(())
    }

    pub async fn push(&mut self, msg: T) {
        if self.worker_num().await < self.options.pool_size {
            self.new_worker(msg).await;
            return;
        }
        if let Some(worker) = self.get_worker().await {
            worker.send(Item::new(msg)).await.ok();
        }
    }

    async fn new_worker(&self, msg: T) {
        let (main_tx, main_rx) = mpsc::channel(self.options.worker_cap);
        main_tx.send(Item::new(msg)).await.ok();
        // start worker
        let worker = Worker::new(
            self.options.worker_cap,
            self.processor.clone(),
            self.options.batcher.clone(),
            self.backoff.clone(),
            main_rx,
            self.error_queue.tx.clone(),
        );
        tokio::spawn(worker.start(self.token.child_token()));
        self.add_worker_tx(main_tx).await;
    }

    async fn add_worker_tx(&self, tx: mpsc::Sender<Item<T>>) {
        let mut workers = self.worker_queue.lock().await;
        workers.push(tx);
    }

    async fn get_worker_txs(&self) -> Vec<mpsc::Sender<Item<T>>> {
        let workers = self.worker_queue.lock().await;
        let mut res = Vec::with_capacity(workers.len());
        for tx in workers.iter() {
            res.push(tx.clone());
        }
        res
    }

    async fn get_worker(&self) -> Option<mpsc::Sender<Item<T>>> {
        let next = self.worker_index.fetch_add(1, Ordering::Relaxed);
        let workers = self.worker_queue.lock().await;
        workers.get(next / workers.len()).cloned()
    }

    async fn worker_num(&self) -> usize {
        let workers = self.worker_queue.lock().await;
        workers.len()
    }

    pub async fn error_receiver(&self) -> Option<mpsc::Receiver<E>> {
        self.error_queue.rx.lock().await.take()
    }

    pub async fn stop(self) -> Result<(), MessageBufferError> {
        let mut workers = self.worker_queue.lock().await;
        while let Some(queue) = workers.pop() {
            drop(queue)
        }
        self.token.cancel();
        Ok(())
    }
}

/// msg and exec count
/// 0 for new msg
#[derive(Clone)]
pub struct Item<T> {
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

pub trait BackOff: Clone {
    fn next(&self, current: Option<Duration>) -> Duration;
}

#[derive(Clone)]
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

#[derive(Clone)]
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
    T: Clone + Send + 'static,
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

    async fn start(self, token: CancellationToken) -> Result<(), MessageBufferError> {
        // user -> retry_tx, retry_rx -> retry_q
        let (retry_tx, retry_rx) = mpsc::channel::<Item<T>>(1);
        // retry_q -> retry_q_tx, retry_q_rx -> poll
        let (retry_q_tx, retry_q_rx) = mpsc::channel::<Item<T>>(self.retry_cap);

        // retry loop task
        let (retry_task, retry_handle) = Self::retry_loop(
            self.retry_q,
            self.backoff,
            retry_q_tx,
            retry_rx,
            token.child_token(),
        )
        .remote_handle();
        tokio::spawn(retry_task);

        // process loop task
        let (process_task, process_handle) = Self::process_loop(
            self.batcher,
            self.processor,
            self.error_tx,
            self.main_rx,
            retry_tx,
            retry_q_rx,
            token.child_token(),
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
        token: CancellationToken,
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
                            return Ok(());
                        },
                    }
                }
                _ = token.cancelled() => {
                    return Ok(())
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
        token: CancellationToken,
    ) -> Result<(), MessageBufferError> {
        loop {
            select! {
                poll_msgs = batcher.poll(&mut main_rx, &mut retry_q_rx) => {
                    let poll_msgs = poll_msgs?;
                    let msgs = Messages::new(poll_msgs, retry_tx.clone());
                    if let Err(e) = processor.call(msgs).await {
                        if let Some(e_tx) = &error_tx {
                            let _ = e_tx.try_send(e);
                        }
                    }
                }
                _ = token.cancelled() => {
                    return Ok(())
                }
            }
        }
    }
}
