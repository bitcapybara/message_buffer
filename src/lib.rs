#![allow(dead_code)]

use std::{error::Error, fmt::Display, future::Future, mem, task::Poll, time::Duration};

use futures::{stream::select_all, Stream, StreamExt};
use tokio::{
    select,
    sync::{
        mpsc::error::SendError,
        mpsc::{self, error::TrySendError},
    },
    task::JoinHandle,
    time::timeout,
};
use tokio_util::time::{delay_queue::Expired, DelayQueue};

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

impl<T> From<TrySendError<T>> for MessageBufferError {
    fn from(e: TrySendError<T>) -> Self {
        match e {
            TrySendError::Full(_) => Self::QueueFull,
            TrySendError::Closed(_) => Self::Stopped,
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

pub fn service_fn<F>(f: F) -> ServiceFn<F> {
    ServiceFn { f }
}

pub struct ServiceFn<F> {
    f: F,
}

impl<T, Fut, F, E> Processor<T, E> for ServiceFn<F>
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
        println!("poll one");
        select! {
            msg_res = main_rx.recv() => {
                println!("main_rx recved");
                msg_res.ok_or(MessageBufferError::Stopped)
            }
            ex_res = retry_q.recv() => {
                println!("retry_q recved");
                ex_res.ok_or(MessageBufferError::Stopped)
            }
            else => {
                println!("poll is empty");
                unreachable!()
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

pub struct MessageBuffer<T: Clone + Send, E> {
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
        println!("options {:?}", options);
        let (main_tx, main_rx) = mpsc::channel(options.cap);
        let (mut error_tx, mut error_rx) = (None, None);
        if options.rcv_err {
            let (e_tx, e_rx) = mpsc::channel(1);
            error_tx = Some(e_tx);
            error_rx = Some(e_rx);
        }

        // start worker
        let worker = Worker::new(processor, options.batcher, backoff, main_rx, error_tx);
        let handle = tokio::spawn(async move {
            if let Err(e) = worker.start().await {
                println!("worker exit... {e}");
                return Err(e);
            }
            Ok(())
        });

        Self {
            main_tx,
            handle,
            error_rx,
        }
    }

    pub async fn push(&mut self, msg: T) -> Result<(), MessageBufferError> {
        Ok(self.main_tx.send(Item { data: msg, trys: 0 }).await?)
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
pub struct Item<T: Clone + Send + Send> {
    pub data: T,
    pub trys: usize,
}

impl<T: Clone + Send + Send> Item<T> {
    fn retry(self) -> Self {
        Self {
            data: self.data,
            trys: self.trys + 1,
        }
    }
}

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
    pub async fn retry(&mut self, msg: &Item<T>) -> Result<(), MessageBufferError> {
        Ok(self.retry_tx.send(msg.clone()).await?)
    }
}

struct RetryQueue<T>(DelayQueue<T>);

impl<T> Stream for RetryQueue<T> {
    type Item = T;

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

struct Worker<P, T: Clone + Send + Send, E, B> {
    processor: P,
    backoff: B,
    main_rx: mpsc::Receiver<Item<T>>,
    error_tx: Option<mpsc::Sender<E>>,
    batcher: Batcher,
    retry_q: RetryQueue<Item<T>>,
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
            retry_q: RetryQueue(DelayQueue::new()),
        }
    }

    async fn start(mut self) -> Result<(), MessageBufferError> {
        let (retry_tx, mut retry_rx) = mpsc::channel::<Item<T>>(1);
        let (retry_q_tx, mut retry_q_rx) = mpsc::channel::<Item<T>>(1);
        tokio::spawn(async move {
            loop {
                select! {
                    retry_q_res = self.retry_q.next() => {
                        if let  Some(item) = retry_q_res {
                            if let Err(e) = retry_q_tx.send(item).await {
                                println!("retry_q_tx send err: {e}");
                            }
                            println!("retry_q_tx added")
                        } else {
                            println!("retry_q empty")
                        }
                    }
                    retry_rx_res = retry_rx.recv() => {
                        match retry_rx_res {
                            Some(item) => {
                                println!("retry_q added");
                                self.retry_q.0.insert(item.retry(), self.backoff.next());
                            }
                            None => {
                                println!("retry_rx dropped");
                                return;
                            },
                        }
                    }
                }
            }
        });
        loop {
            println!("start poll");
            let poll_msgs = self
                .batcher
                .poll(&mut self.main_rx, &mut retry_q_rx)
                .await?;
            println!("end poll");
            let msgs = Messages::new(poll_msgs, retry_tx.clone());
            if let Err(e) = self.processor.call(msgs).await {
                if let Some(e_tx) = &self.error_tx {
                    let _ = e_tx.try_send(e);
                }
            }
        }
    }
}
