#![allow(dead_code)]

use std::{collections::HashMap, future::Future, time::Duration};

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
    fn with_capacity<F, Fut>(cb: F, options: Options) -> Self
    where
        F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Retry>, E>> + Send + 'static,
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

impl<T: Clone> Item<T> {
    fn retry(&self) -> Self {
        Self {
            id: self.id,
            data: self.data.clone(),
            trys: self.trys + 1,
        }
    }
}

/// send msg to retry queue
/// duration to delay
struct Retry(usize, Duration);

impl Retry {
    fn id(&self) -> usize {
        self.0
    }

    fn duration(&self) -> Duration {
        self.1
    }
}

struct Worker<F, Fut, T, E>
where
    F: Fn(Vec<Item<T>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<Retry>, E>> + Send + 'static,
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
    Fut: Future<Output = Result<Vec<Retry>, E>> + Send + 'static,
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
        // store msgs for retry query
        let mut msg_map = HashMap::new();
        loop {
            let msgs = self
                .batcher
                .poll(&mut self.main_rx, &mut self.retry_q)
                .await?;
            for msg in msgs.iter() {
                msg_map.insert(msg.id, msg.clone());
            }

            match (self.cb)(msgs).await {
                Ok(ops) => ops.iter().for_each(|retry| {
                    if let Some(item) = msg_map.get(&retry.id()) {
                        self.retry_q.insert(item.retry(), retry.duration());
                    }
                }),
                Err(e) => {
                    if let Some(e_tx) = &self.error_tx {
                        let _ = e_tx.try_send(e);
                    }
                }
            }
            // clear map for next loop use
            msg_map.clear();
        }
    }
}
