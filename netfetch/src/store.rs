use crate::errconv::ErrConv;
use err::Error;
use futures_util::stream::FuturesOrdered;
use futures_util::{Future, FutureExt, Stream, StreamExt};
use log::*;
use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::{QueryResult, Session as ScySession};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

const CHANNEL_CAP: usize = 128;
const POLLING_CAP: usize = 32;

pub struct ScyInsertFut {
    #[allow(unused)]
    scy: Arc<ScySession>,
    #[allow(unused)]
    query: Arc<PreparedStatement>,
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send>>,
    polled: usize,
    ts_create: Instant,
    ts_poll_first: Instant,
}

impl ScyInsertFut {
    const NAME: &'static str = "ScyInsertFut";

    pub fn new<V>(scy: Arc<ScySession>, query: Arc<PreparedStatement>, values: V) -> Self
    where
        V: ValueList + Send + 'static,
    {
        let scy_ref: &ScySession = unsafe { &*(scy.as_ref() as &_ as *const _) };
        let query_ref = unsafe { &*(query.as_ref() as &_ as *const _) };
        let fut = scy_ref.execute(query_ref, values);
        let fut = Box::pin(fut) as _;
        let tsnow = Instant::now();
        Self {
            scy,
            query,
            fut,
            polled: 0,
            ts_create: tsnow,
            ts_poll_first: tsnow,
        }
    }
}

impl Future for ScyInsertFut {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.polled == 0 {
            self.ts_poll_first = Instant::now();
        }
        self.polled += 1;
        loop {
            break match self.fut.poll_unpin(cx) {
                Ready(k) => match k {
                    Ok(_res) => Ready(Ok(())),
                    Err(e) => {
                        let tsnow = Instant::now();
                        let dt_created = tsnow.duration_since(self.ts_create).as_secs_f32() * 1e3;
                        let dt_poll_first = tsnow.duration_since(self.ts_poll_first).as_secs_f32() * 1e3;
                        error!(
                            "{}  polled {}  dt_created {:6.2} ms  dt_poll_first {:6.2} ms",
                            Self::NAME,
                            self.polled,
                            dt_created,
                            dt_poll_first
                        );
                        error!("{}  done Err  {:?}", Self::NAME, e);
                        Ready(Err(e).err_conv())
                    }
                },
                Pending => Pending,
            };
        }
    }
}

type FutTy = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

pub struct CommonInsertQueueSender {
    sender: async_channel::Sender<FutTy>,
}

impl CommonInsertQueueSender {
    pub async fn send(&self, k: FutTy) -> Result<(), Error> {
        self.sender
            .send(k)
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))
    }
}

pub struct CommonInsertQueue {
    sender: async_channel::Sender<FutTy>,
    recv: async_channel::Receiver<FutTy>,
    futs: FuturesOrdered<FutTy>,
    inp_done: bool,
}

impl CommonInsertQueue {
    pub fn new() -> Self {
        let (tx, rx) = async_channel::bounded(CHANNEL_CAP);
        Self {
            sender: tx.clone(),
            recv: rx,
            futs: FuturesOrdered::new(),
            inp_done: false,
        }
    }

    pub fn sender(&self) -> CommonInsertQueueSender {
        CommonInsertQueueSender {
            sender: self.sender.clone(),
        }
    }
}

impl Stream for CommonInsertQueue {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let _res_inp = if self.futs.len() < POLLING_CAP && !self.inp_done {
                match self.recv.poll_next_unpin(cx) {
                    Ready(Some(k)) => {
                        self.futs.push(k);
                        continue;
                    }
                    Ready(None) => {
                        self.inp_done = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else {
                Ready(Some(()))
            };
            let res_qu = match self.futs.poll_next_unpin(cx) {
                Ready(Some(Ok(_k))) => Ready(Some(Ok(()))),
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => {
                    if self.inp_done {
                        Ready(None)
                    } else {
                        Pending
                    }
                }
                Pending => Pending,
            };
            // TODO monitor queue length and queue pushes per poll of this.
            break res_qu;
        }
    }
}
