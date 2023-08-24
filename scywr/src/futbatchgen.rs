use crate::access::Error;
use crate::session::ScySession;
use futures_util::Future;
use futures_util::FutureExt;
use netpod::log::*;
use scylla::batch::Batch;
use scylla::frame::value::BatchValues;
use scylla::transport::errors::QueryError;
use scylla::QueryResult;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

pub struct ScyBatchFutGen<'a> {
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send + 'a>>,
    polled: usize,
    ts_create: Instant,
    ts_poll_start: Instant,
}

impl<'a> ScyBatchFutGen<'a> {
    pub fn new<V>(scy: &'a ScySession, batch: &'a Batch, values: V) -> Self
    where
        V: BatchValues + Send + Sync + 'static,
    {
        let fut = scy.batch(batch, values);
        let tsnow = Instant::now();
        Self {
            fut: Box::pin(fut),
            polled: 0,
            ts_create: tsnow,
            ts_poll_start: tsnow,
        }
    }
}

impl<'a> Future for ScyBatchFutGen<'a> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.polled == 0 {
            self.ts_poll_start = Instant::now();
        }
        self.polled += 1;
        match self.fut.poll_unpin(cx) {
            Ready(k) => match k {
                Ok(_) => {
                    trace!("ScyBatchFutGen  done Ok");
                    Ready(Ok(()))
                }
                Err(e) => {
                    let tsnow = Instant::now();
                    let dt_created = tsnow.duration_since(self.ts_create).as_secs_f32() * 1e3;
                    let dt_polled = tsnow.duration_since(self.ts_poll_start).as_secs_f32() * 1e3;
                    warn!(
                        "ScyBatchFutGen  polled {}  dt_created {:6.2} ms  dt_polled {:6.2} ms",
                        self.polled, dt_created, dt_polled
                    );
                    warn!("ScyBatchFutGen  done Err  {e:?}");
                    Ready(Err(e.into()))
                }
            },
            Pending => Pending,
        }
    }
}
