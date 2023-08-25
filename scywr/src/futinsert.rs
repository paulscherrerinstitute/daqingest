use crate::access::Error;
use crate::session::ScySession;
use futures_util::Future;
use futures_util::FutureExt;
use netpod::log::*;
use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::QueryResult;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

pub struct ScyInsertFut<'a> {
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send + 'a>>,
    polled: usize,
    ts_create: Instant,
    ts_poll_first: Instant,
}

impl<'a> ScyInsertFut<'a> {
    const NAME: &'static str = "ScyInsertFut";

    pub fn new<V>(scy: &'a ScySession, query: &'a PreparedStatement, values: V) -> Self
    where
        V: ValueList + Send + 'static,
    {
        let fut = scy.execute(query, values);
        let fut = Box::pin(fut) as _;
        let tsnow = Instant::now();
        Self {
            fut,
            polled: 0,
            ts_create: tsnow,
            ts_poll_first: tsnow,
        }
    }
}

impl<'a> Future for ScyInsertFut<'a> {
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
                        Ready(Err(e.into()))
                    }
                },
                Pending => Pending,
            };
        }
    }
}
