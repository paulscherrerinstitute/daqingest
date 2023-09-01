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

pub struct InsertLoopFut<'a> {
    futs: Vec<Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send + 'a>>>,
    fut_ix: usize,
    polled: usize,
    ts_create: Instant,
    ts_poll_start: Instant,
}

impl<'a> InsertLoopFut<'a> {
    pub fn new<V>(scy: &'a ScySession, query: Option<&'a PreparedStatement>, values: Vec<V>, skip_insert: bool) -> Self
    where
        V: ValueList + Send + Sync + 'static,
    {
        let _ = scy;
        let _ = query;
        let mut values = values;
        if skip_insert {
            values.clear();
        }
        // TODO
        // Can I store the values in some better generic form?
        // Or is it acceptable to generate all insert futures right here and poll them later?
        let futs: Vec<_> = values
            .into_iter()
            .map(|_vs| {
                if true {
                    todo!("InsertLoopFut")
                };
                //let fut = scy.execute(query, vs);
                let fut = futures_util::future::ready(Err(QueryError::TimeoutError));
                Box::pin(fut) as _
            })
            .collect();
        let tsnow = Instant::now();
        Self {
            futs,
            fut_ix: 0,
            polled: 0,
            ts_create: tsnow,
            ts_poll_start: tsnow,
        }
    }
}

impl<'a> Future for InsertLoopFut<'a> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.polled == 0 {
            self.ts_poll_start = Instant::now();
        }
        self.polled += 1;
        if self.futs.is_empty() {
            return Ready(Ok(()));
        }
        loop {
            let fut_ix = self.fut_ix;
            break match self.futs[fut_ix].poll_unpin(cx) {
                Ready(k) => match k {
                    Ok(_) => {
                        self.fut_ix += 1;
                        if self.fut_ix >= self.futs.len() {
                            if false {
                                let tsnow = Instant::now();
                                let dt_created = tsnow.duration_since(self.ts_create).as_secs_f32() * 1e3;
                                let dt_polled = tsnow.duration_since(self.ts_poll_start).as_secs_f32() * 1e3;
                                info!(
                                    "InsertLoopFut  polled {}  dt_created {:6.2} ms  dt_polled {:6.2} ms",
                                    self.polled, dt_created, dt_polled
                                );
                            }
                            continue;
                        } else {
                            Ready(Ok(()))
                        }
                    }
                    Err(e) => {
                        let tsnow = Instant::now();
                        let dt_created = tsnow.duration_since(self.ts_create).as_secs_f32() * 1e3;
                        let dt_polled = tsnow.duration_since(self.ts_poll_start).as_secs_f32() * 1e3;
                        warn!(
                            "InsertLoopFut  polled {}  dt_created {:6.2} ms  dt_polled {:6.2} ms",
                            self.polled, dt_created, dt_polled
                        );
                        warn!("InsertLoopFut  done Err  {e:?}");
                        Ready(Err(e.into()))
                    }
                },
                Pending => Pending,
            };
        }
    }
}
