use crate::zmtp::ErrConv;
use crate::zmtp::{CommonQueries, ZmtpFrame};
use err::Error;
use futures_core::Future;
use futures_util::FutureExt;
use log::*;
use scylla::batch::{Batch, BatchType};
use scylla::frame::value::{BatchValues, ValueList};
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::{BatchResult, QueryResult, Session as ScySession};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

pub struct ScyQueryFut<V> {
    #[allow(unused)]
    scy: Arc<ScySession>,
    #[allow(unused)]
    query: Box<PreparedStatement>,
    #[allow(unused)]
    values: Box<V>,
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>>>>,
}

impl<V> ScyQueryFut<V> {
    pub fn new(scy: Arc<ScySession>, query: PreparedStatement, values: V) -> Self
    where
        V: ValueList + 'static,
    {
        let query = Box::new(query);
        let values = Box::new(values);
        let scy2 = unsafe { &*(&scy as &_ as *const _) } as &ScySession;
        let query2 = unsafe { &*(&query as &_ as *const _) } as &PreparedStatement;
        let v2 = unsafe { &*(&values as &_ as *const _) } as &V;
        let fut = scy2.execute(query2, v2);
        Self {
            scy,
            query,
            values,
            fut: Box::pin(fut),
        }
    }
}

impl<V> Future for ScyQueryFut<V> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        match self.fut.poll_unpin(cx) {
            Ready(k) => match k {
                Ok(_) => {
                    info!("ScyQueryFut done Ok");
                    Ready(Ok(()))
                }
                Err(e) => {
                    warn!("ScyQueryFut done Err");
                    Ready(Err(e).err_conv())
                }
            },
            Pending => Pending,
        }
    }
}

pub struct ScyBatchFut<V> {
    #[allow(unused)]
    scy: Arc<ScySession>,
    #[allow(unused)]
    batch: Box<Batch>,
    #[allow(unused)]
    values: Box<V>,
    fut: Pin<Box<dyn Future<Output = Result<BatchResult, QueryError>>>>,
    polled: usize,
    ts_create: Instant,
    ts_poll_start: Instant,
}

impl<V> ScyBatchFut<V> {
    pub fn new(scy: Arc<ScySession>, batch: Batch, values: V) -> Self
    where
        V: BatchValues + 'static,
    {
        let batch = Box::new(batch);
        let values = Box::new(values);
        let scy2 = unsafe { &*(&scy as &_ as *const _) } as &ScySession;
        let batch2 = unsafe { &*(&batch as &_ as *const _) } as &Batch;
        let v2 = unsafe { &*(&values as &_ as *const _) } as &V;
        let fut = scy2.batch(batch2, v2);
        let tsnow = Instant::now();
        Self {
            scy,
            batch,
            values,
            fut: Box::pin(fut),
            polled: 0,
            ts_create: tsnow,
            ts_poll_start: tsnow,
        }
    }
}

impl<V> Future for ScyBatchFut<V> {
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
                    trace!("ScyBatchFut done Ok");
                    Ready(Ok(()))
                }
                Err(e) => {
                    let tsnow = Instant::now();
                    let dt_created = tsnow.duration_since(self.ts_create).as_secs_f32() * 1e3;
                    let dt_polled = tsnow.duration_since(self.ts_poll_start).as_secs_f32() * 1e3;
                    warn!(
                        "ScyBatchFut  polled {}  dt_created {:6.2} ms  dt_polled {:6.2} ms",
                        self.polled, dt_created, dt_polled
                    );
                    warn!("ScyBatchFut done Err  {e:?}");
                    Ready(Err(e).err_conv())
                }
            },
            Pending => Pending,
        }
    }
}

pub struct ChannelWriteFut {
    nn: usize,
    fut1: Option<Pin<Box<dyn Future<Output = Result<(), Error>>>>>,
    fut2: Option<Pin<Box<dyn Future<Output = Result<(), Error>>>>>,
    ts1: Option<Instant>,
    mask: u8,
}

impl Future for ChannelWriteFut {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            break if self.ts1.is_none() {
                self.ts1 = Some(Instant::now());
                continue;
            } else if let Some(f) = self.fut1.as_mut() {
                match f.poll_unpin(cx) {
                    Ready(k) => {
                        trace!("ChannelWriteFut  fut1 Ready");
                        self.fut1 = None;
                        self.mask |= 1;
                        match k {
                            Ok(_) => continue,
                            Err(e) => Ready(Err(e)),
                        }
                    }
                    Pending => Pending,
                }
            } else if let Some(f) = self.fut2.as_mut() {
                match f.poll_unpin(cx) {
                    Ready(k) => {
                        trace!("ChannelWriteFut  fut2 Ready");
                        self.fut2 = None;
                        self.mask |= 2;
                        match k {
                            Ok(_) => continue,
                            Err(e) => Ready(Err(e)),
                        }
                    }
                    Pending => Pending,
                }
            } else {
                if self.mask != 0 {
                    let ts2 = Instant::now();
                    let dt = ts2.duration_since(self.ts1.unwrap()).as_secs_f32() * 1e3;
                    info!("inserted  nn {}  dt {:6.2} ms", self.nn, dt);
                }
                Ready(Ok(()))
            };
        }
    }
}

pub trait ChannelWriter {
    fn write_msg(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error>;
}

pub struct WriteFutF64 {
    nn: usize,
    fut1: Pin<Box<dyn Future<Output = Result<(), Error>>>>,
    fut2: Pin<Box<dyn Future<Output = Result<(), Error>>>>,
}

impl Future for WriteFutF64 {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}

pub async fn run_write_fut(fut: ChannelWriteFut) -> Result<(), Error> {
    err::todo();
    let ts1 = Instant::now();
    if let Some(f) = fut.fut1 {
        f.await?;
    }
    if let Some(f) = fut.fut2 {
        f.await?;
    }
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f32() * 1e3;
    info!("insert f64  nn {}  dt {:6.2} ms", fut.nn, dt);
    Ok(())
}

pub async fn run_write_fut_f64(fut: WriteFutF64) -> Result<(), Error> {
    err::todo();
    let ts1 = Instant::now();
    fut.fut1.await?;
    fut.fut2.await?;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f32() * 1e3;
    info!("insert f64  nn {}  dt {:6.2} ms", fut.nn, dt);
    Ok(())
}

pub struct ChannelWriterScalarF64 {
    series: u32,
    scy: Arc<ScySession>,
    common_queries: Arc<CommonQueries>,
    ts_msp_last: u64,
    tmp_vals: Vec<(i32, i64, i64, i64, f64)>,
}

impl ChannelWriterScalarF64 {
    pub fn new(series: u32, common_queries: Arc<CommonQueries>, scy: Arc<ScySession>) -> Self {
        Self {
            series,
            scy,
            ts_msp_last: 0,
            common_queries,
            tmp_vals: vec![],
        }
    }

    pub fn write_msg_impl(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error> {
        let (ts_msp, ts_lsp) = ts_msp_lsp_1(ts);
        let fut1 = if ts_msp != self.ts_msp_last {
            info!("write_msg_impl  TS MSP CHANGED  ts {}  pulse {}", ts, pulse);
            self.ts_msp_last = ts_msp;
            let fut = ScyQueryFut::new(
                self.scy.clone(),
                self.common_queries.qu_insert_ts_msp.clone(),
                (self.series as i32, ts_msp as i64),
            );
            Some(Box::pin(fut) as _)
        } else {
            None
        };
        let value = f64::from_be_bytes(fr.data().try_into()?);
        self.tmp_vals
            .push((self.series as i32, ts_msp as i64, ts_lsp as i64, pulse as i64, value));
        if self.tmp_vals.len() >= 100 + ((self.series as usize) & 0xf) {
            let vt = std::mem::replace(&mut self.tmp_vals, vec![]);
            let nn = vt.len();
            let mut batch = Batch::new(BatchType::Unlogged);
            for _ in 0..nn {
                batch.append_statement(self.common_queries.qu_insert_scalar_f64.clone());
            }
            let fut = ScyBatchFut::new(self.scy.clone(), batch, vt);
            let fut2 = Some(Box::pin(fut) as _);
            let ret = ChannelWriteFut {
                ts1: None,
                mask: 0,
                nn,
                fut1,
                fut2,
            };
            Ok(ret)
        } else {
            let ret = ChannelWriteFut {
                ts1: None,
                mask: 0,
                nn: 0,
                fut1: fut1,
                fut2: None,
            };
            Ok(ret)
        }
    }
}

impl ChannelWriter for ChannelWriterScalarF64 {
    fn write_msg(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error> {
        self.write_msg_impl(ts, pulse, fr)
    }
}

pub struct ChannelWriterArrayF64 {
    series: u32,
    scy: Arc<ScySession>,
    common_queries: Arc<CommonQueries>,
    ts_msp_last: u64,
    tmp_vals: Vec<(i32, i64, i64, i64, Vec<f64>)>,
    truncate: usize,
}

impl ChannelWriterArrayF64 {
    pub fn new(series: u32, common_queries: Arc<CommonQueries>, scy: Arc<ScySession>, truncate: usize) -> Self {
        Self {
            series,
            scy,
            ts_msp_last: 0,
            common_queries,
            tmp_vals: vec![],
            truncate,
        }
    }

    pub fn write_msg_impl(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error> {
        let (ts_msp, ts_lsp) = ts_msp_lsp_2(ts);
        let fut1 = if ts_msp != self.ts_msp_last {
            info!("write_msg_impl  TS MSP CHANGED  ts {}  pulse {}", ts, pulse);
            self.ts_msp_last = ts_msp;
            let fut = ScyQueryFut::new(
                self.scy.clone(),
                self.common_queries.qu_insert_ts_msp.clone(),
                (self.series as i32, ts_msp as i64),
            );
            Some(Box::pin(fut) as _)
        } else {
            None
        };
        type ST = f64;
        const STL: usize = std::mem::size_of::<ST>();
        let vc = fr.data().len() / STL;
        let mut values = Vec::with_capacity(vc);
        for i in 0..vc {
            let h = i * STL;
            let value = f64::from_be_bytes(fr.data()[h..h + STL].try_into()?);
            values.push(value);
        }
        values.truncate(self.truncate);
        self.tmp_vals
            .push((self.series as i32, ts_msp as i64, ts_lsp as i64, pulse as i64, values));
        if self.tmp_vals.len() >= 40 + ((self.series as usize) & 0x7) {
            let vt = std::mem::replace(&mut self.tmp_vals, vec![]);
            let nn = vt.len();
            let mut batch = Batch::new(BatchType::Unlogged);
            for _ in 0..nn {
                batch.append_statement(self.common_queries.qu_insert_array_f64.clone());
            }
            let fut = ScyBatchFut::new(self.scy.clone(), batch, vt);
            let fut2 = Some(Box::pin(fut) as _);
            let ret = ChannelWriteFut {
                ts1: None,
                mask: 0,
                nn,
                fut1,
                fut2,
            };
            Ok(ret)
        } else {
            let ret = ChannelWriteFut {
                ts1: None,
                mask: 0,
                nn: 0,
                fut1: fut1,
                fut2: None,
            };
            Ok(ret)
        }
    }
}

impl ChannelWriter for ChannelWriterArrayF64 {
    fn write_msg(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error> {
        self.write_msg_impl(ts, pulse, fr)
    }
}

fn ts_msp_lsp_1(ts: u64) -> (u64, u64) {
    const MASK: u64 = u64::MAX >> 23;
    let ts_msp = ts & (!MASK);
    let ts_lsp = ts & MASK;
    (ts_msp, ts_lsp)
}

fn ts_msp_lsp_2(ts: u64) -> (u64, u64) {
    const MASK: u64 = u64::MAX >> 16;
    let ts_msp = ts & (!MASK);
    let ts_lsp = ts & MASK;
    (ts_msp, ts_lsp)
}
