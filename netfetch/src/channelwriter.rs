use crate::errconv::ErrConv;
use crate::zmtp::{CommonQueries, ZmtpFrame};
use err::Error;
use futures_util::{Future, FutureExt};
use log::*;
use netpod::timeunits::SEC;
use netpod::{ByteOrder, ScalarType, Shape};
use scylla::batch::{Batch, BatchType};
use scylla::frame::value::{BatchValues, ValueList};
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::{BatchResult, QueryResult, Session as ScySession};
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub struct ScyQueryFut<'a> {
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send + 'a>>,
}

impl<'a> ScyQueryFut<'a> {
    pub fn new<V>(scy: &'a ScySession, query: &'a PreparedStatement, values: V) -> Self
    where
        V: ValueList + Send + 'static,
    {
        let fut = scy.execute(query, values);
        Self { fut: Box::pin(fut) }
    }
}

impl<'a> Future for ScyQueryFut<'a> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        match self.fut.poll_unpin(cx) {
            Ready(k) => match k {
                Ok(_) => Ready(Ok(())),
                Err(e) => Ready(Err(e).err_conv()),
            },
            Pending => Pending,
        }
    }
}

pub struct ScyBatchFut<'a> {
    fut: Pin<Box<dyn Future<Output = Result<BatchResult, QueryError>> + 'a>>,
    polled: usize,
    ts_create: Instant,
    ts_poll_start: Instant,
}

impl<'a> ScyBatchFut<'a> {
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

impl<'a> Future for ScyBatchFut<'a> {
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

pub struct ScyBatchFutGen<'a> {
    fut: Pin<Box<dyn Future<Output = Result<BatchResult, QueryError>> + Send + 'a>>,
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
                    Ready(Err(e).err_conv())
                }
            },
            Pending => Pending,
        }
    }
}

pub struct InsertLoopFut<'a> {
    futs: Vec<Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send + 'a>>>,
    fut_ix: usize,
    polled: usize,
    ts_create: Instant,
    ts_poll_start: Instant,
}

impl<'a> InsertLoopFut<'a> {
    pub fn new<V>(scy: &'a ScySession, query: &'a PreparedStatement, values: Vec<V>, skip_insert: bool) -> Self
    where
        V: ValueList + Send + Sync + 'static,
    {
        let mut values = values;
        if skip_insert {
            values.clear();
        }
        // TODO
        // Can I store the values in some better generic form?
        // Or is it acceptable to generate all insert futures right here and poll them later?
        let futs: Vec<_> = values
            .into_iter()
            .map(|vs| {
                let fut = scy.execute(query, vs);
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
                        Ready(Err(e).err_conv())
                    }
                },
                Pending => Pending,
            };
        }
    }
}

pub struct ChannelWriteRes {
    pub nrows: u32,
    pub dt: Duration,
}

pub struct ChannelWriteFut<'a> {
    nn: usize,
    fut1: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>>,
    fut2: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>>,
    ts1: Option<Instant>,
    mask: u8,
}

impl<'a> Future for ChannelWriteFut<'a> {
    type Output = Result<ChannelWriteRes, Error>;

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
                    let dt = ts2.duration_since(self.ts1.unwrap());
                    if false {
                        trace!(
                            "ChannelWriteFut  inserted  nn {}  dt {:6.2} ms",
                            self.nn,
                            dt.as_secs_f32() * 1e3
                        );
                    }
                    let res = ChannelWriteRes {
                        nrows: self.nn as u32,
                        dt,
                    };
                    Ready(Ok(res))
                } else {
                    let res = ChannelWriteRes {
                        nrows: 0,
                        dt: Duration::from_millis(0),
                    };
                    Ready(Ok(res))
                }
            };
        }
    }
}

pub trait ChannelWriter {
    fn write_msg(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error>;
}

struct MsgAcceptorOptions {
    cq: Arc<CommonQueries>,
    skip_insert: bool,
    array_truncate: usize,
}

trait MsgAcceptor {
    fn len(&self) -> usize;
    fn accept(&mut self, ts_msp: i64, ts_lsp: i64, pulse: i64, fr: &ZmtpFrame) -> Result<(), Error>;
    fn should_flush(&self) -> bool;
    fn flush_loop<'a>(&'a mut self, scy: &'a ScySession) -> Result<InsertLoopFut<'a>, Error>;
    fn flush_batch<'a>(&'a mut self, scy: &'a ScySession) -> Result<ScyBatchFutGen<'a>, Error>;
}

macro_rules! impl_msg_acceptor_scalar {
    ($sname:ident, $st:ty, $qu_id:ident, $from_bytes:ident) => {
        struct $sname {
            query: PreparedStatement,
            values: Vec<(i64, i64, i64, i64, $st)>,
            series: i64,
            opts: MsgAcceptorOptions,
            batch: Batch,
        }

        impl $sname {
            pub fn new(series: i64, opts: MsgAcceptorOptions) -> Self {
                Self {
                    query: opts.cq.$qu_id.clone(),
                    values: vec![],
                    series,
                    opts,
                    batch: Batch::new((BatchType::Unlogged)),
                }
            }
        }

        impl MsgAcceptor for $sname {
            fn len(&self) -> usize {
                self.values.len()
            }

            fn accept(&mut self, ts_msp: i64, ts_lsp: i64, pulse: i64, fr: &ZmtpFrame) -> Result<(), Error> {
                type ST = $st;
                const STL: usize = std::mem::size_of::<ST>();
                let data = fr.data();
                if data.len() < STL {
                    return Err(Error::with_msg_no_trace(format!(
                        "data frame too small for type: {} vs {}",
                        data.len(),
                        STL
                    )));
                }
                let a = data[..STL].try_into()?;
                let value = ST::$from_bytes(a);
                self.values.push((self.series, ts_msp, ts_lsp, pulse, value));
                Ok(())
            }

            fn should_flush(&self) -> bool {
                self.len() >= 140 + ((self.series as usize) & 0x1f)
            }

            fn flush_batch<'a>(&'a mut self, scy: &'a ScySession) -> Result<ScyBatchFutGen<'a>, Error> {
                let vt = mem::replace(&mut self.values, vec![]);
                let nn = vt.len();
                self.batch = Batch::new(BatchType::Unlogged);
                let batch = &mut self.batch;
                for _ in 0..nn {
                    batch.append_statement(self.query.clone());
                }
                let ret = ScyBatchFutGen::new(&scy, batch, vt);
                Ok(ret)
            }

            fn flush_loop<'a>(&'a mut self, scy: &'a ScySession) -> Result<InsertLoopFut<'a>, Error> {
                let vt = mem::replace(&mut self.values, vec![]);
                let ret = InsertLoopFut::new(scy, &self.query, vt, self.opts.skip_insert);
                Ok(ret)
            }
        }
    };
}

macro_rules! impl_msg_acceptor_array {
    ($sname:ident, $st:ty, $qu_id:ident, $from_bytes:ident) => {
        struct $sname {
            query: PreparedStatement,
            values: Vec<(i64, i64, i64, i64, Vec<$st>)>,
            series: i64,
            array_truncate: usize,
            truncated: usize,
            opts: MsgAcceptorOptions,
            batch: Batch,
        }

        impl $sname {
            pub fn new(series: i64, opts: MsgAcceptorOptions) -> Self {
                Self {
                    query: opts.cq.$qu_id.clone(),
                    values: vec![],
                    series,
                    array_truncate: opts.array_truncate,
                    truncated: 0,
                    opts,
                    batch: Batch::new(BatchType::Unlogged),
                }
            }
        }

        impl MsgAcceptor for $sname {
            fn len(&self) -> usize {
                self.values.len()
            }

            fn accept(&mut self, ts_msp: i64, ts_lsp: i64, pulse: i64, fr: &ZmtpFrame) -> Result<(), Error> {
                type ST = $st;
                const STL: usize = std::mem::size_of::<ST>();
                let vc2 = (fr.data().len() / STL);
                let vc = vc2.min(self.array_truncate);
                if vc != vc2 {
                    self.truncated = self.truncated.saturating_add(1);
                }
                let mut values = Vec::with_capacity(vc);
                for i in 0..vc {
                    let h = i * STL;
                    let value = ST::$from_bytes(fr.data()[h..h + STL].try_into()?);
                    values.push(value);
                }
                self.values.push((self.series, ts_msp, ts_lsp, pulse, values));
                Ok(())
            }

            fn should_flush(&self) -> bool {
                self.len() >= 40 + ((self.series as usize) & 0x7)
            }

            fn flush_batch<'a>(&'a mut self, scy: &'a ScySession) -> Result<ScyBatchFutGen<'a>, Error> {
                let vt = mem::replace(&mut self.values, vec![]);
                let nn = vt.len();
                self.batch = Batch::new(BatchType::Unlogged);
                let batch = &mut self.batch;
                for _ in 0..nn {
                    batch.append_statement(self.query.clone());
                }
                let ret = ScyBatchFutGen::new(&scy, batch, vt);
                Ok(ret)
            }

            fn flush_loop<'a>(&'a mut self, scy: &'a ScySession) -> Result<InsertLoopFut<'a>, Error> {
                let vt = mem::replace(&mut self.values, vec![]);
                let ret = InsertLoopFut::new(scy, &self.query, vt, self.opts.skip_insert);
                Ok(ret)
            }
        }
    };
}

impl_msg_acceptor_scalar!(MsgAcceptorScalarU16LE, i16, qu_insert_scalar_i16, from_le_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarU16BE, i16, qu_insert_scalar_i16, from_be_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarU32LE, i32, qu_insert_scalar_i32, from_le_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarU32BE, i32, qu_insert_scalar_i32, from_be_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarI16LE, i16, qu_insert_scalar_i16, from_le_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarI16BE, i16, qu_insert_scalar_i16, from_be_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarI32LE, i32, qu_insert_scalar_i32, from_le_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarI32BE, i32, qu_insert_scalar_i32, from_be_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarF32LE, f32, qu_insert_scalar_f32, from_le_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarF32BE, f32, qu_insert_scalar_f32, from_be_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarF64LE, f64, qu_insert_scalar_f64, from_le_bytes);
impl_msg_acceptor_scalar!(MsgAcceptorScalarF64BE, f64, qu_insert_scalar_f64, from_be_bytes);

impl_msg_acceptor_array!(MsgAcceptorArrayU16LE, i16, qu_insert_array_u16, from_le_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayU16BE, i16, qu_insert_array_u16, from_be_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayI16LE, i16, qu_insert_array_i16, from_le_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayI16BE, i16, qu_insert_array_i16, from_be_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayI32LE, i32, qu_insert_array_i32, from_le_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayI32BE, i32, qu_insert_array_i32, from_be_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayF32LE, f32, qu_insert_array_f32, from_le_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayF32BE, f32, qu_insert_array_f32, from_be_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayF64LE, f64, qu_insert_array_f64, from_le_bytes);
impl_msg_acceptor_array!(MsgAcceptorArrayF64BE, f64, qu_insert_array_f64, from_be_bytes);

struct MsgAcceptorArrayBool {
    query: PreparedStatement,
    values: Vec<(i64, i64, i64, i64, Vec<bool>)>,
    series: i64,
    array_truncate: usize,
    truncated: usize,
    opts: MsgAcceptorOptions,
    batch: Batch,
}

impl MsgAcceptorArrayBool {
    pub fn new(series: i64, opts: MsgAcceptorOptions) -> Self {
        Self {
            query: opts.cq.qu_insert_array_bool.clone(),
            values: vec![],
            series,
            array_truncate: opts.array_truncate,
            truncated: 0,
            opts,
            batch: Batch::new(BatchType::Unlogged),
        }
    }
}

impl MsgAcceptor for MsgAcceptorArrayBool {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn accept(&mut self, ts_msp: i64, ts_lsp: i64, pulse: i64, fr: &ZmtpFrame) -> Result<(), Error> {
        type ST = bool;
        const STL: usize = std::mem::size_of::<ST>();
        let vc = fr.data().len() / STL;
        let mut values = Vec::with_capacity(vc);
        for i in 0..vc {
            let h = i * STL;
            let value = u8::from_le_bytes(fr.data()[h..h + STL].try_into()?);
            values.push(value);
        }
        if values.len() > self.array_truncate {
            if self.truncated < 10 {
                warn!(
                    "truncate {} to {} for series {}",
                    values.len(),
                    self.array_truncate,
                    self.series
                );
            }
            values.truncate(self.array_truncate);
            self.truncated = self.truncated.saturating_add(1);
        }
        let values = values.into_iter().map(|x| x != 0).collect();
        self.values.push((self.series, ts_msp, ts_lsp, pulse, values));
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.len() >= 40 + ((self.series as usize) & 0x7)
    }

    fn flush_batch<'a>(&'a mut self, scy: &'a ScySession) -> Result<ScyBatchFutGen, Error> {
        let vt = mem::replace(&mut self.values, vec![]);
        let nn = vt.len();
        self.batch = Batch::new(BatchType::Unlogged);
        let batch = &mut self.batch;
        for _ in 0..nn {
            batch.append_statement(self.query.clone());
        }
        let ret = ScyBatchFutGen::new(&scy, batch, vt);
        Ok(ret)
    }

    fn flush_loop<'a>(&'a mut self, scy: &'a ScySession) -> Result<InsertLoopFut<'a>, Error> {
        let vt = mem::replace(&mut self.values, vec![]);
        let ret = InsertLoopFut::new(scy, &self.query, vt, self.opts.skip_insert);
        Ok(ret)
    }
}

pub struct ChannelWriterAll {
    series: u64,
    scy: Arc<ScySession>,
    common_queries: Arc<CommonQueries>,
    ts_msp_lsp: fn(u64, u64) -> (u64, u64),
    ts_msp_last: u64,
    acceptor: Box<dyn MsgAcceptor + Send>,
    #[allow(unused)]
    scalar_type: ScalarType,
    #[allow(unused)]
    shape: Shape,
    pulse_last: u64,
    #[allow(unused)]
    skip_insert: bool,
}

impl ChannelWriterAll {
    pub fn new(
        series: u64,
        common_queries: Arc<CommonQueries>,
        scy: Arc<ScySession>,
        scalar_type: ScalarType,
        shape: Shape,
        byte_order: ByteOrder,
        array_truncate: usize,
        skip_insert: bool,
    ) -> Result<Self, Error> {
        let opts = MsgAcceptorOptions {
            cq: common_queries.clone(),
            skip_insert,
            array_truncate,
        };
        let (ts_msp_lsp, acc): (fn(u64, u64) -> (u64, u64), Box<dyn MsgAcceptor + Send>) = match &shape {
            Shape::Scalar => match &scalar_type {
                ScalarType::U16 => match &byte_order {
                    ByteOrder::Little => {
                        let acc = MsgAcceptorScalarU16LE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                    ByteOrder::Big => {
                        let acc = MsgAcceptorScalarU16BE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                },
                ScalarType::U32 => match &byte_order {
                    ByteOrder::Little => {
                        let acc = MsgAcceptorScalarU32LE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                    ByteOrder::Big => {
                        let acc = MsgAcceptorScalarU32BE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                },
                ScalarType::I16 => match &byte_order {
                    ByteOrder::Little => {
                        let acc = MsgAcceptorScalarI16LE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                    ByteOrder::Big => {
                        let acc = MsgAcceptorScalarI16BE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                },
                ScalarType::I32 => match &byte_order {
                    ByteOrder::Little => {
                        let acc = MsgAcceptorScalarI32LE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                    ByteOrder::Big => {
                        let acc = MsgAcceptorScalarI32BE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                },
                ScalarType::F32 => match &byte_order {
                    ByteOrder::Little => {
                        let acc = MsgAcceptorScalarF32LE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                    ByteOrder::Big => {
                        let acc = MsgAcceptorScalarF32BE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                },
                ScalarType::F64 => match &byte_order {
                    ByteOrder::Little => {
                        let acc = MsgAcceptorScalarF64LE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                    ByteOrder::Big => {
                        let acc = MsgAcceptorScalarF64BE::new(series as i64, opts);
                        (ts_msp_lsp_1, Box::new(acc) as _)
                    }
                },
                _ => {
                    return Err(Error::with_msg_no_trace(format!(
                        "TODO  {:?}  {:?}  {:?}",
                        scalar_type, shape, byte_order
                    )));
                }
            },
            Shape::Wave(nele) => {
                info!("set up wave acceptor  nele {nele}");
                match &scalar_type {
                    ScalarType::BOOL => match &byte_order {
                        _ => {
                            let acc = MsgAcceptorArrayBool::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                    },
                    ScalarType::U16 => match &byte_order {
                        ByteOrder::Little => {
                            let acc = MsgAcceptorArrayU16LE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                        ByteOrder::Big => {
                            let acc = MsgAcceptorArrayU16BE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                    },
                    ScalarType::I16 => match &byte_order {
                        ByteOrder::Little => {
                            let acc = MsgAcceptorArrayI16LE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                        ByteOrder::Big => {
                            let acc = MsgAcceptorArrayI16BE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                    },
                    ScalarType::I32 => match &byte_order {
                        ByteOrder::Little => {
                            let acc = MsgAcceptorArrayI32LE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                        ByteOrder::Big => {
                            let acc = MsgAcceptorArrayI32BE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                    },
                    ScalarType::F32 => match &byte_order {
                        ByteOrder::Little => {
                            let acc = MsgAcceptorArrayF32LE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                        ByteOrder::Big => {
                            let acc = MsgAcceptorArrayF32BE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                    },
                    ScalarType::F64 => match &byte_order {
                        ByteOrder::Little => {
                            let acc = MsgAcceptorArrayF64LE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                        ByteOrder::Big => {
                            let acc = MsgAcceptorArrayF64BE::new(series as i64, opts);
                            (ts_msp_lsp_2, Box::new(acc) as _)
                        }
                    },
                    _ => {
                        return Err(Error::with_msg_no_trace(format!(
                            "TODO  {:?}  {:?}  {:?}",
                            scalar_type, shape, byte_order
                        )));
                    }
                }
            }
            _ => {
                return Err(Error::with_msg_no_trace(format!(
                    "TODO  {:?}  {:?}  {:?}",
                    scalar_type, shape, byte_order
                )));
            }
        };
        let ret = Self {
            series,
            scy,
            common_queries,
            ts_msp_lsp,
            ts_msp_last: 0,
            acceptor: acc,
            scalar_type,
            shape,
            pulse_last: 0,
            skip_insert,
        };
        Ok(ret)
    }

    pub fn write_msg_impl(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error> {
        // TODO limit log rate
        // TODO for many channels, it's normal to have gaps.
        if false && pulse != 0 && pulse != self.pulse_last + 1 {
            let gap = pulse as i64 - self.pulse_last as i64;
            warn!("GAP  series {}  pulse {}  gap {}", self.series, pulse, gap);
        }
        self.pulse_last = pulse;
        let (ts_msp, ts_lsp) = (self.ts_msp_lsp)(ts, self.series);
        let fut1 = if ts_msp != self.ts_msp_last {
            debug!("ts_msp changed  ts {ts}  pulse {pulse}  ts_msp {ts_msp}  ts_lsp {ts_lsp}");
            self.ts_msp_last = ts_msp;
            if !self.skip_insert {
                let fut = ScyQueryFut::new(
                    &self.scy,
                    &self.common_queries.qu_insert_ts_msp,
                    (self.series as i64, ts_msp as i64),
                );
                Some(Box::pin(fut) as _)
            } else {
                None
            }
        } else {
            None
        };
        self.acceptor.accept(ts_msp as i64, ts_lsp as i64, pulse as i64, fr)?;
        if self.acceptor.should_flush() {
            let nn = self.acceptor.len();
            let fut = self.acceptor.flush_loop(&self.scy)?;
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
                fut1,
                fut2: None,
            };
            Ok(ret)
        }
    }
}

impl ChannelWriter for ChannelWriterAll {
    fn write_msg(&mut self, ts: u64, pulse: u64, fr: &ZmtpFrame) -> Result<ChannelWriteFut, Error> {
        self.write_msg_impl(ts, pulse, fr)
    }
}

fn ts_msp_lsp_1(ts: u64, series: u64) -> (u64, u64) {
    ts_msp_lsp_gen(ts, series, 100 * SEC)
}

fn ts_msp_lsp_2(ts: u64, series: u64) -> (u64, u64) {
    ts_msp_lsp_gen(ts, series, 10 * SEC)
}

fn ts_msp_lsp_gen(ts: u64, series: u64, fak: u64) -> (u64, u64) {
    if ts < u32::MAX as u64 {
        return (0, 0);
    }
    let off = series & 0xffffffff;
    let ts_a = ts - off;
    let ts_b = ts_a / fak;
    let ts_lsp = ts_a % fak;
    let ts_msp = ts_b * fak + off;
    (ts_msp, ts_lsp)
}
