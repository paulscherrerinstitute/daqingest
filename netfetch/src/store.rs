use crate::ca::proto::{CaDataArrayValue, CaDataScalarValue, CaDataValue};
use crate::ca::store::DataStore;
use crate::errconv::ErrConv;
use err::Error;
use futures_util::stream::FuturesOrdered;
use futures_util::{Future, FutureExt, Stream, StreamExt};
use log::*;
use netpod::{ScalarType, Shape};
use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::{QueryResult, Session as ScySession};
use stats::CaConnStats;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

const CHANNEL_CAP: usize = 128;
const POLLING_CAP: usize = 32;
const TABLE_SERIES_MOD: u32 = 128;

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

#[derive(Debug)]
pub struct InsertItem {
    pub series: u64,
    pub ts_msp: u64,
    pub ts_lsp: u64,
    pub msp_bump: bool,
    pub pulse: u64,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub val: CaDataValue,
}

pub struct CommonInsertItemQueueSender {
    sender: async_channel::Sender<InsertItem>,
}

impl CommonInsertItemQueueSender {
    #[inline(always)]
    pub fn send(&self, k: InsertItem) -> async_channel::Send<InsertItem> {
        self.sender.send(k)
    }
}

pub struct CommonInsertItemQueue {
    sender: async_channel::Sender<InsertItem>,
    recv: async_channel::Receiver<InsertItem>,
}

impl CommonInsertItemQueue {
    pub fn new(cap: usize) -> Self {
        let (tx, rx) = async_channel::bounded(cap);
        Self {
            sender: tx.clone(),
            recv: rx,
        }
    }

    pub fn sender(&self) -> CommonInsertItemQueueSender {
        CommonInsertItemQueueSender {
            sender: self.sender.clone(),
        }
    }

    pub fn receiver(&self) -> async_channel::Receiver<InsertItem> {
        self.recv.clone()
    }
}

struct InsParCom {
    series: u64,
    ts_msp: u64,
    ts_lsp: u64,
    pulse: u64,
}

async fn insert_scalar_gen<ST>(
    par: InsParCom,
    val: ST,
    qu: &PreparedStatement,
    data_store: &DataStore,
) -> Result<(), Error>
where
    ST: scylla::frame::value::Value,
{
    let params = (
        par.series as i64,
        par.ts_msp as i64,
        par.ts_lsp as i64,
        par.pulse as i64,
        val,
    );
    data_store.scy.execute(qu, params).await.err_conv()?;
    Ok(())
}

async fn insert_array_gen<ST>(
    par: InsParCom,
    val: Vec<ST>,
    qu: &PreparedStatement,
    data_store: &DataStore,
) -> Result<(), Error>
where
    ST: scylla::frame::value::Value,
{
    let params = (
        par.series as i64,
        par.ts_msp as i64,
        par.ts_lsp as i64,
        par.pulse as i64,
        val,
    );
    data_store.scy.execute(qu, params).await.err_conv()?;
    Ok(())
}

pub async fn insert_item(item: InsertItem, data_store: &DataStore, stats: &CaConnStats) -> Result<(), Error> {
    if item.msp_bump {
        let params = (
            (item.series as u32 % TABLE_SERIES_MOD) as i32,
            item.series as i64,
            item.ts_msp as i64,
            item.scalar_type.to_scylla_i32(),
            item.shape.to_scylla_vec(),
        );
        data_store
            .scy
            .execute(&data_store.qu_insert_series, params)
            .await
            .err_conv()?;
        let params = (item.series as i64, item.ts_msp as i64);
        data_store
            .scy
            .execute(&data_store.qu_insert_ts_msp, params)
            .await
            .err_conv()?;
        stats.inserts_msp_inc()
    }
    let par = InsParCom {
        series: item.series,
        ts_msp: item.ts_msp,
        ts_lsp: item.ts_lsp,
        pulse: item.pulse,
    };
    use CaDataValue::*;
    match item.val {
        Scalar(val) => {
            use CaDataScalarValue::*;
            match val {
                I8(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i8, &data_store).await?,
                I16(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i16, &data_store).await?,
                Enum(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i16, &data_store).await?,
                I32(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i32, &data_store).await?,
                F32(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_f32, &data_store).await?,
                F64(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_f64, &data_store).await?,
                String(_) => (),
            }
        }
        Array(val) => {
            use CaDataArrayValue::*;
            match val {
                I8(val) => insert_array_gen(par, val, &data_store.qu_insert_array_i8, &data_store).await?,
                I16(val) => insert_array_gen(par, val, &data_store.qu_insert_array_i16, &data_store).await?,
                I32(val) => insert_array_gen(par, val, &data_store.qu_insert_array_i32, &data_store).await?,
                F32(val) => insert_array_gen(par, val, &data_store.qu_insert_array_f32, &data_store).await?,
                F64(val) => insert_array_gen(par, val, &data_store.qu_insert_array_f64, &data_store).await?,
            }
        }
    }
    stats.inserts_val_inc();
    Ok(())
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
