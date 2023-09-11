pub use netpod::CONNECTION_STATUS_DIV;

use crate::store::DataStore;
use async_channel::Receiver;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::ScalarType;
use netpod::Shape;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use series::SeriesId;
use stats::CaConnStats;
use std::net::SocketAddrV4;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    DbTimeout,
    DbOverload,
    DbUnavailable,
    DbError(#[from] DbError),
    QueryError(#[from] QueryError),
}

#[derive(Clone, Debug)]
pub enum ScalarValue {
    I8(i8),
    I16(i16),
    I32(i32),
    F32(f32),
    F64(f64),
    Enum(i16),
    String(String),
    Bool(bool),
}

#[derive(Clone, Debug)]
pub enum ArrayValue {
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

#[derive(Clone, Debug)]
pub enum DataValue {
    Scalar(ScalarValue),
    Array(ArrayValue),
}

#[derive(Debug)]
pub enum ConnectionStatus {
    ConnectError,
    ConnectTimeout,
    Established,
    Closing,
    ClosedUnexpected,
    ConnectionHandlerDone,
}

impl ConnectionStatus {
    pub fn to_kind(&self) -> u32 {
        use ConnectionStatus::*;
        match self {
            ConnectError => 1,
            ConnectTimeout => 2,
            Established => 3,
            Closing => 4,
            ClosedUnexpected => 5,
            ConnectionHandlerDone => 6,
        }
    }

    pub fn from_kind(kind: u32) -> Result<Self, err::Error> {
        use ConnectionStatus::*;
        let ret = match kind {
            1 => ConnectError,
            2 => ConnectTimeout,
            3 => Established,
            4 => Closing,
            5 => ClosedUnexpected,
            6 => ConnectionHandlerDone,
            _ => {
                return Err(err::Error::with_msg_no_trace(format!(
                    "unknown ConnectionStatus kind {kind}"
                )));
            }
        };
        Ok(ret)
    }
}

#[derive(Debug)]
pub struct ConnectionStatusItem {
    pub ts: SystemTime,
    pub addr: SocketAddrV4,
    pub status: ConnectionStatus,
}

#[derive(Debug, Clone)]
pub enum ChannelStatusClosedReason {
    ShutdownCommand,
    ChannelRemove,
    ProtocolError,
    FrequencyQuota,
    BandwidthQuota,
    InternalError,
    IocTimeout,
    NoProtocol,
    ProtocolDone,
}

#[derive(Debug)]
pub enum ChannelStatus {
    AssignedToAddress,
    Opened,
    Closed(ChannelStatusClosedReason),
}

impl ChannelStatus {
    pub fn to_kind(&self) -> u32 {
        use ChannelStatus::*;
        use ChannelStatusClosedReason::*;
        match self {
            AssignedToAddress => 24,
            Opened => 1,
            Closed(x) => match x {
                ShutdownCommand => 2,
                ChannelRemove => 3,
                ProtocolError => 4,
                FrequencyQuota => 5,
                BandwidthQuota => 6,
                InternalError => 7,
                IocTimeout => 8,
                NoProtocol => 9,
                ProtocolDone => 10,
            },
        }
    }

    pub fn from_kind(kind: u32) -> Result<Self, err::Error> {
        use ChannelStatus::*;
        use ChannelStatusClosedReason::*;
        let ret = match kind {
            1 => Opened,
            2 => Closed(ShutdownCommand),
            3 => Closed(ChannelRemove),
            4 => Closed(ProtocolError),
            5 => Closed(FrequencyQuota),
            6 => Closed(BandwidthQuota),
            7 => Closed(InternalError),
            8 => Closed(IocTimeout),
            9 => Closed(NoProtocol),
            10 => Closed(ProtocolDone),
            24 => AssignedToAddress,
            _ => {
                return Err(err::Error::with_msg_no_trace(format!(
                    "unknown ChannelStatus kind {kind}"
                )));
            }
        };
        Ok(ret)
    }
}

#[derive(Debug)]
pub struct ChannelStatusItem {
    pub ts: SystemTime,
    pub series: SeriesId,
    pub status: ChannelStatus,
}

#[derive(Debug)]
pub struct InsertItem {
    pub series: SeriesId,
    pub ts_msp: u64,
    pub ts_lsp: u64,
    pub msp_bump: bool,
    pub ts_msp_grid: Option<u32>,
    pub pulse: u64,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub val: DataValue,
}

#[derive(Debug)]
pub struct MuteItem {
    pub series: SeriesId,
    pub ts: u64,
    pub ema: f32,
    pub emd: f32,
}

#[derive(Debug)]
pub struct IvlItem {
    pub series: SeriesId,
    pub ts: u64,
    pub ema: f32,
    pub emd: f32,
}

#[derive(Debug)]
pub struct ChannelInfoItem {
    pub ts_msp: u32,
    pub series: SeriesId,
    pub ivl: f32,
    pub interest: f32,
    pub evsize: u32,
}

#[derive(Debug)]
pub struct TimeBinPatchSimpleF32 {
    pub series: SeriesId,
    pub bin_len_sec: u32,
    pub bin_count: u32,
    pub off_msp: u32,
    pub off_lsp: u32,
    pub counts: Vec<i64>,
    pub mins: Vec<f32>,
    pub maxs: Vec<f32>,
    pub avgs: Vec<f32>,
}

#[derive(Debug)]
pub enum QueryItem {
    ConnectionStatus(ConnectionStatusItem),
    ChannelStatus(ChannelStatusItem),
    Insert(InsertItem),
    Mute(MuteItem),
    Ivl(IvlItem),
    ChannelInfo(ChannelInfoItem),
    TimeBinPatchSimpleF32(TimeBinPatchSimpleF32),
}

pub struct CommonInsertItemQueueSender {
    sender: Sender<QueryItem>,
}

impl CommonInsertItemQueueSender {
    #[inline(always)]
    pub fn send(&self, k: QueryItem) -> async_channel::Send<QueryItem> {
        self.sender.send(k)
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    pub fn inner(&self) -> &Sender<QueryItem> {
        &self.sender
    }
}

pub struct CommonInsertItemQueue {
    sender: Mutex<Option<Sender<QueryItem>>>,
    recv: Receiver<QueryItem>,
}

impl CommonInsertItemQueue {
    pub fn new(cap: usize) -> Self {
        let (tx, rx) = async_channel::bounded(cap);
        Self {
            sender: Mutex::new(Some(tx)),
            recv: rx,
        }
    }

    pub fn from_tx_rx(tx: Sender<QueryItem>, rx: Receiver<QueryItem>) -> Self {
        Self {
            sender: Mutex::new(Some(tx)),
            recv: rx,
        }
    }

    pub fn sender(&self) -> Option<CommonInsertItemQueueSender> {
        match self.sender.lock().unwrap().as_ref() {
            Some(sender) => {
                let ret = CommonInsertItemQueueSender { sender: sender.clone() };
                Some(ret)
            }
            None => None,
        }
    }

    pub fn receiver(&self) -> Option<Receiver<QueryItem>> {
        let ret = self.recv.clone();
        Some(ret)
    }

    pub fn sender_count(&self) -> Option<usize> {
        self.sender.lock().unwrap().as_ref().map(|x| x.sender_count())
    }

    pub fn sender_count_2(&self) -> usize {
        self.recv.sender_count()
    }

    pub fn receiver_count(&self) -> usize {
        self.recv.receiver_count()
    }

    pub fn close(&self) {
        self.sender.lock().unwrap().as_ref().map(|x| x.close());
    }

    pub fn drop_sender(&self) {
        self.sender.lock().unwrap().take();
    }
}

struct InsParCom {
    series: u64,
    ts_msp: u64,
    ts_lsp: u64,
    pulse: u64,
    ttl: u32,
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
        par.ttl as i32,
    );
    let y = data_store.scy.execute(qu, params).await;
    match y {
        Ok(_) => Ok(()),
        Err(e) => match e {
            QueryError::TimeoutError => Err(Error::DbTimeout),
            // TODO use `msg`
            QueryError::DbError(e, _msg) => match e {
                DbError::Overloaded => Err(Error::DbOverload),
                _ => Err(e.into()),
            },
            _ => Err(e.into()),
        },
    }
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
        par.ttl as i32,
    );
    data_store.scy.execute(qu, params).await?;
    Ok(())
}

pub async fn insert_item(
    item: InsertItem,
    ttl_index: Duration,
    ttl_0d: Duration,
    ttl_1d: Duration,
    data_store: &DataStore,
    stats: &CaConnStats,
) -> Result<(), Error> {
    if item.msp_bump {
        let params = (item.series.id() as i64, item.ts_msp as i64, ttl_index.as_secs() as i32);
        data_store.scy.execute(&data_store.qu_insert_ts_msp, params).await?;
        stats.inserts_msp.inc();
    }
    if let Some(ts_msp_grid) = item.ts_msp_grid {
        let params = (
            (item.series.id() as i32) & 0xff,
            ts_msp_grid as i32,
            if item.shape.to_scylla_vec().is_empty() { 0 } else { 1 } as i32,
            item.scalar_type.to_scylla_i32(),
            item.series.id() as i64,
            ttl_index.as_secs() as i32,
        );
        data_store
            .scy
            .execute(&data_store.qu_insert_series_by_ts_msp, params)
            .await?;
        stats.inserts_msp_grid.inc();
    }
    use DataValue::*;
    match item.val {
        Scalar(val) => {
            let par = InsParCom {
                series: item.series.id(),
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                pulse: item.pulse,
                ttl: ttl_0d.as_secs() as _,
            };
            use ScalarValue::*;
            match val {
                I8(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i8, &data_store).await?,
                I16(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i16, &data_store).await?,
                Enum(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i16, &data_store).await?,
                I32(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i32, &data_store).await?,
                F32(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_f32, &data_store).await?,
                F64(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_f64, &data_store).await?,
                String(_) => warn!("TODO string insert"),
                Bool(_v) => warn!("TODO bool insert"),
            }
        }
        Array(val) => {
            let par = InsParCom {
                series: item.series.id(),
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                pulse: item.pulse,
                ttl: ttl_1d.as_secs() as _,
            };
            use ArrayValue::*;
            match val {
                I8(val) => insert_array_gen(par, val, &data_store.qu_insert_array_i8, &data_store).await?,
                I16(val) => insert_array_gen(par, val, &data_store.qu_insert_array_i16, &data_store).await?,
                I32(val) => insert_array_gen(par, val, &data_store.qu_insert_array_i32, &data_store).await?,
                F32(val) => insert_array_gen(par, val, &data_store.qu_insert_array_f32, &data_store).await?,
                F64(val) => insert_array_gen(par, val, &data_store.qu_insert_array_f64, &data_store).await?,
                Bool(val) => insert_array_gen(par, val, &data_store.qu_insert_array_bool, &data_store).await?,
            }
        }
    }
    stats.inserts_val.inc();
    Ok(())
}

pub async fn insert_connection_status(
    item: ConnectionStatusItem,
    ttl: Duration,
    data_store: &DataStore,
    _stats: &CaConnStats,
) -> Result<(), Error> {
    let tsunix = item.ts.duration_since(std::time::UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = tsunix.as_secs() * netpod::timeunits::SEC;
    let nanos = tsunix.subsec_nanos() as u64;
    let ts = secs + nanos;
    let ts_msp = ts / CONNECTION_STATUS_DIV * CONNECTION_STATUS_DIV;
    let ts_lsp = ts - ts_msp;
    let kind = item.status.to_kind();
    let addr = format!("{}", item.addr);
    let params = (ts_msp as i64, ts_lsp as i64, kind as i32, addr, ttl.as_secs() as i32);
    data_store
        .scy
        .execute(&data_store.qu_insert_connection_status, params)
        .await?;
    Ok(())
}

pub async fn insert_channel_status(
    item: ChannelStatusItem,
    ttl: Duration,
    data_store: &DataStore,
    _stats: &CaConnStats,
) -> Result<(), Error> {
    let tsunix = item.ts.duration_since(std::time::UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = tsunix.as_secs() * netpod::timeunits::SEC;
    let nanos = tsunix.subsec_nanos() as u64;
    let ts = secs + nanos;
    let ts_msp = ts / CONNECTION_STATUS_DIV * CONNECTION_STATUS_DIV;
    let ts_lsp = ts - ts_msp;
    let kind = item.status.to_kind();
    let series = item.series.id();
    let params = (
        series as i64,
        ts_msp as i64,
        ts_lsp as i64,
        kind as i32,
        ttl.as_secs() as i32,
    );
    data_store
        .scy
        .execute(&data_store.qu_insert_channel_status, params)
        .await?;
    let params = (
        ts_msp as i64,
        ts_lsp as i64,
        series as i64,
        kind as i32,
        ttl.as_secs() as i32,
    );
    data_store
        .scy
        .execute(&data_store.qu_insert_channel_status_by_ts_msp, params)
        .await?;
    Ok(())
}
