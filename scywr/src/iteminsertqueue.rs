pub use netpod::CONNECTION_STATUS_DIV;

use crate::session::ScySession;
use crate::store::DataStore;
use bytes::BufMut;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use netpod::DtNano;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use scylla::frame::value::Value;
use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
use scylla::serialize::row::SerializeRow;
use scylla::serialize::value::SerializeCql;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::QueryResult;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use smallvec::smallvec;
use smallvec::SmallVec;
use stats::InsertWorkerStats;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    DbTimeout,
    DbOverload,
    DbUnavailable,
    DbError(#[from] DbError),
    QueryError(#[from] QueryError),
    GetValHelpTodoWaveform,
    GetValHelpInnerTypeMismatch,
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

impl ScalarValue {
    pub fn byte_size(&self) -> u32 {
        match self {
            ScalarValue::I8(_) => 1,
            ScalarValue::I16(_) => 2,
            ScalarValue::I32(_) => 4,
            ScalarValue::F32(_) => 4,
            ScalarValue::F64(_) => 8,
            ScalarValue::Enum(_) => 2,
            ScalarValue::String(x) => x.len() as u32,
            ScalarValue::Bool(_) => 1,
        }
    }
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

impl ArrayValue {
    pub fn len(&self) -> usize {
        use ArrayValue::*;
        match self {
            I8(a) => a.len(),
            I16(a) => a.len(),
            I32(a) => a.len(),
            F32(a) => a.len(),
            F64(a) => a.len(),
            Bool(a) => a.len(),
        }
    }

    pub fn byte_size(&self) -> u32 {
        use ArrayValue::*;
        match self {
            I8(a) => 1 * a.len() as u32,
            I16(a) => 2 * a.len() as u32,
            I32(a) => 4 * a.len() as u32,
            F32(a) => 4 * a.len() as u32,
            F64(a) => 8 * a.len() as u32,
            Bool(a) => 1 * a.len() as u32,
        }
    }

    pub fn to_binary_blob(&self) -> Vec<u8> {
        use ArrayValue::*;
        match self {
            I8(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_i8(x);
                }
                blob
            }
            I16(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_i16_le(x);
                }
                blob
            }
            I32(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_i32_le(x);
                }
                blob
            }
            F32(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_f32_le(x);
                }
                blob
            }
            F64(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_f64_le(x);
                }
                blob
            }
            Bool(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    let x = if x { 1 } else { 0 };
                    blob.put_u8(x);
                }
                blob
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum DataValue {
    Scalar(ScalarValue),
    Array(ArrayValue),
}

impl DataValue {
    pub fn byte_size(&self) -> u32 {
        match self {
            DataValue::Scalar(x) => x.byte_size(),
            DataValue::Array(x) => x.byte_size(),
        }
    }

    pub fn scalar_type(&self) -> ScalarType {
        match self {
            DataValue::Scalar(x) => match x {
                ScalarValue::I8(_) => ScalarType::I8,
                ScalarValue::I16(_) => ScalarType::I16,
                ScalarValue::I32(_) => ScalarType::I32,
                ScalarValue::F32(_) => ScalarType::F32,
                ScalarValue::F64(_) => ScalarType::F64,
                ScalarValue::Enum(_) => ScalarType::U16,
                ScalarValue::String(_) => ScalarType::STRING,
                ScalarValue::Bool(_) => ScalarType::BOOL,
            },
            DataValue::Array(x) => match x {
                ArrayValue::I8(_) => ScalarType::I8,
                ArrayValue::I16(_) => ScalarType::I16,
                ArrayValue::I32(_) => ScalarType::I32,
                ArrayValue::F32(_) => ScalarType::F32,
                ArrayValue::F64(_) => ScalarType::F64,
                ArrayValue::Bool(_) => ScalarType::BOOL,
            },
        }
    }

    pub fn shape(&self) -> Shape {
        match self {
            DataValue::Scalar(_) => Shape::Scalar,
            DataValue::Array(a) => Shape::Wave(a.len() as u32),
        }
    }
}

pub trait GetValHelp<T> {
    type ScalTy: Clone;
    fn get(&self) -> Result<&Self::ScalTy, Error>;
}

impl GetValHelp<i8> for DataValue {
    type ScalTy = i8;
    fn get(&self) -> Result<&Self::ScalTy, Error> {
        match self {
            DataValue::Scalar(v) => match v {
                ScalarValue::I8(v) => Ok(v),
                _ => {
                    //let ty = any::type_name::<Self::ScalTy>();
                    Err(Error::GetValHelpInnerTypeMismatch)
                }
            },
            _ => Err(Error::GetValHelpTodoWaveform),
        }
    }
}

impl GetValHelp<i16> for DataValue {
    type ScalTy = i16;
    fn get(&self) -> Result<&Self::ScalTy, Error> {
        match self {
            DataValue::Scalar(v) => match v {
                ScalarValue::I16(v) => Ok(v),
                _ => {
                    //let ty = any::type_name::<Self::ScalTy>();
                    Err(Error::GetValHelpInnerTypeMismatch)
                }
            },
            _ => Err(Error::GetValHelpTodoWaveform),
        }
    }
}

impl GetValHelp<i32> for DataValue {
    type ScalTy = i32;
    fn get(&self) -> Result<&Self::ScalTy, Error> {
        match self {
            DataValue::Scalar(v) => match v {
                ScalarValue::I32(v) => Ok(v),
                _ => {
                    //let ty = any::type_name::<Self::ScalTy>();
                    Err(Error::GetValHelpInnerTypeMismatch)
                }
            },
            _ => Err(Error::GetValHelpTodoWaveform),
        }
    }
}

impl GetValHelp<f32> for DataValue {
    type ScalTy = f32;
    fn get(&self) -> Result<&Self::ScalTy, Error> {
        match self {
            DataValue::Scalar(v) => match v {
                ScalarValue::F32(v) => Ok(v),
                _ => {
                    //let ty = any::type_name::<Self::ScalTy>();
                    Err(Error::GetValHelpInnerTypeMismatch)
                }
            },
            _ => Err(Error::GetValHelpTodoWaveform),
        }
    }
}

impl GetValHelp<f64> for DataValue {
    type ScalTy = f64;
    fn get(&self) -> Result<&Self::ScalTy, Error> {
        match self {
            DataValue::Scalar(v) => match v {
                ScalarValue::F64(v) => Ok(v),
                _ => {
                    //let ty = any::type_name::<Self::ScalTy>();
                    Err(Error::GetValHelpInnerTypeMismatch)
                }
            },
            _ => Err(Error::GetValHelpTodoWaveform),
        }
    }
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
    ConnectFail,
    IoError,
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
                ConnectFail => 11,
                IoError => 12,
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
            11 => Closed(ConnectFail),
            12 => Closed(IoError),
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

#[derive(Debug, Clone)]
pub enum ShutdownReason {
    ConnectFail,
    IoError,
    ShutdownCommand,
    InternalError,
    Protocol,
    ProtocolMissing,
    IocTimeout,
}

#[derive(Debug)]
pub struct ChannelStatusItem {
    pub ts: SystemTime,
    pub cssid: ChannelStatusSeriesId,
    pub status: ChannelStatus,
}

impl ChannelStatusItem {
    pub fn new_closed_conn_timeout(ts: SystemTime, cssid: ChannelStatusSeriesId) -> Self {
        Self {
            ts,
            cssid,
            status: ChannelStatus::Closed(ChannelStatusClosedReason::IocTimeout),
        }
    }
}

#[derive(Debug)]
pub struct InsertItem {
    pub series: SeriesId,
    pub ts_msp: TsMs,
    pub ts_lsp: DtNano,
    pub msp_bump: bool,
    pub pulse: u64,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub val: DataValue,
    pub ts_local: TsMs,
}

#[derive(Debug)]
pub struct TimeBinSimpleF32 {
    pub series: SeriesId,
    pub bin_len_ms: i32,
    pub ts_msp: TsMs,
    pub off: i32,
    pub count: i64,
    pub min: f32,
    pub max: f32,
    pub avg: f32,
}

#[derive(Debug)]
pub enum QueryItem {
    ConnectionStatus(ConnectionStatusItem),
    ChannelStatus(ChannelStatusItem),
    Insert(InsertItem),
    TimeBinSimpleF32(TimeBinSimpleF32),
    Accounting(Accounting),
}

#[derive(Debug)]
pub struct Accounting {
    pub part: i32,
    pub ts: TsMs,
    pub series: SeriesId,
    pub count: i64,
    pub bytes: i64,
}

struct InsParCom {
    series: SeriesId,
    ts_msp: TsMs,
    ts_lsp: DtNano,
    ts_local: TsMs,
    pulse: u64,
    do_insert: bool,
    stats: Arc<InsertWorkerStats>,
}

fn insert_scalar_gen_fut<ST>(par: InsParCom, val: ST, qu: Arc<PreparedStatement>, scy: Arc<ScySession>) -> InsertFut
where
    ST: Value + SerializeCql + Send + 'static,
{
    let params = (
        par.series.to_i64(),
        par.ts_msp.to_i64(),
        par.ts_lsp.to_i64(),
        par.pulse as i64,
        val,
    );
    InsertFut::new(scy, qu, params, par.ts_local, par.stats)
}

// val: Vec<ST>   where ST: Value + SerializeCql + Send + 'static,
fn insert_array_gen_fut(par: InsParCom, val: Vec<u8>, qu: Arc<PreparedStatement>, scy: Arc<ScySession>) -> InsertFut {
    let params = (
        par.series.to_i64(),
        par.ts_msp.to_i64(),
        par.ts_lsp.to_i64(),
        par.pulse as i64,
        val,
    );
    InsertFut::new(scy, qu, params, par.ts_local, par.stats)
}

#[pin_project::pin_project]
pub struct InsertFut {
    #[allow(unused)]
    scy: Arc<ScySession>,
    #[allow(unused)]
    qu: Arc<PreparedStatement>,
    fut: Pin<Box<dyn Future<Output = Result<QueryResult, QueryError>> + Send>>,
    // #[pin]
    // fut: StackFuture<'static, Result<QueryResult, QueryError>, { 1024 * 3 }>,
}

impl InsertFut {
    pub fn new<V: ValueList + SerializeRow + Send + 'static>(
        scy: Arc<ScySession>,
        qu: Arc<PreparedStatement>,
        params: V,
        // timestamp when we first encountered the data to-be inserted, for metrics
        tsnet: TsMs,
        stats: Arc<InsertWorkerStats>,
    ) -> Self {
        let scy_ref = unsafe { NonNull::from(scy.as_ref()).as_ref() };
        let qu_ref = unsafe { NonNull::from(qu.as_ref()).as_ref() };
        let fut = scy_ref.execute_paged(qu_ref, params, None);
        let fut = fut.map(move |x| {
            let tsnow = TsMs::from_system_time(SystemTime::now());
            let dt = tsnow.to_u64().saturating_sub(tsnet.to_u64()) as u32;
            stats.item_lat_net_store().ingest(dt);
            x
        });
        let fut = taskrun::tokio::task::unconstrained(fut);
        let fut = Box::pin(fut);
        // let fut = StackFuture::from(fut);
        Self { scy, qu, fut }
    }
}

impl Future for InsertFut {
    type Output = Result<QueryResult, QueryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.fut.poll_unpin(cx)
    }
}

async fn insert_scalar_gen<ST>(
    par: InsParCom,
    val: ST,
    qu: &PreparedStatement,
    data_store: &DataStore,
) -> Result<(), Error>
where
    ST: Value + SerializeCql,
{
    let params = (
        par.series.to_i64(),
        par.ts_msp.to_i64(),
        par.ts_lsp.to_i64(),
        par.pulse as i64,
        val,
    );
    if par.do_insert {
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
    } else {
        Ok(())
    }
}

async fn insert_array_gen<ST>(
    par: InsParCom,
    val: Vec<ST>,
    qu: &PreparedStatement,
    data_store: &DataStore,
) -> Result<(), Error>
where
    ST: Value + SerializeCql,
{
    if par.do_insert {
        let params = (
            par.series.to_i64(),
            par.ts_msp.to_i64(),
            par.ts_lsp.to_i64(),
            par.pulse as i64,
            val,
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
    } else {
        Ok(())
    }
}

// TODO currently not in use, anything to merge?
pub async fn insert_item(
    item: InsertItem,
    data_store: &DataStore,
    do_insert: bool,
    stats: &Arc<InsertWorkerStats>,
) -> Result<(), Error> {
    if item.msp_bump {
        let params = (item.series.id() as i64, item.ts_msp.to_i64());
        data_store.scy.execute(&data_store.qu_insert_ts_msp, params).await?;
        stats.inserts_msp().inc();
    }
    use DataValue::*;
    match item.val {
        Scalar(val) => {
            let par = InsParCom {
                series: item.series,
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                ts_local: item.ts_local,
                pulse: item.pulse,
                do_insert,
                stats: stats.clone(),
            };
            use ScalarValue::*;
            match val {
                I8(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i8, &data_store).await?,
                I16(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i16, &data_store).await?,
                Enum(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i16, &data_store).await?,
                I32(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_i32, &data_store).await?,
                F32(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_f32, &data_store).await?,
                F64(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_f64, &data_store).await?,
                String(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_string, &data_store).await?,
                Bool(val) => insert_scalar_gen(par, val, &data_store.qu_insert_scalar_bool, &data_store).await?,
            }
        }
        Array(val) => {
            let par = InsParCom {
                series: item.series,
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                ts_local: item.ts_local,
                pulse: item.pulse,
                do_insert,
                stats: stats.clone(),
            };
            err::todo();
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
    stats.inserts_value().inc();
    Ok(())
}

pub fn insert_msp_fut(
    series: SeriesId,
    ts_msp: TsMs,
    // for stats, the timestamp when we received that data
    tsnet: TsMs,
    scy: Arc<ScySession>,
    qu: Arc<PreparedStatement>,
    stats: Arc<InsertWorkerStats>,
) -> InsertFut {
    let params = (series.to_i64(), ts_msp.to_i64());
    InsertFut::new(scy, qu, params, tsnet, stats)
}

pub fn insert_item_fut(
    item: InsertItem,
    data_store: &DataStore,
    do_insert: bool,
    stats: &Arc<InsertWorkerStats>,
) -> InsertFut {
    let scy = data_store.scy.clone();
    use DataValue::*;
    match item.val {
        Scalar(val) => {
            let par = InsParCom {
                series: item.series,
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                ts_local: item.ts_local,
                pulse: item.pulse,
                do_insert,
                stats: stats.clone(),
            };
            use ScalarValue::*;
            match val {
                I8(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i8.clone(), scy),
                I16(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i16.clone(), scy),
                I32(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i32.clone(), scy),
                F32(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_f32.clone(), scy),
                F64(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_f64.clone(), scy),
                Enum(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i16.clone(), scy),
                String(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_string.clone(), scy),
                Bool(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_bool.clone(), scy),
            }
        }
        Array(val) => {
            let par = InsParCom {
                series: item.series,
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                ts_local: item.ts_local,
                pulse: item.pulse,
                do_insert,
                stats: stats.clone(),
            };
            use ArrayValue::*;
            let blob = val.to_binary_blob();
            #[allow(unused)]
            match val {
                I8(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i8.clone(), scy),
                I16(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i16.clone(), scy),
                I32(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i32.clone(), scy),
                F32(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_f32.clone(), scy),
                F64(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_f64.clone(), scy),
                Bool(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_bool.clone(), scy),
            }
        }
    }
}

pub fn insert_connection_status_fut(
    item: ConnectionStatusItem,
    data_store: &DataStore,
    stats: Arc<InsertWorkerStats>,
) -> InsertFut {
    let ts = TsMs::from_system_time(item.ts);
    let (msp, lsp) = ts.to_grid_02(CONNECTION_STATUS_DIV);
    // TODO is that the good tsnet to use?
    let tsnet = ts;
    let kind = item.status.to_kind();
    let addr = format!("{}", item.addr);
    let params = (msp.to_i64(), lsp.to_i64(), kind as i32, addr);
    InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_connection_status.clone(),
        params,
        tsnet,
        stats,
    )
}

pub fn insert_channel_status_fut(
    item: ChannelStatusItem,
    data_store: &DataStore,
    stats: Arc<InsertWorkerStats>,
) -> SmallVec<[InsertFut; 4]> {
    let ts = TsMs::from_system_time(item.ts);
    let (msp, lsp) = ts.to_grid_02(CONNECTION_STATUS_DIV);
    let tsnet = ts;
    let kind = item.status.to_kind();
    let cssid = item.cssid.id();
    let params = (cssid as i64, msp.to_i64(), lsp.to_i64(), kind as i32);
    let fut1 = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_channel_status.clone(),
        params,
        tsnet,
        stats.clone(),
    );
    let params = (msp.to_i64(), lsp.to_i64(), cssid as i64, kind as i32);
    let fut2 = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_channel_status_by_ts_msp.clone(),
        params,
        tsnet,
        stats,
    );
    smallvec![fut1, fut2]
}

pub async fn insert_connection_status(item: ConnectionStatusItem, data_store: &DataStore) -> Result<(), Error> {
    let ts = TsMs::from_system_time(item.ts);
    let (msp, lsp) = ts.to_grid_02(CONNECTION_STATUS_DIV);
    let kind = item.status.to_kind();
    let addr = format!("{}", item.addr);
    let params = (msp.to_i64(), lsp.to_i64(), kind as i32, addr);
    data_store
        .scy
        .execute(&data_store.qu_insert_connection_status, params)
        .await?;
    Ok(())
}

pub async fn insert_channel_status(item: ChannelStatusItem, data_store: &DataStore) -> Result<(), Error> {
    let ts = TsMs::from_system_time(item.ts);
    let (msp, lsp) = ts.to_grid_02(CONNECTION_STATUS_DIV);
    let kind = item.status.to_kind();
    let cssid = item.cssid.id();
    let params = (cssid as i64, msp.to_i64(), lsp.to_i64(), kind as i32);
    data_store
        .scy
        .execute(&data_store.qu_insert_channel_status, params)
        .await?;
    let params = (msp.to_i64(), lsp.to_i64(), cssid as i64, kind as i32);
    data_store
        .scy
        .execute(&data_store.qu_insert_channel_status_by_ts_msp, params)
        .await?;
    Ok(())
}

pub enum InsertFutKind {
    Value,
}

pub struct InsertItemFut {
    data_store: Arc<DataStore>,
    stats: Arc<InsertWorkerStats>,
    item: InsertItem,
}

impl Future for InsertItemFut {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        todo!()
    }
}
