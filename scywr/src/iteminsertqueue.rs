pub use netpod::CONNECTION_STATUS_DIV;

use crate::session::ScySession;
use crate::store::DataStore;
use bytes::BufMut;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
#[allow(unused)]
use netpod::log::*;
use netpod::DtNano;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
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
use std::time::Instant;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaItemInsertQueue")]
pub enum Error {
    DbTimeout,
    DbOverload,
    DbUnavailable,
    DbError(#[from] DbError),
    QueryError(#[from] QueryError),
    GetValHelpTodoWaveform,
    GetValHelpInnerTypeMismatch,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Enum(i16, String),
    String(String),
    Bool(bool),
    CaStatus(i16),
}

impl ScalarValue {
    pub fn byte_size(&self) -> u32 {
        match self {
            ScalarValue::U8(_) => 1,
            ScalarValue::U16(_) => 1,
            ScalarValue::U32(_) => 1,
            ScalarValue::U64(_) => 1,
            ScalarValue::I8(_) => 1,
            ScalarValue::I16(_) => 2,
            ScalarValue::I32(_) => 4,
            ScalarValue::I64(_) => 8,
            ScalarValue::F32(_) => 4,
            ScalarValue::F64(_) => 8,
            ScalarValue::Enum(_, y) => 2 + y.len() as u32,
            ScalarValue::String(x) => x.len() as u32,
            ScalarValue::Bool(_) => 1,
            ScalarValue::CaStatus(_) => 2,
        }
    }

    pub fn string_short(&self) -> String {
        match self {
            ScalarValue::U8(x) => x.to_string(),
            ScalarValue::U16(x) => x.to_string(),
            ScalarValue::U32(x) => x.to_string(),
            ScalarValue::U64(x) => x.to_string(),
            ScalarValue::I8(x) => x.to_string(),
            ScalarValue::I16(x) => x.to_string(),
            ScalarValue::I32(x) => x.to_string(),
            ScalarValue::I64(x) => x.to_string(),
            ScalarValue::F32(x) => x.to_string(),
            ScalarValue::F64(x) => x.to_string(),
            ScalarValue::Enum(x, y) => format!("({}, {})", x, y),
            ScalarValue::String(x) => x.to_string(),
            ScalarValue::Bool(x) => x.to_string(),
            ScalarValue::CaStatus(x) => x.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ArrayValue {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

impl ArrayValue {
    pub fn len(&self) -> usize {
        use ArrayValue::*;
        match self {
            U8(a) => a.len(),
            U16(a) => a.len(),
            U32(a) => a.len(),
            U64(a) => a.len(),
            I8(a) => a.len(),
            I16(a) => a.len(),
            I32(a) => a.len(),
            I64(a) => a.len(),
            F32(a) => a.len(),
            F64(a) => a.len(),
            Bool(a) => a.len(),
        }
    }

    pub fn byte_size(&self) -> u32 {
        use ArrayValue::*;
        match self {
            U8(a) => 1 * a.len() as u32,
            U16(a) => 2 * a.len() as u32,
            U32(a) => 4 * a.len() as u32,
            U64(a) => 8 * a.len() as u32,
            I8(a) => 1 * a.len() as u32,
            I16(a) => 2 * a.len() as u32,
            I32(a) => 4 * a.len() as u32,
            I64(a) => 8 * a.len() as u32,
            F32(a) => 4 * a.len() as u32,
            F64(a) => 8 * a.len() as u32,
            Bool(a) => 1 * a.len() as u32,
        }
    }

    pub fn to_binary_blob(&self) -> Vec<u8> {
        use ArrayValue::*;
        match self {
            U8(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_u8(x);
                }
                blob
            }
            U16(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_u16_le(x);
                }
                blob
            }
            U32(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_u32_le(x);
                }
                blob
            }
            U64(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_u64_le(x);
                }
                blob
            }
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
            I64(a) => {
                let n = self.byte_size();
                let mut blob = Vec::with_capacity(32 + n as usize);
                for _ in 0..4 {
                    blob.put_u64_le(0);
                }
                for &x in a {
                    blob.put_i64_le(x);
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

    pub fn string_short(&self) -> String {
        use ArrayValue::*;
        match self {
            U8(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            U16(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            U32(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            U64(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            I8(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            I16(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            I32(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            I64(x) => format!("{}", x.get(0).map_or(0, |x| *x)),
            F32(x) => format!("{}", x.get(0).map_or(0., |x| *x)),
            F64(x) => format!("{}", x.get(0).map_or(0., |x| *x)),
            Bool(x) => format!("{}", x.get(0).map_or(false, |x| *x)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
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

    fn unused_scalar_type(&self) -> ScalarType {
        match self {
            DataValue::Scalar(x) => match x {
                ScalarValue::U8(_) => ScalarType::U8,
                ScalarValue::U16(_) => ScalarType::U16,
                ScalarValue::U32(_) => ScalarType::U32,
                ScalarValue::U64(_) => ScalarType::U64,
                ScalarValue::I8(_) => ScalarType::I8,
                ScalarValue::I16(_) => ScalarType::I16,
                ScalarValue::I32(_) => ScalarType::I32,
                ScalarValue::I64(_) => ScalarType::I64,
                ScalarValue::F32(_) => ScalarType::F32,
                ScalarValue::F64(_) => ScalarType::F64,
                ScalarValue::Enum(..) => ScalarType::Enum,
                ScalarValue::String(_) => ScalarType::STRING,
                ScalarValue::Bool(_) => ScalarType::BOOL,
                ScalarValue::CaStatus(_) => ScalarType::I16,
            },
            DataValue::Array(x) => match x {
                ArrayValue::U8(_) => ScalarType::U8,
                ArrayValue::U16(_) => ScalarType::U16,
                ArrayValue::U32(_) => ScalarType::U32,
                ArrayValue::U64(_) => ScalarType::U64,
                ArrayValue::I8(_) => ScalarType::I8,
                ArrayValue::I16(_) => ScalarType::I16,
                ArrayValue::I32(_) => ScalarType::I32,
                ArrayValue::I64(_) => ScalarType::I64,
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

    pub fn string_short(&self) -> String {
        match self {
            DataValue::Scalar(x) => x.string_short(),
            DataValue::Array(x) => x.string_short(),
        }
    }
}

pub trait GetValHelp<T> {
    type ScalTy: Clone;
    fn get(&self) -> Result<&Self::ScalTy, Error>;
}

macro_rules! impl_scalar_get_val_help {
    ($sty:ty, $varname:ident) => {
        impl GetValHelp<$sty> for DataValue {
            type ScalTy = $sty;
            fn get(&self) -> Result<&Self::ScalTy, Error> {
                match self {
                    DataValue::Scalar(v) => match v {
                        ScalarValue::$varname(v) => Ok(v),
                        _ => {
                            //let ty = any::type_name::<Self::ScalTy>();
                            Err(Error::GetValHelpInnerTypeMismatch)
                        }
                    },
                    _ => Err(Error::GetValHelpTodoWaveform),
                }
            }
        }
    };
}

impl_scalar_get_val_help!(u8, U8);
impl_scalar_get_val_help!(u16, U16);
impl_scalar_get_val_help!(u32, U32);
impl_scalar_get_val_help!(u64, U64);
impl_scalar_get_val_help!(i8, I8);
impl_scalar_get_val_help!(i16, I16);
impl_scalar_get_val_help!(i32, I32);
impl_scalar_get_val_help!(i64, I64);
impl_scalar_get_val_help!(f32, F32);
impl_scalar_get_val_help!(f64, F64);

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    ConnectRefused,
    ConnectTimeout,
    IoError,
    ShutdownCommand,
    InternalError,
    Protocol,
    ProtocolMissing,
    IocTimeout,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct InsertItem {
    pub series: SeriesId,
    pub ts_msp: TsMs,
    pub ts_lsp: DtNano,
    pub msp_bump: bool,
    pub val: DataValue,
    pub ts_net: Instant,
}

impl InsertItem {
    pub fn string_short(&self) -> String {
        format!(
            "{}  {}  {}  {}",
            self.series.id(),
            self.ts_msp.ms(),
            self.ts_lsp.ms_u64(),
            self.val.string_short()
        )
    }
}

#[derive(Debug, Clone)]
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

// Needs to be Clone to send it to multiple retention times if required.
#[derive(Debug, Clone)]
pub enum QueryItem {
    ConnectionStatus(ConnectionStatusItem),
    ChannelStatus(ChannelStatusItem),
    Insert(InsertItem),
    TimeBinSimpleF32(TimeBinSimpleF32),
    Accounting(Accounting),
    AccountingRecv(AccountingRecv),
}

#[derive(Debug, Clone)]
pub struct Accounting {
    pub part: i32,
    pub ts: TsMs,
    pub series: SeriesId,
    pub count: i64,
    pub bytes: i64,
}

#[derive(Debug, Clone)]
pub struct AccountingRecv {
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
    ts_net: Instant,
    do_insert: bool,
    stats: Arc<InsertWorkerStats>,
}

fn insert_scalar_gen_fut<ST>(par: InsParCom, val: ST, qu: Arc<PreparedStatement>, scy: Arc<ScySession>) -> InsertFut
where
    ST: Value + SerializeCql + Send + 'static,
{
    let params = (par.series.to_i64(), par.ts_msp.to_i64(), par.ts_lsp.to_i64(), val);
    InsertFut::new(scy, qu, params, par.ts_net, par.stats)
}

fn insert_scalar_enum_gen_fut<ST1, ST2>(
    par: InsParCom,
    val: ST1,
    valstr: ST2,
    qu: Arc<PreparedStatement>,
    scy: Arc<ScySession>,
) -> InsertFut
where
    ST1: Value + SerializeCql + Send + 'static,
    ST2: Value + SerializeCql + Send + 'static,
{
    let params = (
        par.series.to_i64(),
        par.ts_msp.to_i64(),
        par.ts_lsp.to_i64(),
        val,
        valstr,
    );
    InsertFut::new(scy, qu, params, par.ts_net, par.stats)
}

// val: Vec<ST>   where ST: Value + SerializeCql + Send + 'static,
fn insert_array_gen_fut(par: InsParCom, val: Vec<u8>, qu: Arc<PreparedStatement>, scy: Arc<ScySession>) -> InsertFut {
    let params = (par.series.to_i64(), par.ts_msp.to_i64(), par.ts_lsp.to_i64(), val);
    InsertFut::new(scy, qu, params, par.ts_net, par.stats)
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
        tsnet: Instant,
        stats: Arc<InsertWorkerStats>,
    ) -> Self {
        let scy_ref = unsafe { NonNull::from(scy.as_ref()).as_ref() };
        let qu_ref = unsafe { NonNull::from(qu.as_ref()).as_ref() };
        let fut = scy_ref.execute_paged(qu_ref, params, None);
        let fut = fut.map(move |x| {
            let dt = tsnet.elapsed();
            let dt_ms = 1000 * dt.as_secs() as u32 + dt.subsec_millis();
            stats.item_lat_net_store().ingest(dt_ms);
            x
        });
        let fut = taskrun::tokio::task::unconstrained(fut);
        let fut = Box::pin(fut);
        // let fut = StackFuture::from(fut);
        Self { scy, qu, fut }
    }

    pub fn dummy(scy: Arc<ScySession>, qu: Arc<PreparedStatement>) -> Self {
        Self {
            scy,
            qu,
            fut: Box::pin(async { Err(QueryError::InvalidMessage("no longer used".into())) }),
        }
    }
}

impl Future for InsertFut {
    type Output = Result<QueryResult, QueryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.fut.poll_unpin(cx)
    }
}

pub fn insert_msp_fut(
    series: SeriesId,
    ts_msp: TsMs,
    // for stats, the timestamp when we received that data
    tsnet: Instant,
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
                ts_net: item.ts_net,
                do_insert,
                stats: stats.clone(),
            };
            use ScalarValue::*;
            match val {
                U8(val) => insert_scalar_gen_fut(par, val as i8, data_store.qu_insert_scalar_u8.clone(), scy),
                U16(val) => insert_scalar_gen_fut(par, val as i16, data_store.qu_insert_scalar_u16.clone(), scy),
                U32(val) => insert_scalar_gen_fut(par, val as i32, data_store.qu_insert_scalar_u32.clone(), scy),
                U64(val) => insert_scalar_gen_fut(par, val as i64, data_store.qu_insert_scalar_u64.clone(), scy),
                I8(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i8.clone(), scy),
                I16(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i16.clone(), scy),
                I32(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i32.clone(), scy),
                I64(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_i64.clone(), scy),
                F32(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_f32.clone(), scy),
                F64(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_f64.clone(), scy),
                Enum(val, valstr) => {
                    insert_scalar_enum_gen_fut(par, val, valstr, data_store.qu_insert_scalar_enum.clone(), scy)
                }
                String(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_string.clone(), scy),
                Bool(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_bool.clone(), scy),
                CaStatus(val) => insert_scalar_gen_fut(par, val, data_store.qu_insert_scalar_castatus.clone(), scy),
            }
        }
        Array(val) => {
            let par = InsParCom {
                series: item.series,
                ts_msp: item.ts_msp,
                ts_lsp: item.ts_lsp,
                ts_net: item.ts_net,
                do_insert,
                stats: stats.clone(),
            };
            use ArrayValue::*;
            let blob = val.to_binary_blob();
            #[allow(unused)]
            match val {
                U8(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_u8.clone(), scy),
                U16(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_u16.clone(), scy),
                U32(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_u32.clone(), scy),
                U64(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_u64.clone(), scy),
                I8(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i8.clone(), scy),
                I16(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i16.clone(), scy),
                I32(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i32.clone(), scy),
                I64(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_i64.clone(), scy),
                F32(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_f32.clone(), scy),
                F64(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_f64.clone(), scy),
                Bool(val) => insert_array_gen_fut(par, blob, data_store.qu_insert_array_bool.clone(), scy),
            }
        }
    }
}

#[cfg(DISABLED)]
pub fn insert_connection_status_fut(
    item: ConnectionStatusItem,
    data_store: &DataStore,
    stats: Arc<InsertWorkerStats>,
) -> InsertFut {
    warn!("separate connection status table no longer used");
    InsertFut::dummy(data_store.scy.clone(), data_store.qu_dummy.clone())
}

pub fn insert_connection_status_fut(
    item: ConnectionStatusItem,
    data_store: &DataStore,
    stats: Arc<InsertWorkerStats>,
) -> InsertFut {
    let tsnow = TsNano::from_system_time(item.ts);
    let (msp, lsp) = tsnow.to_ts_ms().to_grid_02(CONNECTION_STATUS_DIV);
    // TODO is that the good tsnet to use?
    let kind = item.status.to_kind();
    let addr = format!("{}", item.addr);
    let params = (msp.to_i64(), lsp.to_i64(), kind as i32, addr);
    InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_connection_status.clone(),
        params,
        Instant::now(),
        stats,
    )
}

#[cfg(DISABLED)]
pub fn insert_channel_status_fut(
    item: ChannelStatusItem,
    data_store: &DataStore,
    stats: Arc<InsertWorkerStats>,
) -> SmallVec<[InsertFut; 4]> {
    warn!("separate channel status table no longer used");
    SmallVec::new()
}

pub fn insert_channel_status_fut(
    item: ChannelStatusItem,
    data_store: &DataStore,
    stats: Arc<InsertWorkerStats>,
) -> SmallVec<[InsertFut; 4]> {
    let tsnow = TsNano::from_system_time(item.ts);
    let (msp, lsp) = tsnow.to_ts_ms().to_grid_02(CONNECTION_STATUS_DIV);
    let tsnet = Instant::now();
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

#[cfg(DISABLED)]
pub async fn insert_connection_status(item: ConnectionStatusItem, data_store: &DataStore) -> Result<(), Error> {
    warn!("separate connection status table no longer used");
    Ok(())
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

#[cfg(DISABLED)]
pub async fn insert_channel_status(item: ChannelStatusItem, data_store: &DataStore) -> Result<(), Error> {
    warn!("separate channel status table no longer used");
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
