mod enumfetch;

use super::proto;
use super::proto::CaEventValue;
use super::proto::ReadNotify;
use crate::ca::proto::ChannelClose;
use crate::ca::proto::EventCancel;
use crate::conf::ChannelConfig;
use crate::metrics::status::StorageUsage;
use crate::throttletrace::ThrottleTrace;
use async_channel::Receiver;
use async_channel::Sender;
use core::fmt;
use dbpg::seriesbychannel::ChannelInfoQuery;
use enumfetch::ConnFuture;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use log::*;
use netpod::timeunits::*;
use netpod::ttl::RetentionTime;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use netpod::EMIT_ACCOUNTING_SNAP;
use proto::CaItem;
use proto::CaMsg;
use proto::CaMsgTy;
use proto::CaProto;
use proto::CreateChan;
use proto::EventAdd;
use scywr::insertqueues::InsertDeques;
use scywr::insertqueues::InsertQueuesTx;
use scywr::insertqueues::InsertSenderPolling;
use scywr::iteminsertqueue as scywriiq;
use scywr::iteminsertqueue::Accounting;
use scywr::iteminsertqueue::AccountingRecv;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::ShutdownReason;
use scywr::senderpolling::SenderPolling;
use scywriiq::ChannelStatus;
use scywriiq::ChannelStatusClosedReason;
use scywriiq::ChannelStatusItem;
use scywriiq::ConnectionStatus;
use scywriiq::ConnectionStatusItem;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use serieswriter::binwriter::BinWriter;
use serieswriter::establish_worker::EstablishWorkerJob;
use serieswriter::establish_worker::JobId;
use serieswriter::rtwriter::RtWriter;
use stats::rand_xoshiro::rand_core::RngCore;
use stats::rand_xoshiro::rand_core::SeedableRng;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use stats::CaConnStats;
use stats::CaProtoStats;
use stats::IntervalEma;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tokio::net::TcpStream;

const CONNECTING_TIMEOUT: Duration = Duration::from_millis(6000);
const IOC_PING_IVL: Duration = Duration::from_millis(1000 * 80);
const DO_RATE_CHECK: bool = false;
const MONITOR_POLL_TIMEOUT: Duration = Duration::from_millis(6000);
const TIMEOUT_CHANNEL_CLOSING: Duration = Duration::from_millis(8000);
const TIMEOUT_PONG_WAIT: Duration = Duration::from_millis(10000);

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace3 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace4 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_flush_queue {
    ($($arg:tt)*) => {
        if false {
            trace3!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_event_incoming {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
pub enum Error {
    NoProtocol,
    ProtocolError,
    IocIssue,
    Protocol(#[from] crate::ca::proto::Error),
    RtWriter(#[from] serieswriter::rtwriter::Error),
    BinWriter(#[from] serieswriter::binwriter::Error),
    // TODO remove false positive from ThisError derive
    #[allow(private_interfaces)]
    UnknownCid(Cid),
    #[allow(private_interfaces)]
    NoNameForCid(Cid),
    CreateChannelBadState,
    CommonError(#[from] err::Error),
    LoopInnerLogicError,
    NoSender,
    NotSending,
    ClosedSending,
    NoProgressNoPending,
    ShutdownWithQueuesNoProgressNoPending,
    Error,
    DurationOutOfBounds,
    NoFreeCid,
    InsertQueues(#[from] scywr::insertqueues::Error),
    FutLogic,
}

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        err::Error::with_msg_no_trace(self.to_string())
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum ChannelConnectedInfo {
    Disconnected,
    Connecting,
    Connected,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChannelStateInfo {
    pub stnow: SystemTime,
    pub cssid: ChannelStatusSeriesId,
    pub addr: SocketAddrV4,
    pub series: Option<SeriesId>,
    pub channel_connected_info: ChannelConnectedInfo,
    pub scalar_type: Option<ScalarType>,
    pub shape: Option<Shape>,
    // NOTE: this solution can yield to the same Instant serialize to different string representations.
    // #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "ser_instant")]
    pub ts_created: Option<Instant>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "ser_instant")]
    pub ts_event_last: Option<Instant>,
    pub recv_count: Option<u64>,
    pub recv_bytes: Option<u64>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub item_recv_ivl_ema: Option<f32>,
    pub interest_score: f32,
    pub conf: ChannelConfig,
    pub recv_last: SystemTime,
    pub write_st_last: SystemTime,
    pub write_mt_last: SystemTime,
    pub write_lt_last: SystemTime,
}

mod ser_instant {
    use super::*;
    use netpod::DATETIME_FMT_3MS;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S>(val: &Option<Instant>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(val) => {
                let now = chrono::Utc::now();
                let tsnow = Instant::now();
                let t1 = if tsnow >= *val {
                    let dur = tsnow.duration_since(*val);
                    let dur2 = chrono::Duration::try_seconds(dur.as_secs() as i64)
                        .ok_or(Error::DurationOutOfBounds)
                        .unwrap()
                        .checked_add(&chrono::Duration::microseconds(dur.subsec_micros() as i64))
                        .unwrap();
                    now.checked_sub_signed(dur2).unwrap()
                } else {
                    let dur = (*val).duration_since(tsnow);
                    let dur2 = chrono::Duration::try_seconds(dur.as_secs() as i64)
                        .ok_or(Error::DurationOutOfBounds)
                        .unwrap()
                        .checked_sub(&chrono::Duration::microseconds(dur.subsec_micros() as i64))
                        .unwrap();
                    now.checked_add_signed(dur2).unwrap()
                };
                let s = t1.format(DATETIME_FMT_3MS).to_string();
                ser.serialize_str(&s)
            }
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(_de: D) -> Result<Option<Instant>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let e = serde::de::Error::custom("todo deserialize for ser_instant");
        Err(e)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Cid(pub u32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Subid(pub u32);

impl Subid {
    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Sid(pub u32);

impl Sid {
    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Ioid(pub u32);

#[derive(Clone, Debug)]
enum ChannelError {
    CreateChanFail(ChannelStatusSeriesId),
}

#[derive(Debug, Clone)]
struct CreatingState {
    tsbeg: Instant,
    cssid: ChannelStatusSeriesId,
    cid: Cid,
}

#[derive(Debug, Clone)]
struct MakingSeriesWriterState {
    tsbeg: Instant,
    channel: CreatedState,
}

#[derive(Debug, Clone)]
struct FetchEnumDetails {
    tsbeg: Instant,
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug, Clone)]
struct EnableMonitoringState {
    tsbeg: Instant,
    subid: Subid,
}

#[derive(Debug, Clone)]
struct ReadPendingState {
    tsbeg: Instant,
}

#[derive(Debug, Clone)]
struct Monitoring2PassiveState {
    // Holds instant when we entered this state. A receive of an event is considered a re-enter of the state,
    // so the instant gets updated. Used for timeout check.
    tsbeg: Instant,
}

#[derive(Debug, Clone)]
enum Monitoring2State {
    Passive(Monitoring2PassiveState),
    ReadPending(Ioid, Instant),
}

#[derive(Debug, Clone)]
struct MonitoringState {
    tsbeg: Instant,
    subid: Subid,
    mon2state: Monitoring2State,
}

#[derive(Debug, Clone)]
struct StopMonitoringForPollingState {
    tsbeg: Instant,
}

#[derive(Debug, Clone)]
struct PollingState {
    tsbeg: Instant,
    poll_ivl: Duration,
    tick: PollTickState,
}

#[derive(Debug, Clone)]
enum PollTickState {
    // TODO use inner struct to give this Instant a name.
    // When monitoring, update this ts on received events.
    // It should hold the Instant when we entered this state, but a receive of some event
    // is considered re-entering this state.
    Idle(Instant),
    // TODO use inner struct to give this Instant a name
    Wait(Instant, Ioid),
}

#[derive(Debug)]
struct WritableState {
    tsbeg: Instant,
    channel: CreatedState,
    writer: RtWriter,
    binwriter: BinWriter,
    reading: ReadingState,
}

#[derive(Debug, Clone)]
enum ReadingState {
    EnableMonitoring(EnableMonitoringState),
    Monitoring(MonitoringState),
    StopMonitoringForPolling(StopMonitoringForPollingState),
    Polling(PollingState),
}

#[derive(Debug, Clone)]
struct AccountingInfo {
    usage: StorageUsage,
    beg: TsMs,
}

impl AccountingInfo {
    fn new(beg: TsMs) -> Self {
        Self {
            usage: StorageUsage::new(),
            beg,
        }
    }

    fn push_written(&mut self, payload_len: u32) {
        self.usage.push_written(payload_len);
    }

    fn usage(&self) -> &StorageUsage {
        &self.usage
    }

    fn reset(&mut self, msp: TsMs) {
        self.beg = msp;
        self.usage.reset();
    }
}

#[derive(Debug, Clone)]
struct CreatedState {
    cssid: ChannelStatusSeriesId,
    cid: Cid,
    sid: Sid,
    ca_dbr_type: u16,
    ca_dbr_count: u32,
    ts_created: Instant,
    // Updated when we receive something via monitoring or polling
    ts_alive_last: Instant,
    // Updated on monitoring, polling or when the channel config changes to reset the timeout
    ts_activity_last: Instant,
    st_activity_last: SystemTime,
    ts_msp_last: u64,
    ts_msp_grid_last: u32,
    inserted_in_ts_msp: u64,
    insert_item_ivl_ema: IntervalEma,
    item_recv_ivl_ema: IntervalEma,
    insert_recv_ivl_last: Instant,
    muted_before: u32,
    info_store_msp_last: u32,
    recv_count: u64,
    recv_bytes: u64,
    stwin_ts: u64,
    stwin_count: u32,
    stwin_bytes: u32,
    acc_recv: AccountingInfo,
    acc_st: AccountingInfo,
    acc_mt: AccountingInfo,
    acc_lt: AccountingInfo,
    dw_st_last: SystemTime,
    dw_mt_last: SystemTime,
    dw_lt_last: SystemTime,
    scalar_type: ScalarType,
    shape: Shape,
    log_more: bool,
    name: String,
}

impl CreatedState {
    fn dummy() -> Self {
        let tsnow = Instant::now();
        let stnow = SystemTime::now();
        let (acc_msp, _) = TsMs::from_system_time(stnow).to_grid_02(EMIT_ACCOUNTING_SNAP);
        Self {
            cssid: ChannelStatusSeriesId::new(0),
            cid: Cid(0),
            sid: Sid(0),
            ca_dbr_type: 0,
            ca_dbr_count: 0,
            ts_created: tsnow,
            ts_alive_last: tsnow,
            ts_activity_last: tsnow,
            st_activity_last: stnow,
            ts_msp_last: 0,
            ts_msp_grid_last: 0,
            inserted_in_ts_msp: 0,
            insert_item_ivl_ema: IntervalEma::new(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: tsnow,
            muted_before: 0,
            info_store_msp_last: 0,
            recv_count: 0,
            recv_bytes: 0,
            stwin_ts: 0,
            stwin_count: 0,
            stwin_bytes: 0,
            acc_recv: AccountingInfo::new(acc_msp),
            acc_st: AccountingInfo::new(acc_msp),
            acc_mt: AccountingInfo::new(acc_msp),
            acc_lt: AccountingInfo::new(acc_msp),
            dw_st_last: SystemTime::UNIX_EPOCH,
            dw_mt_last: SystemTime::UNIX_EPOCH,
            dw_lt_last: SystemTime::UNIX_EPOCH,
            scalar_type: ScalarType::I8,
            shape: Shape::Scalar,
            log_more: false,
            name: String::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
enum ChannelState {
    Init(ChannelStatusSeriesId),
    Creating(CreatingState),
    FetchEnumDetails(FetchEnumDetails),
    MakingSeriesWriter(MakingSeriesWriterState),
    Writable(WritableState),
    Closing(ClosingState),
    Error(ChannelError),
    Ended(ChannelStatusSeriesId),
}

#[derive(Debug)]
struct ClosingState {
    tsbeg: Instant,
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug)]
struct ChannelConf {
    conf: ChannelConfig,
    state: ChannelState,
}

impl ChannelConf {
    pub fn poll_conf(&self) -> Option<(u64,)> {
        self.conf.poll_conf()
    }
}

impl ChannelState {
    fn to_info(
        &self,
        cssid: ChannelStatusSeriesId,
        addr: SocketAddrV4,
        conf: ChannelConfig,
        stnow: SystemTime,
    ) -> ChannelStateInfo {
        let channel_connected_info = match self {
            ChannelState::Init(..) => ChannelConnectedInfo::Disconnected,
            ChannelState::Creating { .. } => ChannelConnectedInfo::Connecting,
            ChannelState::FetchEnumDetails(_) => ChannelConnectedInfo::Connecting,
            ChannelState::MakingSeriesWriter(_) => ChannelConnectedInfo::Connecting,
            ChannelState::Writable(_) => ChannelConnectedInfo::Connected,
            ChannelState::Error(_) => ChannelConnectedInfo::Error,
            ChannelState::Ended(_) => ChannelConnectedInfo::Disconnected,
            ChannelState::Closing(_) => ChannelConnectedInfo::Disconnected,
        };
        let scalar_type = match self {
            ChannelState::Writable(s) => Some(s.writer.scalar_type().clone()),
            _ => None,
        };
        let shape = match self {
            ChannelState::Writable(s) => Some(s.writer.shape().clone()),
            _ => None,
        };
        let ts_created = match self {
            ChannelState::Writable(s) => Some(s.channel.ts_created.clone()),
            _ => None,
        };
        let ts_event_last = match self {
            ChannelState::Writable(s) => Some(s.channel.ts_alive_last),
            _ => None,
        };
        let recv_count = match self {
            ChannelState::Writable(s) => Some(s.channel.recv_count),
            _ => None,
        };
        let recv_bytes = match self {
            ChannelState::Writable(s) => Some(s.channel.recv_bytes),
            _ => None,
        };
        let (recv_last, write_st_last, write_mt_last, write_lt_last) = match self {
            ChannelState::Writable(s) => {
                let a = s.channel.st_activity_last;
                let b = s.channel.dw_st_last;
                let c = s.channel.dw_mt_last;
                let d = s.channel.dw_lt_last;
                (a, b, c, d)
            }
            _ => {
                let a = SystemTime::UNIX_EPOCH;
                (a, a, a, a)
            }
        };
        let item_recv_ivl_ema = match self {
            ChannelState::Writable(s) => {
                let ema = s.channel.item_recv_ivl_ema.ema();
                if ema.update_count() == 0 {
                    None
                } else {
                    Some(ema.ema())
                }
            }
            _ => None,
        };
        let series = match self {
            ChannelState::Writable(s) => Some(s.writer.sid()),
            _ => None,
        };
        let interest_score = 1. / item_recv_ivl_ema.unwrap_or(1e10).max(1e-6).min(1e10);
        ChannelStateInfo {
            stnow,
            cssid,
            addr,
            series,
            channel_connected_info,
            scalar_type,
            shape,
            ts_created,
            ts_event_last,
            recv_count,
            recv_bytes,
            item_recv_ivl_ema,
            interest_score,
            conf,
            recv_last,
            write_st_last,
            write_mt_last,
            write_lt_last,
        }
    }

    fn cssid(&self) -> ChannelStatusSeriesId {
        match self {
            ChannelState::Init(cssid) => cssid.clone(),
            ChannelState::Creating(st) => st.cssid.clone(),
            ChannelState::FetchEnumDetails(st) => st.cssid.clone(),
            ChannelState::MakingSeriesWriter(st) => st.channel.cssid.clone(),
            ChannelState::Writable(st) => st.channel.cssid.clone(),
            ChannelState::Error(e) => match e {
                ChannelError::CreateChanFail(cssid) => cssid.clone(),
            },
            ChannelState::Ended(cssid) => cssid.clone(),
            ChannelState::Closing(st) => st.cssid.clone(),
        }
    }
}

enum CaConnState {
    Unconnected(Instant),
    Connecting(
        Instant,
        SocketAddrV4,
        Pin<Box<dyn Future<Output = Result<Result<TcpStream, std::io::Error>, tokio::time::error::Elapsed>> + Send>>,
    ),
    Init,
    Handshake,
    PeerReady,
    Shutdown(EndOfStreamReason),
    EndOfStream,
}

impl fmt::Debug for CaConnState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Unconnected(since) => fmt.debug_tuple("Unconnected").field(since).finish(),
            Self::Connecting(since, addr, _) => fmt.debug_tuple("Connecting").field(since).field(addr).finish(),
            Self::Init => fmt.debug_tuple("Init").finish(),
            Self::Handshake => fmt.debug_tuple("Handshake").finish(),
            Self::PeerReady => fmt.debug_tuple("PeerReady").finish(),
            Self::Shutdown(v0) => fmt.debug_tuple("Shutdown").field(v0).finish(),
            Self::EndOfStream => fmt.debug_tuple("EndOfStream").finish(),
        }
    }
}

struct CidStore {
    cnt: u32,
    rng: Xoshiro128PlusPlus,
}

impl CidStore {
    fn new(seed: u32) -> Self {
        Self {
            cnt: 0,
            rng: Xoshiro128PlusPlus::seed_from_u64(seed as _),
        }
    }

    fn new_from_time() -> Self {
        Self::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos(),
        )
    }

    fn next(&mut self) -> Cid {
        if true {
            let cnt = self.cnt;
            self.cnt += 1;
            return Cid(cnt);
        }
        let c = self.cnt << 8;
        self.cnt += 1;
        let r = self.rng.next_u32();
        Cid(c | ((r ^ (r >> 8) ^ (r >> 16) ^ (r >> 24)) & 0xff))
    }
}

struct SubidStore {
    cnt: u32,
    rng: Xoshiro128PlusPlus,
}

impl SubidStore {
    fn new(seed: u32) -> Self {
        Self {
            cnt: 0,
            rng: Xoshiro128PlusPlus::seed_from_u64(seed as _),
        }
    }

    fn new_from_time() -> Self {
        Self::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos(),
        )
    }

    fn next(&mut self) -> Subid {
        if true {
            let cnt = self.cnt;
            self.cnt += 1;
            return Subid(cnt);
        }
        let c = self.cnt << 8;
        self.cnt += 1;
        let r = self.rng.next_u32();
        let r = r ^ (r >> 8);
        let r = r ^ (r >> 8);
        let r = r ^ (r >> 8);
        Subid(c | r)
    }
}

fn info_store_msp_from_time(ts: SystemTime) -> u32 {
    let dt = ts.duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO);
    (dt.as_secs() / 60 * 60) as u32
}

pub type CmdResTx = Sender<Result<(), Error>>;

#[derive(Debug)]
pub enum ConnCommandKind {
    ChannelAdd(ChannelConfig, ChannelStatusSeriesId),
    ChannelClose(String),
    Shutdown,
}

#[derive(Debug)]
pub struct ConnCommand {
    id: usize,
    kind: ConnCommandKind,
}

impl ConnCommand {
    pub fn channel_add(conf: ChannelConfig, cssid: ChannelStatusSeriesId) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelAdd(conf, cssid),
        }
    }

    pub fn channel_close(name: String) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelClose(name),
        }
    }

    pub fn shutdown() -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::Shutdown,
        }
    }

    fn make_id() -> usize {
        static ID: AtomicUsize = AtomicUsize::new(0);
        ID.fetch_add(1, atomic::Ordering::AcqRel)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
pub struct ChannelStatusPartial {
    pub channel_statuses: BTreeMap<ChannelStatusSeriesId, ChannelStateInfo>,
}

#[derive(Debug)]
pub enum ConnCommandResultKind {
    Unused,
}

#[derive(Debug)]
pub struct ConnCommandResult {
    pub id: usize,
    pub kind: ConnCommandResultKind,
}

impl ConnCommandResult {
    pub fn id(&self) -> usize {
        self.id
    }

    fn make_id() -> usize {
        static ID: AtomicUsize = AtomicUsize::new(0);
        ID.fetch_add(1, atomic::Ordering::AcqRel)
    }
}

#[derive(Debug)]
pub struct CaConnEvent {
    pub ts: Instant,
    pub value: CaConnEventValue,
}

impl CaConnEvent {
    pub fn new(ts: Instant, value: CaConnEventValue) -> Self {
        Self { ts, value }
    }

    pub fn err_now(err: Error) -> Self {
        Self::new_now(CaConnEventValue::EndOfStream(EndOfStreamReason::Error(err)))
    }

    pub fn new_now(value: CaConnEventValue) -> Self {
        Self {
            ts: Instant::now(),
            value,
        }
    }

    pub fn desc_short(&self) -> CaConnEventDescShort {
        CaConnEventDescShort {}
    }
}

pub struct CaConnEventDescShort {}

impl fmt::Display for CaConnEventDescShort {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "CaConnEventDescShort {{ TODO-impl }}")
    }
}

#[derive(Debug)]
pub enum CaConnEventValue {
    None,
    EchoTimeout,
    ConnCommandResult(ConnCommandResult),
    ChannelStatus(ChannelStatusPartial),
    ChannelCreateFail(String),
    EndOfStream(EndOfStreamReason),
}

#[derive(Debug)]
pub enum EndOfStreamReason {
    UnspecifiedReason,
    Error(Error),
    ConnectRefused,
    ConnectTimeout,
    OnCommand,
    RemoteClosed,
    IocTimeout,
    IoError,
}

pub struct CaConnOpts {
    insert_queue_max: usize,
    array_truncate: usize,
}

impl CaConnOpts {
    pub fn with_insert_queue_max(mut self, val: usize) -> Self {
        self.insert_queue_max = val;
        self
    }
}

impl Default for CaConnOpts {
    fn default() -> Self {
        Self {
            insert_queue_max: 20000,
            array_truncate: 2000000,
        }
    }
}

pub struct CaConn {
    opts: CaConnOpts,
    backend: String,
    state: CaConnState,
    ticker: Pin<Box<tokio::time::Sleep>>,
    proto: Option<CaProto>,
    cid_store: CidStore,
    subid_store: SubidStore,
    channels: HashMap<Cid, ChannelConf>,
    // btree because require order:
    cid_by_name: BTreeMap<String, Cid>,
    cid_by_subid: HashMap<Subid, Cid>,
    cid_by_sid: HashMap<Sid, Cid>,
    channel_status_emit_next: Instant,
    tick_last_writer: Instant,
    init_state_count: u64,
    iqdqs: InsertDeques,
    remote_addr_dbg: SocketAddrV4,
    local_epics_hostname: String,
    stats: Arc<CaConnStats>,
    conn_command_tx: Pin<Box<Sender<ConnCommand>>>,
    conn_command_rx: Pin<Box<Receiver<ConnCommand>>>,
    conn_backoff: f32,
    conn_backoff_beg: f32,
    ioc_ping_last: Instant,
    ioc_ping_next: Instant,
    ioc_ping_start: Option<Instant>,
    iqsp: Pin<Box<InsertSenderPolling>>,
    ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    ca_conn_event_out_queue_max: usize,
    thr_msg_poll: ThrottleTrace,
    ca_proto_stats: Arc<CaProtoStats>,
    weird_count: usize,
    rng: Xoshiro128PlusPlus,
    writer_establish_qu: VecDeque<EstablishWorkerJob>,
    writer_establish_tx: Pin<Box<SenderPolling<EstablishWorkerJob>>>,
    writer_tx: Sender<(JobId, Result<RtWriter, serieswriter::rtwriter::Error>)>,
    writer_rx: Pin<Box<Receiver<(JobId, Result<RtWriter, serieswriter::rtwriter::Error>)>>>,
    tmp_ts_poll: SystemTime,
    poll_tsnow: Instant,
    ioid: u32,
    read_ioids: HashMap<Ioid, Cid>,
    handler_by_ioid: HashMap<Ioid, Option<Pin<Box<dyn ConnFuture>>>>,
}

impl Drop for CaConn {
    fn drop(&mut self) {
        debug!("drop CaConn {}", self.remote_addr_dbg);
    }
}

impl CaConn {
    pub fn new(
        opts: CaConnOpts,
        backend: String,
        remote_addr_dbg: SocketAddrV4,
        local_epics_hostname: String,
        iqtx: InsertQueuesTx,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        stats: Arc<CaConnStats>,
        ca_proto_stats: Arc<CaProtoStats>,
        writer_establish_tx: Sender<EstablishWorkerJob>,
    ) -> Self {
        let _ = channel_info_query_tx;
        let tsnow = Instant::now();
        let (writer_tx, writer_rx) = async_channel::bounded(32);
        let (cq_tx, cq_rx) = async_channel::bounded(32);
        let mut rng = stats::xoshiro_from_time();
        Self {
            opts,
            backend,
            state: CaConnState::Unconnected(tsnow),
            ticker: Self::new_self_ticker(),
            proto: None,
            cid_store: CidStore::new_from_time(),
            subid_store: SubidStore::new_from_time(),
            init_state_count: 0,
            channels: HashMap::new(),
            cid_by_name: BTreeMap::new(),
            cid_by_subid: HashMap::new(),
            cid_by_sid: HashMap::new(),
            channel_status_emit_next: tsnow + Self::channel_status_emit_ivl(&mut rng),
            tick_last_writer: tsnow,
            iqdqs: InsertDeques::new(),
            remote_addr_dbg,
            local_epics_hostname,
            stats,
            conn_command_tx: Box::pin(cq_tx),
            conn_command_rx: Box::pin(cq_rx),
            conn_backoff: 0.02,
            conn_backoff_beg: 0.02,
            ioc_ping_last: tsnow,
            ioc_ping_next: tsnow + Self::ioc_ping_ivl_rng(&mut rng),
            ioc_ping_start: None,
            iqsp: Box::pin(InsertSenderPolling::new(iqtx)),
            ca_conn_event_out_queue: VecDeque::new(),
            ca_conn_event_out_queue_max: 2000,
            thr_msg_poll: ThrottleTrace::new(Duration::from_millis(10000)),
            ca_proto_stats,
            weird_count: 0,
            rng,
            writer_establish_qu: VecDeque::new(),
            writer_establish_tx: Box::pin(SenderPolling::new(writer_establish_tx)),
            writer_tx,
            writer_rx: Box::pin(writer_rx),
            tmp_ts_poll: SystemTime::now(),
            poll_tsnow: tsnow,
            ioid: 100,
            read_ioids: HashMap::new(),
            handler_by_ioid: HashMap::new(),
        }
    }

    fn ioc_ping_ivl_rng(rng: &mut Xoshiro128PlusPlus) -> Duration {
        IOC_PING_IVL * 100 / (70 + (rng.next_u32() % 60))
    }

    fn channel_status_emit_ivl(rng: &mut Xoshiro128PlusPlus) -> Duration {
        Duration::from_millis(6000 + (rng.next_u32() & 0x7ff) as u64)
    }

    fn new_self_ticker() -> Pin<Box<tokio::time::Sleep>> {
        Box::pin(tokio::time::sleep(Duration::from_millis(500)))
    }

    fn proto(&mut self) -> Option<&mut CaProto> {
        self.proto.as_mut()
    }

    fn ioid_next(&mut self) -> Ioid {
        self.ioid = self.ioid.wrapping_add(1);
        Ioid(self.ioid)
    }

    pub fn conn_command_tx(&self) -> Sender<ConnCommand> {
        self.conn_command_tx.as_ref().get_ref().clone()
    }

    fn is_shutdown(&self) -> bool {
        if let CaConnState::Shutdown(..) = self.state {
            true
        } else {
            false
        }
    }

    fn trigger_shutdown(&mut self, reason: ShutdownReason) {
        let channel_reason = match &reason {
            ShutdownReason::ConnectRefused => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::ConnectRefused);
                ChannelStatusClosedReason::ConnectFail
            }
            ShutdownReason::ConnectTimeout => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::ConnectTimeout);
                ChannelStatusClosedReason::ConnectFail
            }
            ShutdownReason::IoError => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::IoError);
                ChannelStatusClosedReason::IoError
            }
            ShutdownReason::ShutdownCommand => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::OnCommand);
                ChannelStatusClosedReason::ShutdownCommand
            }
            ShutdownReason::InternalError => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::Error(Error::Error));
                ChannelStatusClosedReason::InternalError
            }
            ShutdownReason::Protocol => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::Error(Error::ProtocolError));
                ChannelStatusClosedReason::ProtocolError
            }
            ShutdownReason::ProtocolMissing => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::Error(Error::ProtocolError));
                ChannelStatusClosedReason::ProtocolError
            }
            ShutdownReason::IocTimeout => {
                self.state = CaConnState::Shutdown(EndOfStreamReason::IoError);
                ChannelStatusClosedReason::IocTimeout
            }
        };
        self.channel_state_on_shutdown(channel_reason);
        let addr = self.remote_addr_dbg.clone();
        // TODO handle Err:
        let _ = self
            .iqdqs
            .emit_status_item(QueryItem::ConnectionStatus(ConnectionStatusItem {
                ts: self.tmp_ts_poll,
                addr,
                // TODO map to appropriate status
                status: ConnectionStatus::Closing,
            }));
        self.proto = None;
    }

    fn cmd_channel_close(&mut self, name: String) {
        self.channel_close(name);
        // TODO return the result
        //self.stats.caconn_command_can_not_reply.inc();
    }

    fn cmd_shutdown(&mut self) {
        debug!("cmd_shutdown {}", self.remote_addr_dbg);
        self.trigger_shutdown(ShutdownReason::ShutdownCommand);
    }

    fn handle_conn_command(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        self.stats.loop3_count.inc();
        if self.is_shutdown() {
            Ok(Ready(None))
        } else {
            let rx = self.conn_command_rx.as_mut();
            match rx.poll_next(cx) {
                Ready(Some(a)) => {
                    trace3!("handle_conn_command received a command  {}", self.remote_addr_dbg);
                    match a.kind {
                        ConnCommandKind::ChannelAdd(conf, cssid) => {
                            self.channel_add(conf, cssid);
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::ChannelClose(name) => {
                            self.cmd_channel_close(name);
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::Shutdown => {
                            self.cmd_shutdown();
                            Ok(Ready(Some(())))
                        }
                    }
                }
                Ready(None) => {
                    error!("command queue closed");
                    Ok(Ready(None))
                }
                Pending => Ok(Pending),
            }
        }
    }

    fn handle_writer_establish_result(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        if self.is_shutdown() {
            Ok(Ready(None))
        } else {
            let rx = self.writer_rx.as_mut();
            match rx.poll_next(cx) {
                Ready(Some(res)) => {
                    trace!("handle_writer_establish_result  recv  {}", self.remote_addr_dbg);
                    let jobid = res.0;
                    // by convention:
                    let cid = Cid(jobid.0 as _);
                    let wr = res.1?;
                    self.handle_writer_establish_inner(cid, wr)?;
                    Ok(Ready(Some(())))
                }
                Ready(None) => {
                    error!("writer_establish queue closed");
                    Ok(Ready(None))
                }
                Pending => Ok(Pending),
            }
        }
    }

    fn handle_writer_establish_inner(&mut self, cid: Cid, writer: RtWriter) -> Result<(), Error> {
        trace!("handle_writer_establish_inner  {cid:?}");
        let stnow = self.tmp_ts_poll.clone();
        // At this point we have created the channel and created a writer for that type and sid.
        // We do not yet monitor.
        // TODO main objectives now:
        // Store the writer with the channel state.
        // Create a monitor for the channel.
        // NOTE: must store the Writer even if not yet in Evented, we could also transition to Polled!
        if let Some(conf) = self.channels.get_mut(&cid) {
            // TODO refactor, should only execute this when required:
            let conf_poll_conf = conf.poll_conf();
            let chst = &mut conf.state;
            if let ChannelState::MakingSeriesWriter(st2) = chst {
                let dt = stnow.duration_since(SystemTime::UNIX_EPOCH).unwrap();
                let beg = TsNano::from_ns(SEC * dt.as_secs() + dt.subsec_nanos() as u64);
                let binwriter = BinWriter::new(
                    beg,
                    RetentionTime::Short,
                    st2.channel.cssid,
                    writer.sid(),
                    st2.channel.scalar_type.clone(),
                    st2.channel.shape.clone(),
                )?;
                self.stats.get_series_id_ok.inc();
                {
                    let item = QueryItem::ChannelStatus(ChannelStatusItem {
                        ts: self.tmp_ts_poll,
                        cssid: st2.channel.cssid.clone(),
                        status: ChannelStatus::Opened,
                    });
                    self.iqdqs.emit_status_item(item);
                }
                if let Some((ivl,)) = conf_poll_conf {
                    let created_state = WritableState {
                        tsbeg: self.poll_tsnow,
                        channel: std::mem::replace(&mut st2.channel, CreatedState::dummy()),
                        // channel: st2.channel.clone(),
                        writer,
                        binwriter,
                        reading: ReadingState::Polling(PollingState {
                            tsbeg: self.poll_tsnow,
                            poll_ivl: Duration::from_millis(ivl),
                            tick: PollTickState::Idle(self.poll_tsnow),
                        }),
                    };
                    conf.state = ChannelState::Writable(created_state);
                    Ok(())
                } else {
                    let subid = {
                        let subid = self.subid_store.next();
                        self.cid_by_subid.insert(subid, cid);
                        trace!(
                            "new {:?} for {:?}  chst {:?} {:?}",
                            subid,
                            cid,
                            st2.channel.cid,
                            st2.channel.sid
                        );
                        subid
                    };
                    {
                        trace!("send out EventAdd for {cid:?}");
                        let ty = CaMsgTy::EventAdd(EventAdd {
                            sid: st2.channel.sid.to_u32(),
                            data_type: st2.channel.ca_dbr_type,
                            data_count: st2.channel.ca_dbr_count,
                            subid: subid.to_u32(),
                        });
                        let msg = CaMsg::from_ty_ts(ty, self.poll_tsnow);
                        let proto = self.proto.as_mut().unwrap();
                        proto.push_out(msg);
                    }
                    let created_state = WritableState {
                        tsbeg: self.poll_tsnow,
                        channel: std::mem::replace(&mut st2.channel, CreatedState::dummy()),
                        writer,
                        binwriter,
                        reading: ReadingState::EnableMonitoring(EnableMonitoringState {
                            tsbeg: self.poll_tsnow,
                            subid,
                        }),
                    };
                    conf.state = ChannelState::Writable(created_state);
                    Ok(())
                }
            } else {
                warn!("TODO handle_series_lookup_result channel in bad state, reset");
                Ok(())
            }
        } else {
            warn!("TODO handle_series_lookup_result channel in bad state, reset");
            Ok(())
        }
    }

    pub fn stats(&self) -> Arc<CaConnStats> {
        self.stats.clone()
    }

    pub fn channel_add(&mut self, conf: ChannelConfig, cssid: ChannelStatusSeriesId) -> Result<(), Error> {
        if self.cid_by_name(conf.name()).is_some() {
            self.stats.channel_add_exists.inc();
            error!("logic error channel already exists {conf:?}");
            Ok(())
        } else {
            let cid = self.cid_by_name_or_insert(conf.name())?;
            if self.channels.contains_key(&cid) {
                self.stats.channel_add_exists.inc();
                error!("logic error channel already exists {conf:?}");
                Ok(())
            } else {
                let conf = ChannelConf {
                    conf,
                    state: ChannelState::Init(cssid),
                };
                self.channels.insert(cid, conf);
                // TODO do not count, use separate queue for those channels.
                self.init_state_count += 1;
                Ok(())
            }
        }
    }

    pub fn channel_close(&mut self, name: String) {
        error!("TODO actually cause the channel to get closed and removed  {}", name);
    }

    pub fn channel_remove(&mut self, name: String) {
        if let Some(cid) = self.cid_by_name(&name) {
            self.channel_remove_by_cid(cid);
        } else {
            warn!("channel_remove  does not exist  {name}");
        }
    }

    fn channel_remove_by_cid(&mut self, cid: Cid) {
        self.cid_by_name.retain(|_, v| *v != cid);
        self.channels.remove(&cid);
    }

    fn cid_by_name(&self, name: &str) -> Option<Cid> {
        self.cid_by_name.get(name).map(Clone::clone)
    }

    fn cid_by_name_or_insert(&mut self, name: &str) -> Result<Cid, Error> {
        if let Some(cid) = self.cid_by_name.get(name) {
            Ok(*cid)
        } else {
            let mut found = None;
            for _ in 0..1000 {
                let cid = self.cid_store.next();
                if !self.channels.contains_key(&cid) {
                    self.cid_by_name.insert(name.into(), cid);
                    found = Some(cid);
                    break;
                }
            }
            found.ok_or(Error::NoFreeCid)
        }
    }

    fn name_by_cid(&self, cid: Cid) -> Option<&str> {
        self.channels.get(&cid).map(|x| x.conf.name())
    }

    fn backoff_next(&mut self) -> u64 {
        let dt = (self.conn_backoff * 300. * 1e3) as u64;
        self.conn_backoff = (self.conn_backoff * 2.).tanh();
        dt
    }

    fn backoff_reset(&mut self) {
        self.conn_backoff = self.conn_backoff_beg;
    }

    fn channel_state_on_shutdown(&mut self, channel_reason: ChannelStatusClosedReason) {
        // TODO  can I reuse emit_channel_info_insert_items ?
        trace!("channel_state_on_shutdown  channels {}", self.channels.len());
        for (_cid, conf) in &mut self.channels {
            let chst = &mut conf.state;
            match chst {
                ChannelState::Init(cssid) => {
                    *chst = ChannelState::Ended(cssid.clone());
                }
                ChannelState::Creating(st2) => {
                    *chst = ChannelState::Ended(st2.cssid.clone());
                }
                ChannelState::FetchEnumDetails(st) => {
                    *chst = ChannelState::Ended(st.cssid.clone());
                }
                ChannelState::MakingSeriesWriter(st) => {
                    *chst = ChannelState::Ended(st.channel.cssid.clone());
                }
                ChannelState::Writable(st2) => {
                    let cssid = st2.channel.cssid.clone();
                    // TODO should call the proper channel-close handler which in turn emits the status item.
                    // Make sure I record the reason for the "Close": user command, IOC error, etc..
                    let item = QueryItem::ChannelStatus(ChannelStatusItem {
                        ts: self.tmp_ts_poll,
                        cssid: cssid.clone(),
                        status: ChannelStatus::Closed(channel_reason.clone()),
                    });
                    self.iqdqs.emit_status_item(item);
                    *chst = ChannelState::Ended(cssid);
                }
                ChannelState::Error(..) => {
                    warn!("TODO emit error status");
                    // *chst = ChannelState::Ended;
                }
                ChannelState::Ended(_) => {}
                ChannelState::Closing(_) => {}
            }
        }
    }

    fn emit_channel_info_insert_items(&mut self) -> Result<(), Error> {
        let timenow = self.tmp_ts_poll;
        for (_, conf) in &mut self.channels {
            let st = &mut conf.state;
            match st {
                ChannelState::Init(..) => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::FetchEnumDetails(..) => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::Creating(..) => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::MakingSeriesWriter(..) => {
                    // TODO ?
                }
                ChannelState::Writable(st) => {
                    let crst = &mut st.channel;
                    // TODO if we don't wave a series id yet, dont' save? write-ampl.
                    let msp = info_store_msp_from_time(timenow.clone());
                    if msp != crst.info_store_msp_last {
                        crst.info_store_msp_last = msp;
                    }
                }
                ChannelState::Error(_) => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::Ended(_) => {}
                ChannelState::Closing(_) => {}
            }
        }
        Ok(())
    }

    fn transition_to_polling(&mut self, subid: Subid, tsnow: Instant) -> Result<(), Error> {
        let cid = if let Some(x) = self.cid_by_subid.get(&subid) {
            *x
        } else {
            self.stats.unknown_subid().inc();
            return Ok(());
        };
        let ch_s = if let Some(x) = self.channels.get_mut(&cid) {
            &mut x.state
        } else {
            // TODO return better as error and let caller decide (with more structured errors)
            // TODO
            // When removing a channel, keep it in "closed" btree for some time because messages can
            // still arrive from all buffers.
            // If we don't have it in the "closed" btree, then close connection to the IOC and count
            // as logic error.
            // Close connection to the IOC. Cout as logic error.
            let e = Error::UnknownCid(cid);
            error!("{e}");
            return Err(e);
        };
        if let ChannelState::Writable(st2) = ch_s {
            // TODO emit
            // TODO for cancel, must supply again the same DBR type and count as in the EventAdd.
            let ty = CaMsgTy::EventCancel(EventCancel {
                data_type: st2.channel.ca_dbr_type,
                data_count: st2.channel.ca_dbr_count,
                sid: st2.channel.sid.to_u32(),
                subid: subid.to_u32(),
            });
            let msg = CaMsg::from_ty_ts(ty, self.poll_tsnow);
            let proto = self.proto.as_mut().unwrap();
            proto.push_out(msg);
            st2.reading = ReadingState::StopMonitoringForPolling(StopMonitoringForPollingState { tsbeg: tsnow });
        } else {
            warn!("can not transition to polling, channel not established");
        }
        Ok(())
    }

    fn handle_event_add_res(&mut self, ev: proto::EventAddRes, tsnow: Instant) -> Result<(), Error> {
        let subid = Subid(ev.subid);
        // TODO handle subid-not-found which can also be peer error:
        let cid = if let Some(x) = self.cid_by_subid.get(&subid) {
            *x
        } else {
            warn!("can not find cid for subid {subid:?}");
            // return Err(Error::with_msg_no_trace());
            return Ok(());
        };
        let ch_s = if let Some(x) = self.channels.get_mut(&cid) {
            &mut x.state
        } else {
            // TODO return better as error and let caller decide (with more structured errors)
            warn!("TODO handle_event_add_res can not find channel for  {cid:?}  {subid:?}");
            // TODO
            // When removing a channel, keep it in "closed" btree for some time because messages can
            // still arrive from all buffers.
            // If we don't have it in the "closed" btree, then close connection to the IOC and count
            // as logic error.
            // Close connection to the IOC. Cout as logic error.
            // return Err(Error::with_msg_no_trace());
            return Ok(());
        };
        trace!("handle_event_add_res {:?}", ch_s.cssid());
        match ch_s {
            ChannelState::Writable(st) => {
                // debug!(
                //     "CaConn sees  data_count {}  payload_len {}",
                //     ev.data_count, ev.payload_len
                // );
                let stnow = self.tmp_ts_poll;
                let crst = &mut st.channel;
                let stwin_ts = stnow.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() / 4;
                if crst.stwin_ts != stwin_ts {
                    crst.stwin_ts = stwin_ts;
                    crst.stwin_count = 0;
                }
                if DO_RATE_CHECK {
                    crst.stwin_count += 1;
                    crst.stwin_bytes += ev.payload_len;
                    if crst.stwin_count > 30000 || crst.stwin_bytes > 1024 * 1024 * 500 {
                        let subid = match &mut st.reading {
                            ReadingState::EnableMonitoring(x) => Some(x.subid.clone()),
                            ReadingState::Monitoring(x) => {
                                match x.mon2state {
                                    // actually, no differing behavior needed so far.
                                    Monitoring2State::Passive(_) => {}
                                    Monitoring2State::ReadPending(ioid, since) => {}
                                }
                                Some(x.subid.clone())
                            }
                            ReadingState::StopMonitoringForPolling(_) => {
                                self.stats.transition_to_polling_bad_state().inc();
                                None
                            }
                            ReadingState::Polling(_) => {
                                self.stats.transition_to_polling_already_in().inc();
                                None
                            }
                        };
                        if let Some(subid) = subid {
                            self.stats.transition_to_polling().inc();
                            self.transition_to_polling(subid, tsnow)?;
                        } else {
                            self.stats.transition_to_polling_bad_state().inc();
                        }
                        return Ok(());
                    }
                }
                match &mut st.reading {
                    ReadingState::EnableMonitoring(st2) => {
                        let dt = st2.tsbeg.elapsed().as_secs_f32();
                        trace!("change to Monitoring after dt {dt:.0} ms");
                        st.reading = ReadingState::Monitoring(MonitoringState {
                            tsbeg: tsnow,
                            subid: st2.subid,
                            mon2state: Monitoring2State::Passive(Monitoring2PassiveState { tsbeg: tsnow }),
                        });
                        let crst = &mut st.channel;
                        let writer = &mut st.writer;
                        let binwriter = &mut st.binwriter;
                        let iqdqs = &mut self.iqdqs;
                        let stats = self.stats.as_ref();
                        Self::event_add_ingest(
                            ev.payload_len,
                            ev.value,
                            crst,
                            writer,
                            binwriter,
                            iqdqs,
                            tsnow,
                            stnow,
                            stats,
                        )?;
                    }
                    ReadingState::Monitoring(st2) => {
                        match &mut st2.mon2state {
                            Monitoring2State::Passive(st3) => {
                                st3.tsbeg = tsnow;
                            }
                            Monitoring2State::ReadPending(_ioid, _since) => {
                                // Received EventAdd while still waiting for answer to explicit ReadNotify.
                                // This is fine.
                                self.stats.recv_event_add_while_wait_on_read_notify.inc();
                            }
                        }
                        let crst = &mut st.channel;
                        let writer = &mut st.writer;
                        let binwriter = &mut st.binwriter;
                        let iqdqs = &mut self.iqdqs;
                        let stats = self.stats.as_ref();
                        Self::event_add_ingest(
                            ev.payload_len,
                            ev.value,
                            crst,
                            writer,
                            binwriter,
                            iqdqs,
                            tsnow,
                            stnow,
                            stats,
                        )?;
                    }
                    ReadingState::StopMonitoringForPolling(st2) => {
                        // TODO count for metrics
                        if st2.tsbeg + Duration::from_millis(2000) < tsnow {
                            error!("TODO  handle_event_add_res  handle StopMonitoringForPolling");
                            std::process::exit(1);
                        }
                    }
                    ReadingState::Polling(st2) => {
                        // TODO count for metrics
                        if st2.tsbeg + Duration::from_millis(2000) < tsnow {
                            error!("TODO  handle_event_add_res  handle Polling");
                            std::process::exit(1);
                        }
                    }
                }
            }
            ChannelState::Creating(_)
            | ChannelState::Init(_)
            | ChannelState::FetchEnumDetails(_)
            | ChannelState::MakingSeriesWriter(_) => {
                self.stats.recv_read_notify_but_not_init_yet.inc();
            }
            ChannelState::Closing(_) | ChannelState::Ended(_) | ChannelState::Error(_) => {
                self.stats.recv_read_notify_but_no_longer_ready.inc();
            }
        }
        Ok(())
    }

    fn handle_event_add_res_empty(&mut self, ev: proto::EventAddResEmpty, tsnow: Instant) -> Result<(), Error> {
        let subid = Subid(ev.subid);
        // TODO handle subid-not-found which can also be peer error:
        let cid = if let Some(x) = self.cid_by_subid.get(&subid) {
            *x
        } else {
            warn!("can not find cid for subid {subid:?}");
            // return Err(Error::with_msg_no_trace());
            return Ok(());
        };
        let ch_s = if let Some(x) = self.channels.get_mut(&cid) {
            &mut x.state
        } else {
            // TODO return better as error and let caller decide (with more structured errors)
            warn!("TODO handle_event_add_res can not find channel for  {cid:?}  {subid:?}");
            // TODO
            // When removing a channel, keep it in "closed" btree for some time because messages can
            // still arrive from all buffers.
            // If we don't have it in the "closed" btree, then close connection to the IOC and count
            // as logic error.
            // Close connection to the IOC. Cout as logic error.
            // return Err(Error::with_msg_no_trace());
            return Ok(());
        };
        // debug!("handle_event_add_res {ev:?}");
        match ch_s {
            ChannelState::Writable(st) => match &mut st.reading {
                ReadingState::StopMonitoringForPolling(..) => {
                    st.reading = ReadingState::Polling(PollingState {
                        tsbeg: tsnow,
                        poll_ivl: Duration::from_millis(1000),
                        tick: PollTickState::Idle(tsnow),
                    });
                }
                ReadingState::EnableMonitoring(..) => {
                    let name = self.name_by_cid(cid);
                    warn!("received event-cancel but channel {name:?} in wrong state");
                }
                ReadingState::Monitoring(st2) => {
                    match &mut st2.mon2state {
                        // no special discrimination needed
                        Monitoring2State::Passive(st3) => {
                            st3.tsbeg = tsnow;
                        }
                        Monitoring2State::ReadPending(ioid, since) => {}
                    }
                    let name = self.name_by_cid(cid);
                    warn!("received event-cancel but channel {name:?} in wrong state");
                }
                ReadingState::Polling(..) => {
                    let name = self.name_by_cid(cid);
                    warn!("received event-cancel but channel {name:?} in wrong state");
                }
            },
            _ => {
                // TODO count instead of print
                error!("unexpected state: EventAddRes while having {ch_s:?}");
            }
        }
        Ok(())
    }

    fn handle_read_notify_res(
        &mut self,
        ev: proto::ReadNotifyRes,
        camsg_ts: Instant,
        tsnow: Instant,
    ) -> Result<(), Error> {
        // trace!("handle_read_notify_res  {ev:?}");
        // TODO can not rely on the SID in the response.
        let sid_ev = Sid(ev.sid);
        let ioid = Ioid(ev.ioid);
        if let Some(pp) = self.handler_by_ioid.get_mut(&ioid) {
            if let Some(mut fut) = pp.take() {
                let camsg = CaMsg {
                    ty: CaMsgTy::ReadNotifyRes(ev),
                    ts: camsg_ts,
                };
                fut.as_mut().camsg(camsg, self);
                Ok(())
            } else {
                Err(Error::FutLogic)
            }
        } else {
            if let Some(cid) = self.read_ioids.get(&ioid) {
                let ch_s = if let Some(x) = self.channels.get_mut(cid) {
                    &mut x.state
                } else {
                    warn!("handle_read_notify_res can not find channel for  {cid:?}  {ioid:?}");
                    return Ok(());
                };
                match ch_s {
                    ChannelState::Writable(st) => {
                        if st.channel.sid != sid_ev {
                            // TODO count for metrics
                            // warn!("mismatch in ReadNotifyRes {:?} {:?}", st.channel.sid, sid_ev);
                        }
                        let stnow = self.tmp_ts_poll;
                        let crst = &mut st.channel;
                        let stwin_ts = stnow.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() / 1;
                        if crst.stwin_ts != stwin_ts {
                            crst.stwin_ts = stwin_ts;
                            crst.stwin_count = 0;
                        }
                        {
                            crst.stwin_count += 1;
                            crst.stwin_bytes += ev.payload_len;
                        }
                        match &mut st.reading {
                            ReadingState::Polling(st2) => match &mut st2.tick {
                                PollTickState::Idle(_st3) => {
                                    self.stats.recv_read_notify_while_polling_idle.inc();
                                }
                                PollTickState::Wait(st3, ioid) => {
                                    let dt = tsnow.saturating_duration_since(*st3);
                                    self.stats.caget_lat().ingest((1e3 * dt.as_secs_f32()) as u32);
                                    // TODO maintain histogram of read-notify latencies
                                    self.read_ioids.remove(ioid);
                                    st2.tick = PollTickState::Idle(tsnow);
                                    let iqdqs = &mut self.iqdqs;
                                    let stats = self.stats.as_ref();
                                    Self::read_notify_res_for_write(ev, st, iqdqs, stnow, tsnow, stats)?;
                                }
                            },
                            ReadingState::EnableMonitoring(_) => {
                                self.stats.recv_read_notify_while_enabling_monitoring.inc();
                            }
                            ReadingState::Monitoring(st2) => match &mut st2.mon2state {
                                Monitoring2State::Passive(st3) => {
                                    if self.read_ioids.remove(&ioid).is_some() {
                                        self.stats.recv_read_notify_state_passive_found_ioid.inc();
                                    } else {
                                        self.stats.recv_read_notify_state_passive.inc();
                                    }
                                    st3.tsbeg = tsnow;
                                }
                                Monitoring2State::ReadPending(ioid2, _since) => {
                                    // We don't check again for `since` here. That's done in timeout checking.
                                    // So we could be here a little beyond timeout but we don't care about that.
                                    if ioid != *ioid2 {
                                        // warn!("IOID mismatch ReadNotifyRes on Monitor Read Pending  {ioid:?}  {ioid2:?}");
                                        self.stats.recv_read_notify_state_read_pending_bad_ioid.inc();
                                    } else {
                                        self.stats.recv_read_notify_state_read_pending.inc();
                                    }
                                    self.read_ioids.remove(&ioid);
                                    st2.mon2state = Monitoring2State::Passive(Monitoring2PassiveState { tsbeg: tsnow });
                                    let iqdqs = &mut self.iqdqs;
                                    let stats = self.stats.as_ref();
                                    Self::read_notify_res_for_write(ev, st, iqdqs, stnow, tsnow, stats)?;
                                }
                            },
                            ReadingState::StopMonitoringForPolling(..) => {
                                error!("TODO  handle_read_notify_res  handle StopMonitoringForPolling");
                            }
                        }
                    }
                    _ => {
                        // TODO count instead of print
                        error!("unexpected state: ReadNotifyRes while having {ch_s:?}");
                    }
                }
            } else {
                // warn!("unknown {ioid:?}");
                self.stats.unknown_ioid().inc();
            }
            Ok(())
        }
    }

    fn read_notify_res_for_write(
        ev: proto::ReadNotifyRes,
        st: &mut WritableState,
        iqdqs: &mut InsertDeques,
        stnow: SystemTime,
        tsnow: Instant,
        stats: &CaConnStats,
    ) -> Result<(), Error> {
        let crst = &mut st.channel;
        let writer = &mut st.writer;
        let binwriter = &mut st.binwriter;
        Self::event_add_ingest(
            ev.payload_len,
            ev.value,
            crst,
            writer,
            binwriter,
            iqdqs,
            tsnow,
            stnow,
            stats,
        )?;
        Ok(())
    }

    fn event_add_ingest(
        payload_len: u32,
        value: CaEventValue,
        crst: &mut CreatedState,
        writer: &mut RtWriter,
        binwriter: &mut BinWriter,
        iqdqs: &mut InsertDeques,
        tsnow: Instant,
        stnow: SystemTime,
        stats: &CaConnStats,
    ) -> Result<(), Error> {
        if crst.log_more {
            info!(
                "event_add_ingest  payload_len {}  value {:?}  {}  {}",
                payload_len, value, value.status, value.severity
            );
        } else {
            trace_event_incoming!(
                "event_add_ingest  payload_len {}  value {:?}  {}  {}",
                payload_len,
                value,
                value.status,
                value.severity
            );
        }
        crst.ts_alive_last = tsnow;
        crst.ts_activity_last = tsnow;
        crst.st_activity_last = stnow;
        crst.item_recv_ivl_ema.tick(tsnow);
        crst.recv_count += 1;
        crst.recv_bytes += payload_len as u64;
        crst.acc_recv.push_written(payload_len);
        // TODO should attach these counters already to Writable state.
        let ts_local = {
            let epoch = stnow.duration_since(std::time::UNIX_EPOCH).unwrap_or(Duration::ZERO);
            epoch.as_secs() * SEC + epoch.subsec_nanos() as u64
        };
        let ts = value.ts;
        let ts_diff = ts.abs_diff(ts_local);
        stats.ca_ts_off().ingest((ts_diff / MS) as u32);
        {
            Self::check_ev_value_data(&value.data, &writer.scalar_type())?;
            crst.muted_before = 0;
            crst.insert_item_ivl_ema.tick(tsnow);
            let ts_ioc = TsNano::from_ns(ts);
            let ts_local = TsNano::from_ns(ts_local);
            let val: DataValue = value.data.into();
            // binwriter.ingest(ts_ioc, ts_local, &val, iqdqs)?;
            {
                let ((dwst, dwmt, dwlt),) = writer.write(ts_ioc, ts_local, val, iqdqs)?;
                if dwst {
                    crst.dw_st_last = stnow;
                    crst.acc_st.push_written(payload_len);
                }
                if dwmt {
                    crst.dw_mt_last = stnow;
                    crst.acc_mt.push_written(payload_len);
                }
                if dwlt {
                    crst.dw_lt_last = stnow;
                    crst.acc_lt.push_written(payload_len);
                }
            }
        }
        if false {
            // TODO record stats on drop with the new filter
            stats.channel_fast_item_drop.inc();
            {
                if tsnow.duration_since(crst.insert_recv_ivl_last) >= Duration::from_millis(10000) {
                    crst.insert_recv_ivl_last = tsnow;
                    let ema = crst.insert_item_ivl_ema.ema();
                    let _ = ema;
                }
                if crst.muted_before == 0 {}
                crst.muted_before = 1;
            }
        }
        Ok(())
    }

    fn check_ev_value_data(data: &proto::CaDataValue, scalar_type: &ScalarType) -> Result<(), Error> {
        use crate::ca::proto::CaDataScalarValue;
        use crate::ca::proto::CaDataValue;
        match data {
            CaDataValue::Scalar(x) => match &x {
                CaDataScalarValue::F32(..) => match &scalar_type {
                    ScalarType::F32 => {}
                    _ => {
                        error!("MISMATCH  got f32  exp {:?}", scalar_type);
                    }
                },
                CaDataScalarValue::F64(..) => match &scalar_type {
                    ScalarType::F64 => {}
                    _ => {
                        error!("MISMATCH  got f64  exp {:?}", scalar_type);
                    }
                },
                CaDataScalarValue::I16(..) => match &scalar_type {
                    ScalarType::I16 => {}
                    ScalarType::Enum => {}
                    _ => {
                        error!("MISMATCH  got i16  exp {:?}", scalar_type);
                    }
                },
                CaDataScalarValue::I32(..) => match &scalar_type {
                    ScalarType::I32 => {}
                    _ => {
                        error!("MISMATCH  got i32  exp {:?}", scalar_type);
                    }
                },
                _ => {}
            },
            _ => {}
        }
        Ok(())
    }

    /*
    Acts more like a stream? Can be:
    Pending
    Ready(no-more-work, something-was-done, error)
    */
    fn handle_handshake(&mut self, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let proto = self.proto.as_mut().ok_or_else(|| Error::NoProtocol)?;
        match proto.poll_next_unpin(cx) {
            Ready(Some(k)) => match k {
                Ok(k) => match k {
                    CaItem::Empty => {
                        debug!("CaItem::Empty");
                        Ready(Some(Ok(())))
                    }
                    CaItem::Msg(msg) => match msg.ty {
                        CaMsgTy::VersionRes(n) => {
                            // debug!("see incoming  {:?}  {:?}", self.remote_addr_dbg, msg);
                            if n < 12 || n > 13 {
                                error!("See some unexpected version {n}  channel search may not work.");
                                Ready(Some(Ok(())))
                            } else {
                                if n != 13 {
                                    warn!("Received peer version {n}");
                                }
                                self.state = CaConnState::PeerReady;
                                Ready(Some(Ok(())))
                            }
                        }
                        k => {
                            warn!("Got some other unhandled message: {k:?}");
                            Ready(Some(Ok(())))
                        }
                    },
                },
                Err(e) => {
                    error!("got error item from CaProto {e:?}");
                    Ready(Some(Err(e.into())))
                }
            },
            Ready(None) => {
                warn!("handle_conn_listen CaProto is done  {:?}", self.remote_addr_dbg);
                self.proto = None;
                self.state = CaConnState::EndOfStream;
                Ready(None)
            }
            Pending => Pending,
        }
    }

    fn check_channels_state_init(&mut self, tsnow: Instant, cx: &mut Context) -> Result<(), Error> {
        let mut do_wake_again = false;
        // TODO profile, efficient enough?
        if self.init_state_count == 0 {
            return Ok(());
        }
        let channels = &mut self.channels;
        let proto = self.proto.as_mut().ok_or_else(|| Error::NoProtocol)?;
        let keys: Vec<Cid> = channels.keys().map(|x| *x).collect();
        for cid in keys {
            let conf = channels.get(&cid).ok_or_else(|| Error::UnknownCid(cid))?;
            let st = &conf.state;
            match st {
                ChannelState::Init(cssid) => {
                    let cssid = cssid.clone();
                    let name = conf.conf.name();
                    let msg = CaMsg::from_ty_ts(
                        CaMsgTy::CreateChan(CreateChan {
                            cid: cid.0,
                            channel: name.into(),
                        }),
                        tsnow,
                    );
                    do_wake_again = true;
                    proto.push_out(msg);
                    // TODO handle not-found error, just count and continue?
                    let ch_s = channels.get_mut(&cid).ok_or_else(|| Error::UnknownCid(cid))?;
                    ch_s.state = ChannelState::Creating(CreatingState {
                        tsbeg: tsnow,
                        cssid,
                        cid,
                    });
                    self.init_state_count -= 1;
                }
                _ => {}
            }
        }
        if do_wake_again {
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    fn check_channels_state_poll(&mut self, tsnow: Instant, cx: &mut Context) -> Result<(), Error> {
        let mut do_wake_again = false;
        let mut do_shutdown = None;
        let channels = &mut self.channels;
        for (_k, conf) in channels {
            let chst = &mut conf.state;
            match chst {
                ChannelState::Init(_) => {}
                ChannelState::Creating(_) => {}
                ChannelState::FetchEnumDetails(_) => {}
                ChannelState::MakingSeriesWriter(_) => {}
                ChannelState::Writable(st2) => match &mut st2.reading {
                    ReadingState::EnableMonitoring(_) => {}
                    ReadingState::Monitoring(st3) => match &st3.mon2state {
                        Monitoring2State::Passive(st4) => {
                            if st4.tsbeg + conf.conf.manual_poll_on_quiet_after() < tsnow {
                                debug!("check_channels_state_poll  Monitoring2State::Passive  timeout");
                                // TODO encapsulate and unify with Polling handler
                                let ioid = Ioid(self.ioid);
                                self.ioid = self.ioid.wrapping_add(1);
                                self.read_ioids.insert(ioid, st2.channel.cid.clone());
                                let msg = CaMsg::from_ty_ts(
                                    CaMsgTy::ReadNotify(ReadNotify {
                                        data_type: st2.channel.ca_dbr_type,
                                        data_count: st2.channel.ca_dbr_count,
                                        sid: st2.channel.sid.to_u32(),
                                        ioid: ioid.0,
                                    }),
                                    tsnow,
                                );
                                do_wake_again = true;
                                self.proto.as_mut().ok_or_else(|| Error::NoProtocol)?.push_out(msg);
                                st3.mon2state = Monitoring2State::ReadPending(ioid, tsnow);
                                self.stats.caget_issued().inc();
                            }
                        }
                        Monitoring2State::ReadPending(ioid, since) => {
                            if *since + MONITOR_POLL_TIMEOUT < tsnow {
                                // Something is wrong with this channel.
                                // Maybe we lost connection, maybe the IOC went down, maybe there is a bug where only
                                // this or a subset of the subscribed channels no longer give updates.
                                let name = conf.conf.name();
                                warn!("channel monitor explicit read timeout  {}  ioid {:?}", name, ioid);
                                if false {
                                    // Here we try to close the channel at hand.

                                    // TODO need to define the transition from operating channel to inoperable channel in
                                    // a better and reusable way:
                                    // Do not go directly into error state: need to at least attempt to close the channel and wait/timeout for reply.

                                    let proto = self.proto.as_mut().ok_or(Error::NoProtocol)?;
                                    let item = CaMsg {
                                        ty: CaMsgTy::ChannelClose(ChannelClose {
                                            sid: st2.channel.sid.0,
                                            cid: st2.channel.cid.0,
                                        }),
                                        ts: tsnow,
                                    };
                                    proto.push_out(item);
                                    *chst = ChannelState::Closing(ClosingState {
                                        tsbeg: tsnow,
                                        cssid: st2.channel.cssid,
                                    });
                                } else {
                                    do_shutdown = Some(ShutdownReason::IocTimeout);
                                }
                            }
                        }
                    },
                    ReadingState::StopMonitoringForPolling(_) => {}
                    ReadingState::Polling(st3) => match &mut st3.tick {
                        PollTickState::Idle(x) => {
                            if *x + st3.poll_ivl <= tsnow {
                                let ioid = Ioid(self.ioid);
                                self.ioid = self.ioid.wrapping_add(1);
                                self.read_ioids.insert(ioid, st2.channel.cid.clone());
                                let msg = CaMsg::from_ty_ts(
                                    CaMsgTy::ReadNotify(ReadNotify {
                                        data_type: st2.channel.ca_dbr_type,
                                        data_count: st2.channel.ca_dbr_count,
                                        sid: st2.channel.sid.to_u32(),
                                        ioid: ioid.0,
                                    }),
                                    tsnow,
                                );
                                do_wake_again = true;
                                self.proto.as_mut().unwrap().push_out(msg);
                                st3.tick = PollTickState::Wait(tsnow, ioid);
                                self.stats.caget_issued().inc();
                            }
                        }
                        PollTickState::Wait(x, ioid) => {
                            if *x + Duration::from_millis(10000) <= tsnow {
                                self.read_ioids.remove(ioid);
                                self.stats.caget_timeout().inc();
                                // warn!("channel caget timeout");
                                // std::process::exit(1);
                                st3.tick = PollTickState::Idle(tsnow);
                            }
                        }
                    },
                },
                ChannelState::Error(_) => {}
                ChannelState::Ended(_) => {}
                ChannelState::Closing(st2) => {
                    if st2.tsbeg + TIMEOUT_CHANNEL_CLOSING < tsnow {
                        let name = conf.conf.name();
                        warn!("timeout while closing channel {name}");
                        do_shutdown = Some(ShutdownReason::IocTimeout);
                    }
                }
            }
        }
        if let Some(reason) = do_shutdown {
            self.trigger_shutdown(reason);
        }
        if do_wake_again {
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    fn check_channels_alive(&mut self, tsnow: Instant, _cx: &mut Context) -> Result<(), Error> {
        trace3!("check_channels_alive  {}", self.remote_addr_dbg);
        if let Some(started) = self.ioc_ping_start {
            if started + TIMEOUT_PONG_WAIT < tsnow {
                self.stats.pong_timeout().inc();
                warn!("pong timeout {}", self.remote_addr_dbg);
                self.ioc_ping_start = None;
                let item = CaConnEvent {
                    ts: tsnow,
                    value: CaConnEventValue::EchoTimeout,
                };
                self.ca_conn_event_out_queue.push_back(item);
                self.trigger_shutdown(ShutdownReason::IocTimeout);
            }
        } else {
            if self.ioc_ping_next < tsnow {
                if let Some(proto) = &mut self.proto {
                    self.stats.ping_start().inc();
                    self.ioc_ping_start = Some(tsnow);
                    let msg = CaMsg::from_ty_ts(CaMsgTy::Echo, tsnow);
                    proto.push_out(msg);
                } else {
                    self.stats.ping_no_proto().inc();
                    warn!("can not ping {}  no proto", self.remote_addr_dbg);
                    self.trigger_shutdown(ShutdownReason::ProtocolMissing);
                }
            }
        }
        let mut alive_count = 0;
        let mut not_alive_count = 0;
        for (_, conf) in &mut self.channels {
            let st = &mut conf.state;
            match st {
                ChannelState::Writable(st2) => {
                    match &mut st2.reading {
                        ReadingState::EnableMonitoring(_) => {
                            // TODO handle timeout check
                        }
                        ReadingState::Monitoring(st3) => match &st3.mon2state {
                            Monitoring2State::Passive(_st4) => {}
                            Monitoring2State::ReadPending(_, _) => {
                                // This is handled in check_channels_state_poll
                                // TODO should unify.
                            }
                        },
                        ReadingState::StopMonitoringForPolling(_) => {
                            // TODO handle timeout check
                        }
                        ReadingState::Polling(_st3) => {
                            // This is handled in check_channels_state_poll
                            // TODO should unify.
                        }
                    }
                    if st2.channel.ts_activity_last + conf.conf.expect_activity_within() < tsnow {
                        not_alive_count += 1;
                        self.stats.channel_not_alive_no_activity.inc();
                    } else {
                        alive_count += 1;
                    }
                }
                _ => {}
            }
        }
        self.stats.channel_all_count.__set(self.channels.len() as _);
        self.stats.channel_alive_count.__set(alive_count as _);
        self.stats.channel_not_alive_count.__set(not_alive_count as _);
        Ok(())
    }

    // Can return:
    // Pending, error, work-done (pending state unknown), no-more-work-ever-again.
    fn handle_peer_ready(&mut self, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let mut ts1 = Instant::now();
        // TODO unify with Listen state where protocol gets polled as well.
        let ts2 = Instant::now();
        self.stats
            .time_check_channels_state_init
            .add((ts2.duration_since(ts1) * MS as u32).as_secs());
        ts1 = ts2;
        let _ = ts1;
        let tsnow = Instant::now();
        let proto = if let Some(x) = self.proto.as_mut() {
            x
        } else {
            return Ready(Some(Err(Error::NoProtocol)));
        };
        let res = match proto.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                match k {
                    CaItem::Msg(camsg) => {
                        match camsg.ty {
                            CaMsgTy::SearchRes(k) => {
                                let a = k.addr.to_be_bytes();
                                let addr = format!("{}.{}.{}.{}:{}", a[0], a[1], a[2], a[3], k.tcp_port);
                                trace!("search result indicates server address: {addr}");
                                // TODO count this unexpected case.
                            }
                            CaMsgTy::CreateChanRes(k) => {
                                let stnow = SystemTime::now();
                                self.handle_create_chan_res(k, tsnow, stnow)?;
                                cx.waker().wake_by_ref();
                            }
                            CaMsgTy::EventAddRes(ev) => {
                                trace4!("got EventAddRes  {:?}  cnt {}", camsg.ts, ev.data_count);
                                self.stats.event_add_res_recv.inc();
                                Self::handle_event_add_res(self, ev, tsnow)?
                            }
                            CaMsgTy::EventAddResEmpty(ev) => {
                                trace4!("got EventAddResEmpty  {:?}", camsg.ts);
                                Self::handle_event_add_res_empty(self, ev, tsnow)?
                            }
                            CaMsgTy::ReadNotifyRes(ev) => Self::handle_read_notify_res(self, ev, camsg.ts, tsnow)?,
                            CaMsgTy::Echo => {
                                // let addr = &self.remote_addr_dbg;
                                if let Some(started) = self.ioc_ping_start {
                                    let dt = started.elapsed();
                                    let dt = dt.as_secs() as u32 + dt.subsec_millis();
                                    self.stats.pong_recv_lat().ingest(dt);
                                } else {
                                    let addr = &self.remote_addr_dbg;
                                    warn!("received Echo even though we didn't asked for it  {addr:?}");
                                }
                                self.ioc_ping_last = tsnow;
                                self.ioc_ping_next = tsnow + Self::ioc_ping_ivl_rng(&mut self.rng);
                                self.ioc_ping_start = None;
                            }
                            CaMsgTy::CreateChanFail(msg) => {
                                // TODO
                                // Here, must indicate that the address could be wrong!
                                // The channel status must be "Fail" so that ConnSet can decide to re-search.
                                // TODO how to transition the channel state? Any invariants or simply write to the map?
                                let cid = Cid(msg.cid);
                                if let Some(conf) = self.channels.get(&cid) {
                                    let name = conf.conf.name();
                                    debug!("queue event to notive channel create fail {name}");
                                    let item = CaConnEvent {
                                        ts: tsnow,
                                        value: CaConnEventValue::ChannelCreateFail(name.into()),
                                    };
                                    self.ca_conn_event_out_queue.push_back(item);
                                }
                                self.channel_remove_by_cid(cid);
                                warn!("CaConn sees: {msg:?}");
                            }
                            CaMsgTy::Error(msg) => {
                                warn!("CaConn sees: {msg:?}");
                            }
                            CaMsgTy::AccessRightsRes(msg) => {
                                if false {
                                    warn!("CaConn sees: {msg:?}");
                                }
                            }
                            #[cfg(DISABLED)]
                            CaMsgTy::IssueDataCount(hi, stat, sev, secs, nanos) => {
                                let cid = *self.cid_by_subid.get(&hi.param2()).unwrap();
                                let name = self.name_by_cid.get(&cid).unwrap();
                                debug!("ca large count for  {name}  {hi:?}  {stat}  {sev}  {secs}  {nanos}");
                                self.weird_count += 1;
                                if self.weird_count > 200 {
                                    std::process::exit(13);
                                }
                            }
                            CaMsgTy::VersionRes(x) => {
                                debug!("VersionRes({x})");
                                self.weird_count += 1;
                                if self.weird_count > 200 {
                                    std::process::exit(13);
                                }
                            }
                            CaMsgTy::ChannelCloseRes(x) => {
                                self.handle_channel_close_res(x, tsnow)?;
                            }
                            _ => {
                                warn!("Received unexpected protocol message {:?}", camsg);
                            }
                        }
                    }
                    CaItem::Empty => {}
                }
                Ready(Some(Ok(())))
            }
            Ready(Some(Err(e))) => {
                error!("CaProto yields error: {e:?}  remote {:?}", self.remote_addr_dbg);
                self.trigger_shutdown(ShutdownReason::Protocol);
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                warn!("handle_peer_ready CaProto is done  {:?}", self.remote_addr_dbg);
                self.trigger_shutdown(ShutdownReason::ProtocolMissing);
                Ready(None)
            }
            Pending => Pending,
        };
        res.map_err(Into::into)
    }

    fn handle_create_chan_res(
        &mut self,
        k: proto::CreateChanRes,
        tsnow: Instant,
        stnow: SystemTime,
    ) -> Result<(), Error> {
        let cid = Cid(k.cid);
        let sid = Sid(k.sid);
        let conf = if let Some(x) = self.channels.get_mut(&cid) {
            x
        } else {
            // TODO handle not-found error: just count for metrics?
            warn!("CreateChanRes  {:?} unknown", cid);
            return Ok(());
        };
        let chst = &mut conf.state;
        let cssid = match chst {
            ChannelState::Creating(st) => st.cssid.clone(),
            _ => {
                // TODO handle in better way:
                // Remove channel and emit notice that channel is removed with reason.
                let e = Error::CreateChannelBadState;
                return Err(e);
            }
        };
        self.cid_by_sid.insert(sid, cid);
        if k.data_type > 6 {
            error!("CreateChanRes with unexpected data_type {}", k.data_type);
        }
        // Ask for DBR_TIME_...
        let ca_dbr_type = k.data_type + 14;
        let scalar_type = ScalarType::from_ca_id(k.data_type)?;
        let shape = Shape::from_ca_count(k.data_count)?;

        let log_more = match &scalar_type {
            ScalarType::Enum => {
                if cssid.id() % 20 == 14 {
                    let name = conf.conf.name();
                    info!("ENUM  {}", name);
                    true
                } else {
                    false
                }
            }
            _ => false,
        };

        let (acc_msp, _) = TsMs::from_system_time(stnow).to_grid_02(EMIT_ACCOUNTING_SNAP);
        let created_state = CreatedState {
            cssid,
            cid,
            sid,
            ca_dbr_type,
            ca_dbr_count: k.data_count,
            ts_created: tsnow,
            ts_alive_last: tsnow,
            ts_activity_last: tsnow,
            st_activity_last: stnow,
            ts_msp_last: 0,
            ts_msp_grid_last: 0,
            inserted_in_ts_msp: u64::MAX,
            insert_item_ivl_ema: IntervalEma::new(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: tsnow,
            muted_before: 0,
            info_store_msp_last: info_store_msp_from_time(self.tmp_ts_poll),
            recv_count: 0,
            recv_bytes: 0,
            stwin_ts: 0,
            stwin_count: 0,
            stwin_bytes: 0,
            acc_recv: AccountingInfo::new(acc_msp),
            acc_st: AccountingInfo::new(acc_msp),
            acc_mt: AccountingInfo::new(acc_msp),
            acc_lt: AccountingInfo::new(acc_msp),
            dw_st_last: SystemTime::UNIX_EPOCH,
            dw_mt_last: SystemTime::UNIX_EPOCH,
            dw_lt_last: SystemTime::UNIX_EPOCH,
            scalar_type: scalar_type.clone(),
            shape: shape.clone(),
            log_more,
            name: conf.conf.name().into(),
        };
        match &scalar_type {
            ScalarType::Enum => {
                if created_state.log_more {
                    let min_quiets = conf.conf.min_quiets();
                    let fut = enumfetch::EnumFetch::new(created_state, self, min_quiets);
                    // TODO should always check if the slot is free.
                    let ioid = fut.ioid();
                    let x = Box::pin(fut);
                    self.handler_by_ioid.insert(ioid, Some(x));
                } else {
                }
            }
            _ => {
                *chst = ChannelState::MakingSeriesWriter(MakingSeriesWriterState {
                    tsbeg: tsnow,
                    channel: created_state,
                });
                let job = EstablishWorkerJob::new(
                    JobId(cid.0 as _),
                    self.backend.clone(),
                    conf.conf.name().into(),
                    cssid,
                    scalar_type,
                    shape,
                    conf.conf.min_quiets(),
                    self.writer_tx.clone(),
                    self.tmp_ts_poll,
                );
                self.writer_establish_qu.push_back(job);
            }
        }
        Ok(())
    }

    fn handle_channel_close_res(&mut self, k: proto::ChannelCloseRes, tsnow: Instant) -> Result<(), Error> {
        debug!("{:?}", k);
        Ok(())
    }

    // `?` works not in here.
    fn _test_control_flow(&mut self, _cx: &mut Context) -> ControlFlow<Poll<Result<(), Error>>> {
        use ControlFlow::*;
        use Poll::*;
        let e = Error::CreateChannelBadState;
        // Err(e)?;
        let _ = e;
        Break(Pending)
    }

    fn handle_conn_state(&mut self, tsnow: Instant, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        match &mut self.state {
            CaConnState::Unconnected(_since) => {
                let addr = self.remote_addr_dbg.clone();

                // TODO issue a TCP-connect event (and later a "connected")
                trace!("create tcp connection to {:?}", (addr.ip(), addr.port()));

                let fut = tokio::time::timeout(Duration::from_millis(1000), TcpStream::connect(addr));
                self.state = CaConnState::Connecting(Instant::now(), addr, Box::pin(fut));
                Ok(Ready(Some(())))
            }
            CaConnState::Connecting(_since, addr, fut) => {
                match fut.poll_unpin(cx) {
                    Ready(connect_result) => {
                        match connect_result {
                            Ok(Ok(tcp)) => {
                                self.stats.tcp_connected.inc();
                                let addr = addr.clone();
                                self.iqdqs
                                    .emit_status_item(QueryItem::ConnectionStatus(ConnectionStatusItem {
                                        ts: self.tmp_ts_poll,
                                        addr,
                                        status: ConnectionStatus::Established,
                                    }))?;
                                self.backoff_reset();
                                let proto = CaProto::new(
                                    tcp,
                                    self.remote_addr_dbg.to_string(),
                                    self.opts.array_truncate,
                                    self.ca_proto_stats.clone(),
                                );
                                self.state = CaConnState::Init;
                                self.proto = Some(proto);
                                Ok(Ready(Some(())))
                            }
                            Ok(Err(e)) => {
                                use std::io::ErrorKind;
                                debug!("error connect to {addr} {e}");
                                let addr = addr.clone();
                                self.iqdqs
                                    .emit_status_item(QueryItem::ConnectionStatus(ConnectionStatusItem {
                                        ts: self.tmp_ts_poll,
                                        addr,
                                        status: ConnectionStatus::ConnectError,
                                    }))?;
                                let reason = match e.kind() {
                                    ErrorKind::ConnectionRefused => ShutdownReason::ConnectRefused,
                                    _ => ShutdownReason::IoError,
                                };
                                self.trigger_shutdown(reason);
                                Ok(Ready(Some(())))
                            }
                            Err(e) => {
                                // TODO log with exponential backoff
                                debug!("timeout connect to {addr} {e}");
                                let addr = addr.clone();
                                self.iqdqs
                                    .emit_status_item(QueryItem::ConnectionStatus(ConnectionStatusItem {
                                        ts: self.tmp_ts_poll,
                                        addr,
                                        status: ConnectionStatus::ConnectTimeout,
                                    }))?;
                                self.trigger_shutdown(ShutdownReason::ConnectTimeout);
                                Ok(Ready(Some(())))
                            }
                        }
                    }
                    Pending => Ok(Pending),
                }
            }
            CaConnState::Init => {
                trace4!("Init");
                let hostname = self.local_epics_hostname.clone();
                let proto = self.proto.as_mut().unwrap();
                let msg = CaMsg::from_ty_ts(CaMsgTy::Version, tsnow);
                proto.push_out(msg);
                let msg = CaMsg::from_ty_ts(CaMsgTy::ClientName, tsnow);
                proto.push_out(msg);
                let msg = CaMsg::from_ty_ts(CaMsgTy::HostName(hostname), tsnow);
                proto.push_out(msg);
                self.state = CaConnState::Handshake;
                Ok(Ready(Some(())))
            }
            CaConnState::Handshake => {
                match {
                    let res = self.handle_handshake(cx);
                    res
                } {
                    Ready(Some(Ok(()))) => Ok(Ready(Some(()))),
                    Ready(Some(Err(e))) => Err(e),
                    Ready(None) => Ok(Ready(Some(()))),
                    Pending => Ok(Pending),
                }
            }
            CaConnState::PeerReady => {
                let res = self.handle_peer_ready(cx);
                match res {
                    Ready(Some(Ok(()))) => Ok(Ready(Some(()))),
                    Ready(Some(Err(e))) => Err(e),
                    Ready(None) => Ok(Ready(Some(()))),
                    Pending => Ok(Pending),
                }
            }
            CaConnState::Shutdown(..) => Ok(Ready(None)),
            CaConnState::EndOfStream => Ok(Ready(None)),
        }
    }

    fn loop_inner(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        let tsnow = Instant::now();
        let mut have_progress = false;
        for _ in 0..64 {
            self.stats.loop2_count.inc();
            if self.is_shutdown() {
                break;
            } else if self.iqdqs.len() >= self.opts.insert_queue_max {
                break;
            } else {
                match self.handle_conn_state(tsnow, cx) {
                    Ok(x) => match x {
                        Ready(Some(())) => {
                            have_progress = true;
                            continue;
                        }
                        Ready(None) => {
                            error!("handle_conn_state yields {x:?}");
                            return Err(Error::LoopInnerLogicError);
                        }
                        Pending => return Ok(Pending),
                    },
                    Err(e) => return Err(e),
                }
            };
        }
        if have_progress {
            Ok(Ready(Some(())))
        } else {
            Ok(Ready(None))
        }
    }

    fn poll_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        use Poll::*;
        match self.ticker.poll_unpin(cx) {
            Ready(()) => match self.as_mut().handle_own_ticker(cx) {
                Ok(_) => Ok(Pending),
                Err(e) => {
                    error!("handle_own_ticker {e}");
                    self.trigger_shutdown(ShutdownReason::InternalError);
                    Err(e)
                }
            },
            Pending => Ok(Pending),
        }
    }

    fn handle_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<(), Error> {
        // debug!("tick  CaConn  {}", self.remote_addr_dbg);
        let tsnow = Instant::now();
        if !self.is_shutdown() {
            self.ticker = Self::new_self_ticker();
            let _ = self.ticker.poll_unpin(cx);
            // cx.waker().wake_by_ref();
        }
        self.check_channels_state_init(tsnow, cx)?;
        self.check_channels_state_poll(tsnow, cx)?;
        self.check_channels_alive(tsnow, cx)?;
        // TODO add some random variation
        if self.channel_status_emit_next <= tsnow {
            self.channel_status_emit_next = tsnow + Self::channel_status_emit_ivl(&mut self.rng);
            self.emit_channel_status()?;
            self.emit_accounting()?;
        }
        if self.tick_last_writer + Duration::from_millis(2000) <= tsnow {
            self.tick_last_writer = tsnow;
            self.tick_writers()?;
        }
        match &self.state {
            CaConnState::Unconnected(_) => {}
            CaConnState::Connecting(since, _, _) => {
                if *since + CONNECTING_TIMEOUT < tsnow {
                    debug!("CONNECTING_TIMEOUT");
                }
            }
            CaConnState::Init => {}
            CaConnState::Handshake => {}
            CaConnState::PeerReady => {}
            CaConnState::Shutdown(..) => {}
            CaConnState::EndOfStream => {}
        }
        Ok(())
    }

    fn emit_channel_status(&mut self) -> Result<(), Error> {
        let stnow = SystemTime::now();
        let mut channel_statuses = BTreeMap::new();
        for (_, conf) in self.channels.iter() {
            let chst = &conf.state;
            let chinfo = chst.to_info(chst.cssid(), self.remote_addr_dbg, conf.conf.clone(), stnow);
            channel_statuses.insert(chst.cssid(), chinfo);
        }
        // trace2!("{:?}", channel_statuses);
        // trace!("emit_channel_status  {}", channel_statuses.len());
        let val = ChannelStatusPartial { channel_statuses };
        let item = CaConnEvent {
            ts: Instant::now(),
            value: CaConnEventValue::ChannelStatus(val),
        };
        // TODO limit the queue length.
        // Maybe factor the actual push item into new function.
        // What to do if limit reached?
        // Increase some error counter.
        if self.ca_conn_event_out_queue.len() > self.ca_conn_event_out_queue_max {
            self.stats.out_queue_full().inc();
        } else {
            self.ca_conn_event_out_queue.push_back(item);
        }
        Ok(())
    }

    fn emit_accounting(&mut self) -> Result<(), Error> {
        let stnow = self.tmp_ts_poll;
        let ts = TsMs::from_system_time(stnow);
        let (msp, _lsp) = ts.to_grid_02(EMIT_ACCOUNTING_SNAP);
        for (_k, chconf) in self.channels.iter_mut() {
            let st0 = &mut chconf.state;
            match st0 {
                ChannelState::Writable(st1) => {
                    let ch = &mut st1.channel;
                    for (acc, rt) in [&mut ch.acc_st, &mut ch.acc_mt, &mut ch.acc_lt].into_iter().zip([
                        RetentionTime::Short,
                        RetentionTime::Medium,
                        RetentionTime::Long,
                    ]) {
                        if acc.beg != msp {
                            if acc.usage().count() != 0 {
                                let series = st1.writer.sid();
                                let item = Accounting {
                                    part: (series.id() & 0xff) as i32,
                                    ts: acc.beg,
                                    series,
                                    count: acc.usage().count() as _,
                                    bytes: acc.usage().bytes() as _,
                                };
                                trace!("EMIT ITEM  {rt:?}  {item:?}");
                                self.iqdqs.emit_accounting_item(rt, item)?;
                            }
                            acc.reset(msp);
                        }
                    }
                    {
                        let acc = &mut ch.acc_recv;
                        if acc.beg != msp {
                            if acc.usage().count() != 0 {
                                let series = st1.writer.sid();
                                let item = AccountingRecv {
                                    part: (series.id() & 0xff) as i32,
                                    ts: acc.beg,
                                    series,
                                    count: acc.usage().count() as _,
                                    bytes: acc.usage().bytes() as _,
                                };
                                self.iqdqs.emit_accounting_recv(item)?;
                            }
                            acc.reset(msp);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn tick_writers(&mut self) -> Result<(), Error> {
        for (_, chconf) in &mut self.channels {
            let chst = &mut chconf.state;
            if let ChannelState::Writable(st2) = chst {
                st2.writer.tick(&mut self.iqdqs)?;
            }
        }
        Ok(())
    }

    fn check_ticker_connecting_timeout(&mut self, since: Instant) -> Result<(), Error> {
        Ok(())
    }

    fn queues_out_flushed(&self) -> bool {
        debug!(
            "async out flushed iiq  {}  {}  caout {}",
            self.iqdqs.len() == 0,
            self.iqsp.is_idle(),
            self.ca_conn_event_out_queue.is_empty()
        );
        self.iqdqs.len() == 0 && self.iqsp.is_idle() && self.ca_conn_event_out_queue.is_empty()
    }

    fn attempt_flush_queue<T, Q, FB, FS>(
        qu: &mut VecDeque<T>,
        // sp: &mut Pin<Box<SenderPolling<Q>>>,
        mut sp: Pin<&mut SenderPolling<Q>>,
        qu_to_si: FB,
        loop_max: u32,
        cx: &mut Context,
        id: &str,
        stats: FS,
    ) -> Result<Poll<Option<()>>, Error>
    where
        Q: Unpin,
        FB: Fn(&mut VecDeque<T>) -> Option<Q>,
        FS: Fn(&Q),
    {
        let self_name = "attempt_flush_queue";
        use Poll::*;
        if qu.len() != 0 {
            trace_flush_queue!("{self_name}  id {:10}  len {}", id, qu.len());
        }
        let mut have_progress = false;
        let mut i = 0;
        loop {
            i += 1;
            if i > loop_max {
                break;
            }
            if !sp.has_sender() {
                return Err(Error::NoSender);
            }
            if sp.is_idle() {
                if let Some(item) = qu_to_si(qu) {
                    stats(&item);
                    sp.as_mut().send_pin(item);
                } else {
                    break;
                }
            }
            if sp.is_sending() {
                match sp.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        trace_flush_queue!("{self_name}  id {:10}  send done", id);
                        have_progress = true;
                    }
                    Ready(Err(e)) => {
                        use scywr::senderpolling::Error as SpErr;
                        match e {
                            SpErr::NoSendInProgress => return Err(Error::NotSending),
                            SpErr::Closed(_) => {
                                error!("{self_name}  queue closed  id {:10}", id);
                                return Err(Error::ClosedSending);
                            }
                        }
                    }
                    Pending => {
                        return Ok(Pending);
                    }
                }
            } else {
                return Err(Error::NotSending);
            }
        }
        if have_progress {
            Ok(Ready(Some(())))
        } else {
            Ok(Ready(None))
        }
    }

    fn log_queues_summary(&self) {
        trace!("{}", self.iqdqs.summary());
        trace!("{}", self.iqsp.summary());
    }
}

// $have is tuple (have_progress, have_pending))
macro_rules! flush_queue {
    ($self:expr, $qu:ident, $sp:ident, $batcher:expr, $loop_max:expr, $have:expr, $id:expr, $cx:expr, $stats:expr) => {
        let obj = $self.as_mut().get_mut();
        let qu = &mut obj.$qu;
        let sp = obj.$sp.as_mut();
        match Self::attempt_flush_queue(qu, sp, $batcher, $loop_max, $cx, $id, $stats) {
            Ok(Ready(Some(()))) => {
                *$have.0 |= true;
            }
            Ok(Ready(None)) => {}
            Ok(Pending) => {
                *$have.1 |= true;
            }
            Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
        }
    };
}

macro_rules! flush_queue_dqs {
    ($self:expr, $qu:ident, $sp:ident, $batcher:expr, $loop_max:expr, $have:expr, $id:expr, $cx:expr, $stats:expr) => {
        let obj = $self.as_mut().get_mut();
        let qu = &mut obj.iqdqs.$qu;
        // let sp = std::pin::pin!(obj.iqsp.$sp);
        // let sp = &mut obj.iqsp.$sp;
        // let sp = std::pin::pin!(sp);
        // let sp = todo!();
        let sp = obj.iqsp.as_mut().$sp();
        match Self::attempt_flush_queue(qu, sp, $batcher, $loop_max, $cx, $id, $stats) {
            Ok(Ready(Some(()))) => {
                *$have.0 |= true;
            }
            Ok(Ready(None)) => {}
            Ok(Pending) => {
                *$have.1 |= true;
            }
            Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
        }
    };
}

fn send_individual<T>(qu: &mut VecDeque<T>) -> Option<T> {
    qu.pop_front()
}

fn send_batched<const N: usize, T>(qu: &mut VecDeque<T>) -> Option<VecDeque<T>> {
    let n = qu.len();
    if n == 0 {
        None
    } else {
        let batch = qu.drain(..n.min(N)).collect();
        Some(batch)
    }
}

impl Stream for CaConn {
    type Item = CaConnEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.poll_tsnow = Instant::now();
        self.tmp_ts_poll = SystemTime::now();
        let poll_ts1 = Instant::now();
        self.stats.poll_count().inc();
        self.stats.poll_fn_begin().inc();
        let mut reloops: u32 = 0;
        let ret = loop {
            let lts1 = Instant::now();

            self.stats.poll_loop_begin().inc();
            let qlen = self.iqdqs.len();
            if qlen >= self.opts.insert_queue_max * 2 / 3 {
                self.stats.insert_item_queue_pressure().inc();
            } else if qlen >= self.opts.insert_queue_max {
                self.stats.insert_item_queue_full().inc();
            }

            let mut have_pending = false;
            let mut have_progress = false;

            if let CaConnState::EndOfStream = self.state {
                break Ready(None);
            } else if let Some(item) = self.ca_conn_event_out_queue.pop_front() {
                break Ready(Some(item));
            }

            let lts2 = Instant::now();

            match self.as_mut().poll_own_ticker(cx) {
                Ok(Ready(())) => {
                    have_progress = true;
                }
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            }

            {
                let n = self.iqdqs.len();
                self.stats.iiq_len().ingest(n as u32);
            }

            {
                let stats2 = self.stats.clone();
                let stats_fn = move |item: &VecDeque<QueryItem>| {
                    stats2.iiq_batch_len().ingest(item.len() as u32);
                };
                flush_queue_dqs!(
                    self,
                    st_rf1_rx,
                    st_rf1_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "st_rf1_rx",
                    cx,
                    stats_fn
                );

                let stats2 = self.stats.clone();
                let stats_fn = move |item: &VecDeque<QueryItem>| {
                    stats2.iiq_batch_len().ingest(item.len() as u32);
                };
                flush_queue_dqs!(
                    self,
                    st_rf3_rx,
                    st_rf3_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "st_rf3_rx",
                    cx,
                    stats_fn
                );

                let stats2 = self.stats.clone();
                let stats_fn = move |item: &VecDeque<QueryItem>| {
                    stats2.iiq_batch_len().ingest(item.len() as u32);
                };
                flush_queue_dqs!(
                    self,
                    mt_rf3_rx,
                    mt_rf3_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "mt_rf3_rx",
                    cx,
                    stats_fn
                );

                let stats2 = self.stats.clone();
                let stats_fn = move |item: &VecDeque<QueryItem>| {
                    stats2.iiq_batch_len().ingest(item.len() as u32);
                };
                flush_queue_dqs!(
                    self,
                    lt_rf3_rx,
                    lt_rf3_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "lt_rf3_rx",
                    cx,
                    stats_fn
                );
            }

            let lts3 = Instant::now();

            if !self.is_shutdown() {
                flush_queue!(
                    self,
                    writer_establish_qu,
                    writer_establish_tx,
                    send_individual,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "wrest",
                    cx,
                    |_| {}
                );
            }

            let lts4 = Instant::now();

            match self.as_mut().handle_writer_establish_result(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            }

            let lts5 = Instant::now();

            match self.as_mut().handle_conn_command(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            }

            let lts6 = Instant::now();

            match self.loop_inner(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => {
                    error!("{e}");
                    self.state = CaConnState::EndOfStream;
                    break Ready(Some(CaConnEvent::err_now(e)));
                }
            }

            let lts7 = Instant::now();

            let max = Duration::from_millis(200);
            let dt = lts2.saturating_duration_since(lts1);
            if dt > max {
                debug!("LONG OPERATION  2  {:.0} ms", 1e3 * dt.as_secs_f32());
            }
            let dt = lts3.saturating_duration_since(lts2);
            self.stats.poll_op3_dt().ingest((1e3 * dt.as_secs_f32()) as u32);
            if dt > max {
                debug!("LONG OPERATION  3  {:.0} ms", 1e3 * dt.as_secs_f32());
            }
            let dt = lts4.saturating_duration_since(lts3);
            if dt > max {
                debug!("LONG OPERATION  4  {:.0} ms", 1e3 * dt.as_secs_f32());
            }
            let dt = lts5.saturating_duration_since(lts4);
            if dt > max {
                debug!("LONG OPERATION  5  {:.0} ms", 1e3 * dt.as_secs_f32());
            }
            let dt = lts6.saturating_duration_since(lts5);
            if dt > max {
                debug!("LONG OPERATION  6  {:.0} ms", 1e3 * dt.as_secs_f32());
            }
            let dt = lts7.saturating_duration_since(lts6);
            if dt > max {
                debug!("LONG OPERATION  7  {:.0} ms", 1e3 * dt.as_secs_f32());
            }

            break if self.is_shutdown() {
                if self.queues_out_flushed() {
                    debug!("is_shutdown  queues_out_flushed  set EOS  {}", self.remote_addr_dbg);
                    if let CaConnState::Shutdown(x) = std::mem::replace(&mut self.state, CaConnState::EndOfStream) {
                        Ready(Some(CaConnEvent::new_now(CaConnEventValue::EndOfStream(x))))
                    } else {
                        continue;
                    }
                } else {
                    if have_progress {
                        debug!("is_shutdown  NOT queues_out_flushed  prog  {}", self.remote_addr_dbg);
                        self.stats.poll_reloop().inc();
                        reloops += 1;
                        continue;
                    } else if have_pending {
                        debug!("is_shutdown  NOT queues_out_flushed  pend  {}", self.remote_addr_dbg);
                        self.log_queues_summary();
                        self.stats.poll_pending().inc();
                        Pending
                    } else {
                        // TODO error
                        error!("shutting down, queues not flushed, no progress, no pending");
                        self.stats.logic_error().inc();
                        let e = Error::ShutdownWithQueuesNoProgressNoPending;
                        Ready(Some(CaConnEvent::err_now(e)))
                    }
                }
            } else {
                if have_progress {
                    if poll_ts1.elapsed() > Duration::from_millis(5) {
                        self.stats.poll_wake_break().inc();
                        cx.waker().wake_by_ref();
                        break Ready(Some(CaConnEvent::new(self.poll_tsnow, CaConnEventValue::None)));
                    } else {
                        self.stats.poll_reloop().inc();
                        reloops += 1;
                        continue;
                    }
                } else if have_pending {
                    self.stats.poll_pending().inc();
                    Pending
                } else {
                    self.stats.poll_no_progress_no_pending().inc();
                    let e = Error::NoProgressNoPending;
                    Ready(Some(CaConnEvent::err_now(e)))
                }
            };
        };
        let poll_ts2 = Instant::now();
        let dt = poll_ts2.saturating_duration_since(poll_ts1);
        self.stats.poll_all_dt().ingest((1e3 * dt.as_secs_f32()) as u32);
        self.stats.read_ioids_len().set(self.read_ioids.len() as u64);
        let n = match &self.proto {
            Some(x) => x.proto_out_len() as u64,
            None => 0,
        };
        self.stats.proto_out_len().set(n);
        self.stats.poll_reloops().ingest(reloops);
        ret
    }
}
