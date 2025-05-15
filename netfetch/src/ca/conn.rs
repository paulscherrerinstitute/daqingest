mod enumfetch;

use crate::conf::ChannelConfig;
use crate::metrics::status::StorageUsage;
use crate::throttletrace::ThrottleTrace;
use async_channel::Receiver;
use async_channel::Sender;
use ca_proto::ca::proto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoResult;
use enumfetch::ConnFuture;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use log::*;
use netpod::EMIT_ACCOUNTING_SNAP;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use netpod::channelstatus::ChannelStatus;
use netpod::channelstatus::ChannelStatusClosedReason;
use netpod::ttl::RetentionTime;
use proto::CaDataValue;
use proto::CaEventValue;
use proto::CaItem;
use proto::CaMsg;
use proto::CaMsgTy;
use proto::CaProto;
use proto::ChannelClose;
use proto::CreateChan;
use proto::EventAdd;
use proto::EventCancel;
use proto::ReadNotify;
use scywr::insertqueues::InsertDeques;
use scywr::insertqueues::InsertQueuesTx;
use scywr::insertqueues::InsertSenderPolling;
use scywr::iteminsertqueue as scywriiq;
use scywr::senderpolling::SenderPolling;
use scywriiq::Accounting;
use scywriiq::AccountingRecv;
use scywriiq::ChannelStatusItem;
use scywriiq::ConnectionStatus;
use scywriiq::ConnectionStatusItem;
use scywriiq::MspItem;
use scywriiq::QueryItem;
use scywriiq::ShutdownReason;
use serde::Serialize;
use serde_helper::serde_instant::serde_Instant_elapsed_ms;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use serieswriter::binwriter::BinWriter;
use serieswriter::fixgridwriter::ChannelStatusSeriesWriter;
use serieswriter::fixgridwriter::ChannelStatusWriteState;
use serieswriter::msptool::MspSplit;
use serieswriter::rtwriter::RtWriter;
use serieswriter::writer::EmittableType;
use stats::IntervalEma;
use stats::mett::CaConnMetrics;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use stats::rand_xoshiro::rand_core::RngCore;
use stats::rand_xoshiro::rand_core::SeedableRng;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use time::UtcDateTime;
use tokio::net::TcpStream;

const CONNECTING_TIMEOUT: Duration = Duration::from_millis(1000 * 6);
const CHANNEL_STATUS_EMIT_IVL: Duration = Duration::from_millis(1000 * 8);
const IOC_PING_IVL: Duration = Duration::from_millis(1000 * 180);
const MONITOR_POLL_TIMEOUT: Duration = Duration::from_millis(1000 * 6);
const TIMEOUT_CHANNEL_CLOSING: Duration = Duration::from_millis(1000 * 8);
const TIMEOUT_PONG_WAIT: Duration = Duration::from_millis(1000 * 10);
const READ_CHANNEL_VALUE_STATUS_EMIT_QUIET_MIN: Duration = Duration::from_millis(1000 * 120);
const SILENCE_READ_NEXT_IVL: Duration = Duration::from_millis(1000 * 200);
const POLL_READ_TIMEOUT: Duration = Duration::from_millis(1000 * 10);
const DO_RATE_CHECK: bool = false;
const CHANNEL_STATUS_PONG_QUIET: Duration = Duration::from_millis(1000 * 60 * 60);
const METRICS_EMIT_IVL: Duration = Duration::from_millis(1000 * 1);
const USE_BIN_WRITER: bool = false;

macro_rules! trace3 { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

macro_rules! trace4 { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

macro_rules! trace_flush_queue { ($($arg:expr),*) => ( if false { trace3!($($arg),*); } ); }

macro_rules! trace_event_incoming { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

macro_rules! trace_monitor_stale { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

fn dbg_chn_cid(cid: Cid, conn: &CaConn) -> bool {
    if let Some(name) = conn.name_by_cid(cid) {
        series::dbg::dbg_chn(name)
    } else {
        false
    }
}

type CaRtWriter = RtWriter<CaWriterValue>;

autoerr::create_error_v1!(
    name(Error, "NetfetchConn"),
    enum variants {
        NoProtocol,
        ProtocolError,
        IocIssue,
        Protocol(#[from] proto::Error),
        RtWriter(#[from] serieswriter::rtwriter::Error),
        BinWriter(#[from] serieswriter::binwriter::Error),
        SeriesWriter(#[from] serieswriter::writer::Error),
        UnknownCid(Cid),
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
        MissingTimestamp,
        EnumFetch(#[from] enumfetch::Error),
        SeriesLookup(#[from] dbpg::seriesbychannel::Error),
        Netpod(#[from] netpod::Error),
    },
);

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
    pub ping_last: Option<SystemTime>,
    pub pong_last: Option<SystemTime>,
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
    pub status_emit_count: u64,
    pub last_comparisons: Option<VecDeque<(UtcDateTime, MonitorReadCmp)>>,
}

mod ser_instant {
    use super::*;
    use netpod::DATETIME_FMT_3MS;
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
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Cid(pub u32);

impl fmt::Display for Cid {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Cid({})", self.0)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
struct Subid(pub u32);

impl Subid {
    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
struct Sid(pub u32);

impl Sid {
    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
struct Ioid(pub u32);

#[derive(Clone, Debug, Serialize)]
enum ChannelError {
    CreateChanFail(ChannelStatusSeriesId),
}

#[derive(Debug, Clone, Serialize)]
struct CreatingState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    cssid: ChannelStatusSeriesId,
    cid: Cid,
}

#[derive(Debug, Clone, Serialize)]
struct MakingSeriesWriterState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    channel: CreatedState,
    series_status: SeriesId,
}

#[derive(Debug, Clone, Serialize)]
struct FetchEnumDetails {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug, Clone, Serialize)]
struct EnableMonitoringState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    subid: Subid,
}

#[derive(Debug, Clone, Serialize)]
struct ReadPendingState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
}

#[derive(Debug, Clone, Serialize)]
struct Monitoring2PassiveState {
    // Holds instant when we entered this state. A receive of an event is considered a re-enter of the state,
    // so the instant gets updated. Used for timeout check.
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    #[serde(with = "serde_Instant_elapsed_ms")]
    ts_silence_read_next: Instant,
}

#[derive(Debug, Clone, Serialize)]
struct Monitoring2ReadPendingState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    ioid: Ioid,
}

#[derive(Debug, Clone, Serialize)]
enum Monitoring2State {
    Passive(Monitoring2PassiveState),
    ReadPending(Monitoring2ReadPendingState),
}
#[derive(Debug, Clone, Serialize)]
pub enum MonitorReadCmp {
    Equal,
    DiffTime,
    DiffTimeValue,
    DiffValue,
}

#[derive(Debug, Clone, Serialize)]
struct MonitoringState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    subid: Subid,
    mon2state: Monitoring2State,
    monitoring_event_last: Option<proto::EventAddRes>,
    last_comparisons: VecDeque<(time::UtcDateTime, MonitorReadCmp)>,
}

#[derive(Debug, Clone, Serialize)]
struct StopMonitoringForPollingState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
}

#[derive(Debug, Clone, Serialize)]
struct PollingState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    poll_ivl: Duration,
    tick: PollTickState,
}

#[derive(Debug, Clone, Serialize)]
struct PollTickStateIdle {
    #[serde(with = "serde_Instant_elapsed_ms")]
    next: Instant,
}

impl PollTickStateIdle {
    fn decide_next(next_backup: Instant, ivl: Duration, tsnow: Instant) -> Instant {
        let next = next_backup + ivl;
        if next <= tsnow {
            let mut next = next;
            while next <= tsnow {
                next += ivl;
            }
            next
        } else {
            next
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PollTickStateWait {
    #[serde(with = "serde_Instant_elapsed_ms")]
    next_backup: Instant,
    #[serde(with = "serde_Instant_elapsed_ms")]
    since: Instant,
    ioid: Ioid,
}

#[derive(Debug, Clone, Serialize)]
enum PollTickState {
    Idle(PollTickStateIdle),
    Wait(PollTickStateWait),
}

#[derive(Debug, Serialize)]
struct WritableState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    channel: CreatedState,
    writer: CaRtWriter,
    binwriter: BinWriter,
    reading: ReadingState,
}

#[derive(Debug, Clone, Serialize)]
enum ReadingState {
    EnableMonitoring(EnableMonitoringState),
    Monitoring(MonitoringState),
    StopMonitoringForPolling(StopMonitoringForPollingState),
    Polling(PollingState),
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
struct CreatedState {
    cssid: ChannelStatusSeriesId,
    cid: Cid,
    sid: Sid,
    ca_dbr_type: u16,
    ca_dbr_count: u32,
    #[serde(with = "serde_Instant_elapsed_ms")]
    ts_created: Instant,
    // Updated when we receive something via monitoring or polling
    #[serde(with = "serde_Instant_elapsed_ms")]
    ts_alive_last: Instant,
    // Updated on monitoring, polling or when the channel config changes to reset the timeout
    #[serde(with = "serde_Instant_elapsed_ms")]
    ts_activity_last: Instant,
    st_activity_last: SystemTime,
    // TODO
    #[serde(skip)]
    insert_item_ivl_ema: IntervalEma,
    // TODO
    #[serde(skip)]
    item_recv_ivl_ema: IntervalEma,
    #[serde(with = "serde_Instant_elapsed_ms")]
    insert_recv_ivl_last: Instant,
    muted_before: u32,
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
    name: String,
    enum_str_table: Option<Vec<String>>,
    status_emit_count: u64,
    #[serde(with = "serde_Instant_elapsed_ms")]
    ts_recv_value_status_emit_next: Instant,
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
            insert_item_ivl_ema: IntervalEma::new(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: tsnow,
            muted_before: 0,
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
            name: String::new(),
            enum_str_table: None,
            status_emit_count: 0,
            ts_recv_value_status_emit_next: Instant::now(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Serialize)]
enum ChannelState {
    Init(ChannelStatusSeriesId),
    Creating(CreatingState),
    FetchEnumDetails(FetchEnumDetails),
    FetchCaStatusSeries(MakingSeriesWriterState),
    MakingSeriesWriter(MakingSeriesWriterState),
    Writable(WritableState),
    Closing(ClosingState),
    Error(ChannelError),
    Ended(ChannelStatusSeriesId),
}

#[derive(Debug, Serialize)]
struct ClosingState {
    #[serde(with = "serde_Instant_elapsed_ms")]
    tsbeg: Instant,
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug, Serialize)]
struct ChannelConf {
    conf: ChannelConfig,
    state: ChannelState,
    // TODO
    #[serde(skip)]
    wrst: WriterStatus,
}

impl ChannelConf {
    fn new(conf: ChannelConfig, cssid: ChannelStatusSeriesId) -> Self {
        Self {
            conf,
            state: ChannelState::Init(cssid),
            wrst: WriterStatus {
                writer_status: serieswriter::writer::SeriesWriter::new(SeriesId::new(cssid.id())).unwrap(),
                writer_status_state: serieswriter::fixgridwriter::ChannelStatusWriteState::new(
                    SeriesId::new(cssid.id()),
                    serieswriter::fixgridwriter::CHANNEL_STATUS_GRID,
                ),
            },
        }
    }

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
        conn: &CaConn,
    ) -> ChannelStateInfo {
        let channel_connected_info = match self {
            ChannelState::Init(..) => ChannelConnectedInfo::Disconnected,
            ChannelState::Creating { .. } => ChannelConnectedInfo::Connecting,
            ChannelState::FetchEnumDetails(_) => ChannelConnectedInfo::Connecting,
            ChannelState::FetchCaStatusSeries(_) => ChannelConnectedInfo::Connecting,
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
                if ema.update_count() == 0 { None } else { Some(ema.ema()) }
            }
            _ => None,
        };
        let series = match self {
            ChannelState::Writable(s) => Some(s.writer.series()),
            _ => None,
        };
        let interest_score = 1. / item_recv_ivl_ema.unwrap_or(1e10).max(1e-6).min(1e10);
        let status_emit_count = match self {
            ChannelState::Writable(s) => s.channel.status_emit_count,
            _ => 0,
        };
        let last_comparisons = match self {
            ChannelState::Writable(s) => match &s.reading {
                ReadingState::Monitoring(st2) => Some(st2.last_comparisons.clone()),
                _ => None,
            },
            _ => None,
        };
        ChannelStateInfo {
            stnow,
            cssid,
            addr,
            series,
            channel_connected_info,
            ping_last: conn.ioc_ping_last,
            pong_last: conn.ioc_pong_last,
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
            status_emit_count,
            last_comparisons,
        }
    }

    fn cssid(&self) -> ChannelStatusSeriesId {
        match self {
            ChannelState::Init(cssid) => cssid.clone(),
            ChannelState::Creating(st) => st.cssid.clone(),
            ChannelState::FetchEnumDetails(st) => st.cssid.clone(),
            ChannelState::FetchCaStatusSeries(st) => st.channel.cssid.clone(),
            ChannelState::MakingSeriesWriter(st) => st.channel.cssid.clone(),
            ChannelState::Writable(st) => st.channel.cssid.clone(),
            ChannelState::Error(e) => match e {
                ChannelError::CreateChanFail(cssid) => cssid.clone(),
            },
            ChannelState::Ended(cssid) => cssid.clone(),
            ChannelState::Closing(st) => st.cssid.clone(),
        }
    }

    fn created_state(&self) -> Option<&CreatedState> {
        match self {
            ChannelState::Init(_) => None,
            ChannelState::Creating(_) => None,
            ChannelState::FetchEnumDetails(_) => None,
            ChannelState::FetchCaStatusSeries(st2) => Some(&st2.channel),
            ChannelState::MakingSeriesWriter(st2) => Some(&st2.channel),
            ChannelState::Writable(st2) => Some(&st2.channel),
            ChannelState::Closing(_) => None,
            ChannelState::Error(_) => None,
            ChannelState::Ended(_) => None,
        }
    }

    fn cid(&self) -> Option<Cid> {
        match self {
            ChannelState::Init(_) => None,
            ChannelState::Creating(_) => None,
            ChannelState::FetchEnumDetails(_) => None,
            ChannelState::FetchCaStatusSeries(st2) => Some(st2.channel.cid),
            ChannelState::MakingSeriesWriter(st2) => Some(st2.channel.cid),
            ChannelState::Writable(st2) => Some(st2.channel.cid),
            ChannelState::Closing(_) => None,
            ChannelState::Error(_) => None,
            ChannelState::Ended(_) => None,
        }
    }

    fn sid(&self) -> Option<Sid> {
        match self {
            ChannelState::Init(_) => None,
            ChannelState::Creating(_) => None,
            ChannelState::FetchEnumDetails(_) => None,
            ChannelState::FetchCaStatusSeries(st2) => Some(st2.channel.sid),
            ChannelState::MakingSeriesWriter(st2) => Some(st2.channel.sid),
            ChannelState::Writable(st2) => Some(st2.channel.sid),
            ChannelState::Closing(_) => None,
            ChannelState::Error(_) => None,
            ChannelState::Ended(_) => None,
        }
    }
}

#[derive(Debug)]
struct WriterStatus {
    writer_status: ChannelStatusSeriesWriter,
    writer_status_state: ChannelStatusWriteState,
}

impl WriterStatus {
    fn emit_channel_status_item(
        &mut self,
        item: ChannelStatusItem,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<(), Error> {
        let tsev = TsNano::from_system_time(SystemTime::now());
        let (ts, val) = item.to_ts_val();
        self.writer_status.write(
            serieswriter::fixgridwriter::ChannelStatusWriteValue::new(ts, val),
            &mut self.writer_status_state,
            Instant::now(),
            tsev,
            deque,
        )?;
        Ok(())
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
    MetricsEmitted,
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
            Self::MetricsEmitted => fmt.debug_tuple("MetricsEmitted").finish(),
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
pub struct CmdChannelInspectFull {
    name: String,
    tx: Sender<serde_json::Value>,
}

#[derive(Debug)]
pub enum ConnCommandKind {
    ChannelAdd(ChannelConfig, ChannelStatusSeriesId),
    ChannelClose(String),
    ChannelCloseReconf(String),
    Shutdown,
    ChannelInspectFull(CmdChannelInspectFull),
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

    pub fn channel_close_reconf(name: String) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelCloseReconf(name),
        }
    }

    pub fn channel_inspect(name: String, tx: Sender<serde_json::Value>) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelInspectFull(CmdChannelInspectFull { name, tx }),
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
        CaConnEventDescShort { inner: self }
    }
}

pub struct CaConnEventDescShort<'a> {
    inner: &'a CaConnEvent,
}

impl<'a> fmt::Display for CaConnEventDescShort<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "CaConnEventDescShort {{ ts: {:?}, value: {} }}",
            self.inner.ts,
            self.inner.value.desc_short()
        )
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
    ChannelRemoved(String),
    Metrics(stats::mett::CaConnMetrics),
}

impl CaConnEventValue {
    pub fn desc_short(&self) -> &'static str {
        match self {
            CaConnEventValue::None => "None",
            CaConnEventValue::EchoTimeout => "EchoTimeout",
            CaConnEventValue::ConnCommandResult(_) => "ConnCommandResult",
            CaConnEventValue::ChannelStatus(_) => "ChannelStatus",
            CaConnEventValue::ChannelCreateFail(_) => "ChannelCreateFail",
            CaConnEventValue::EndOfStream(_) => "EndOfStream",
            CaConnEventValue::ChannelRemoved(_) => "ChannelRemoved",
            CaConnEventValue::Metrics(_) => "Metrics",
        }
    }
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
    // TODO make private when we don't share it anymore
    pub(super) insert_queue_max: usize,
    pub(super) array_truncate: usize,
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
    version_seen: bool,
    non_version_seen: bool,
    non_version_seen_emitted: bool,
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
    conn_command_tx: Pin<Box<Sender<ConnCommand>>>,
    conn_command_rx: Pin<Box<Receiver<ConnCommand>>>,
    conn_backoff: f32,
    conn_backoff_beg: f32,
    ioc_ping_next: Instant,
    ioc_ping_start: Option<Instant>,
    ioc_ping_last: Option<SystemTime>,
    ioc_pong_last: Option<SystemTime>,
    iqsp: Pin<Box<InsertSenderPolling>>,
    ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    ca_conn_event_out_queue_max: usize,
    thr_msg_poll: ThrottleTrace,
    rng: Xoshiro128PlusPlus,
    channel_info_query_qu: VecDeque<ChannelInfoQuery>,
    channel_info_query_tx: Pin<Box<SenderPolling<ChannelInfoQuery>>>,
    channel_info_query_res_rxs: VecDeque<(
        Pin<Box<Receiver<Result<ChannelInfoResult, dbpg::seriesbychannel::Error>>>>,
        Cid,
    )>,
    tmp_ts_poll: SystemTime,
    poll_tsnow: Instant,
    ioid: u32,
    read_ioids: HashMap<Ioid, Cid>,
    handler_by_ioid: HashMap<Ioid, Option<Pin<Box<dyn ConnFuture>>>>,
    trace_channel_poll: bool,
    ts_channel_status_pong_last: Instant,
    mett: stats::mett::CaConnMetrics,
    metrics_emit_last: Instant,
    fionread_last: u32,
    logs_by_channel_name: HashMap<String, VecDeque<(time::UtcDateTime, String)>>,
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
    ) -> Self {
        let tsnow = Instant::now();
        let (cq_tx, cq_rx) = async_channel::bounded(32);
        let mut rng = stats::xoshiro_from_time();
        Self {
            opts,
            backend,
            state: CaConnState::Unconnected(tsnow),
            ticker: Self::new_self_ticker(&mut rng),
            proto: None,
            version_seen: false,
            non_version_seen: false,
            non_version_seen_emitted: false,
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
            conn_command_tx: Box::pin(cq_tx),
            conn_command_rx: Box::pin(cq_rx),
            conn_backoff: 0.02,
            conn_backoff_beg: 0.02,
            ioc_ping_next: tsnow + Self::ioc_ping_ivl_rng(&mut rng),
            ioc_ping_start: None,
            ioc_ping_last: None,
            ioc_pong_last: None,
            iqsp: Box::pin(InsertSenderPolling::new(iqtx)),
            ca_conn_event_out_queue: VecDeque::new(),
            ca_conn_event_out_queue_max: 2000,
            thr_msg_poll: ThrottleTrace::new(Duration::from_millis(2000)),
            rng,
            channel_info_query_qu: VecDeque::new(),
            channel_info_query_tx: Box::pin(SenderPolling::new(channel_info_query_tx)),
            channel_info_query_res_rxs: VecDeque::new(),
            tmp_ts_poll: SystemTime::now(),
            poll_tsnow: tsnow,
            ioid: 100,
            read_ioids: HashMap::new(),
            handler_by_ioid: HashMap::new(),
            trace_channel_poll: false,
            ts_channel_status_pong_last: tsnow,
            mett: stats::mett::CaConnMetrics::new(),
            metrics_emit_last: tsnow,
            fionread_last: 0,
            logs_by_channel_name: HashMap::new(),
        }
    }

    fn ioc_ping_ivl_rng(rng: &mut Xoshiro128PlusPlus) -> Duration {
        let b = IOC_PING_IVL;
        b + b * (rng.next_u32() & 0x3f) / 0xff
    }

    fn channel_status_emit_ivl(rng: &mut Xoshiro128PlusPlus) -> Duration {
        let b = CHANNEL_STATUS_EMIT_IVL;
        b + b * (rng.next_u32() & 0x3f) / 0xff
    }

    fn silence_read_next_ivl_rng(rng: &mut Xoshiro128PlusPlus) -> Duration {
        let b = SILENCE_READ_NEXT_IVL;
        b + b * (rng.next_u32() & 0x7f) / 0xff
    }

    fn recv_value_status_emit_ivl_rng(rng: &mut Xoshiro128PlusPlus) -> Duration {
        let b = READ_CHANNEL_VALUE_STATUS_EMIT_QUIET_MIN;
        b + b * (rng.next_u32() & 0x3f) / 0xff
    }

    fn new_self_ticker(rng: &mut Xoshiro128PlusPlus) -> Pin<Box<tokio::time::Sleep>> {
        let b = Duration::from_millis(110);
        let dur = b + b * (rng.next_u32() & 0x3f) / 0xff;
        Box::pin(tokio::time::sleep(dur))
    }

    fn proto(&mut self) -> Option<&mut CaProto> {
        self.proto.as_mut()
    }

    fn ioid_next(&mut self) -> Ioid {
        self.ioid = self.ioid.wrapping_add(1);
        Ioid(self.ioid)
    }

    fn channel_status_qu(iqdqs: &mut InsertDeques) -> &mut VecDeque<QueryItem> {
        &mut iqdqs.lt_rf3_qu
    }

    fn channel_status_pong_qu(iqdqs: &mut InsertDeques) -> &mut VecDeque<QueryItem> {
        &mut iqdqs.st_rf3_qu
    }

    pub fn conn_command_tx(&self) -> Sender<ConnCommand> {
        self.conn_command_tx.as_ref().get_ref().clone()
    }

    fn log_channel(&mut self, name: &str, msg: String) {
        if let Some(v) = self.logs_by_channel_name.get_mut(name) {
            let n = v.len();
            if n >= 24 {
                for _ in 16..n {
                    v.pop_front();
                }
            }
            v.push_back((time::UtcDateTime::now(), msg));
        } else {
            let mut v = VecDeque::new();
            v.push_back((time::UtcDateTime::now(), msg));
            self.logs_by_channel_name.insert(name.into(), v);
        }
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
        let item = ConnectionStatusItem {
            ts: self.tmp_ts_poll,
            addr,
            // TODO map to appropriate status
            status: ConnectionStatus::Closing,
        };
        if self.emit_connection_status_item(item).is_err() {
            self.mett.logic_error().inc();
        }
        self.proto = None;
    }

    fn emit_connection_status_item(&mut self, _item: ConnectionStatusItem) -> Result<(), Error> {
        // todo!()
        // TODO emit
        Ok(())
    }

    fn cmd_channel_close(&mut self, name: String) {
        self.channel_close(name);
        // TODO return the result
    }

    fn cmd_shutdown(&mut self) {
        debug!("cmd_shutdown {}", self.remote_addr_dbg);
        self.trigger_shutdown(ShutdownReason::ShutdownCommand);
    }

    fn handle_conn_command(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        self.mett.loop3_count().inc();
        if self.is_shutdown() {
            Ok(Ready(None))
        } else {
            let rx = self.conn_command_rx.as_mut();
            match rx.poll_next(cx) {
                Ready(Some(a)) => {
                    trace3!("handle_conn_command received a command  {}", self.remote_addr_dbg);
                    match a.kind {
                        ConnCommandKind::ChannelAdd(conf, cssid) => {
                            self.log_channel(conf.name(), format!("add  file{}", conf.config_file_basename()));
                            self.channel_add(conf, cssid)?;
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::ChannelClose(name) => {
                            self.log_channel(&name, format!("close"));
                            self.cmd_channel_close(name);
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::ChannelCloseReconf(name) => {
                            self.log_channel(&name, format!("close for reconf"));
                            self.cmd_channel_close(name);
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::Shutdown => {
                            self.cmd_shutdown();
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::ChannelInspectFull(cmd) => {
                            let chstate = match self.cid_by_name(&cmd.name) {
                                Some(cid) => match self.channels.get(&cid) {
                                    Some(ch) => match serde_json::to_value(&ch) {
                                        Ok(val) => val,
                                        Err(_) => serde_json::Value::String(format!("serde error")),
                                    },
                                    None => serde_json::Value::String(format!("unknown cid {:?}", cid)),
                                },
                                None => serde_json::Value::String(format!("unknown name {:?}", cmd.name)),
                            };
                            let chlog = match self.logs_by_channel_name.get(&cmd.name) {
                                Some(v) => match serde_json::to_value(&v) {
                                    Ok(val) => val,
                                    Err(_) => serde_json::Value::String(format!("serde error")),
                                },
                                None => serde_json::Value::String(format!("unknown name {:?}", cmd.name)),
                            };
                            let mut h = serde_json::Map::new();
                            h.insert("chstate".into(), chstate);
                            h.insert("chlog".into(), chlog);
                            match cmd.tx.send_blocking(serde_json::Value::Object(h)) {
                                Ok(()) => Ok(Ready(Some(()))),
                                Err(_) => {
                                    // TODO count in metrics
                                    Ok(Ready(Some(())))
                                }
                            }
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
        let mut have_progress = false;
        let mut have_pending = false;
        if self.is_shutdown() {
            Ok(Ready(None))
        } else {
            let n = self.channel_info_query_res_rxs.len().min(16);
            let mut i = 0;
            while let Some(x) = self.channel_info_query_res_rxs.pop_front() {
                let mut rx = x.0;
                let cid = x.1;
                match rx.poll_next_unpin(cx) {
                    Ready(Some(res)) => {
                        trace!("handle_writer_establish_result  recv  {}", self.remote_addr_dbg);
                        let chinfo = res?;
                        if let Some(ch) = self.channels.get(&cid) {
                            match &ch.state {
                                ChannelState::FetchCaStatusSeries(st) => {
                                    let crst = &st.channel;
                                    let cid = crst.cid.clone();
                                    let (tx, rx) = async_channel::bounded(8);
                                    let item = ChannelInfoQuery {
                                        backend: self.backend.clone(),
                                        channel: crst.name().into(),
                                        kind: netpod::SeriesKind::ChannelData,
                                        scalar_type: crst.scalar_type.clone(),
                                        shape: crst.shape.clone(),
                                        tx: Box::pin(tx),
                                    };
                                    self.channel_info_query_qu.push_back(item);
                                    self.channel_info_query_res_rxs.push_back((Box::pin(rx), cid));
                                    self.channels.get_mut(&cid).unwrap().state =
                                        ChannelState::MakingSeriesWriter(MakingSeriesWriterState {
                                            tsbeg: Instant::now(),
                                            channel: st.channel.clone(),
                                            series_status: chinfo.series.to_series(),
                                        });
                                    have_progress = true;
                                }
                                ChannelState::MakingSeriesWriter(st) => {
                                    let scalar_type = st.channel.scalar_type.clone();
                                    let shape = st.channel.shape.clone();
                                    if series::dbg::dbg_chn(st.channel.name()) {
                                        info!("call RtWriter::new  {:?}  {:?}", chinfo, ch.conf);
                                    }
                                    let writer = RtWriter::new(
                                        chinfo.series.to_series(),
                                        scalar_type,
                                        shape,
                                        ch.conf.min_quiets(),
                                        ch.conf.is_polled(),
                                        ch.conf.replication(),
                                        &|| CaWriterValueState::new(st.series_status, chinfo.series.to_series()),
                                    )?;
                                    self.handle_writer_establish_inner(cid, writer)?;
                                    have_progress = true;
                                }
                                _ => {
                                    return Err(Error::Error);
                                }
                            }
                        } else {
                            return Err(Error::Error);
                        }
                    }
                    Ready(None) => {
                        error!("channel lookup queue closed");
                    }
                    Pending => {
                        self.channel_info_query_res_rxs.push_back((rx, cid));
                        have_pending = true;
                    }
                }
                i += 1;
                if i >= n {
                    break;
                }
            }
            if have_progress {
                Ok(Ready(Some(())))
            } else if have_pending {
                Ok(Pending)
            } else {
                Ok(Ready(None))
            }
        }
    }

    fn handle_writer_establish_inner(&mut self, cid: Cid, writer: CaRtWriter) -> Result<(), Error> {
        trace!("handle_writer_establish_inner  {cid:?}");
        let dbg_chn_cid = dbg_chn_cid(cid, self);
        if dbg_chn_cid {
            info!("handle_writer_establish_inner  {:?}", cid);
        }
        let stnow = self.tmp_ts_poll.clone();
        if let Some(conf) = self.channels.get_mut(&cid) {
            // TODO refactor, should only execute this when required:
            let conf_poll_conf = conf.poll_conf();
            if let ChannelState::MakingSeriesWriter(st2) = &mut conf.state {
                let beg = TsNano::from_system_time(stnow);
                let binwriter = BinWriter::new(
                    beg,
                    conf.conf.min_quiets(),
                    conf.conf.is_polled(),
                    st2.channel.cssid,
                    writer.series(),
                    st2.channel.scalar_type.clone(),
                    st2.channel.shape.clone(),
                    conf.conf.name().into(),
                )?;
                self.mett.get_series_id_ok().inc();
                {
                    let item = ChannelStatusItem {
                        ts: self.tmp_ts_poll,
                        cssid: st2.channel.cssid.clone(),
                        status: ChannelStatus::Opened,
                    };
                    conf.wrst
                        .emit_channel_status_item(item, Self::channel_status_qu(&mut self.iqdqs))?;
                }
                if let Some((ivl,)) = conf_poll_conf {
                    let ivl = Duration::from_millis(ivl);
                    if self.trace_channel_poll {
                        trace!("make poll idle state from writer establish");
                    }
                    let next = self.poll_tsnow + ivl * (self.rng.next_u32() & 0x1ff) / 511;
                    let created_state = WritableState {
                        tsbeg: self.poll_tsnow,
                        channel: std::mem::replace(&mut st2.channel, CreatedState::dummy()),
                        writer,
                        binwriter,
                        reading: ReadingState::Polling(PollingState {
                            tsbeg: self.poll_tsnow,
                            poll_ivl: ivl,
                            tick: PollTickState::Idle(PollTickStateIdle { next }),
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
                            subid, cid, st2.channel.cid, st2.channel.sid
                        );
                        subid
                    };
                    {
                        if dbg_chn_cid {
                            info!("send out EventAdd for {cid:?}");
                        }
                        let data_count = st2.channel.ca_dbr_count;
                        let _data_count = 0;
                        let ty = CaMsgTy::EventAdd(EventAdd::new(
                            st2.channel.ca_dbr_type,
                            data_count,
                            st2.channel.sid.to_u32(),
                            subid.to_u32(),
                        ));
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

    pub fn channel_add(&mut self, conf: ChannelConfig, cssid: ChannelStatusSeriesId) -> Result<(), Error> {
        debug!("channel_add  {conf:?}  {cssid:?}");
        if false {
            if series::dbg::dbg_chn(&conf.name()) {
                self.trace_channel_poll = true;
            }
        }
        if self.cid_by_name(conf.name()).is_some() {
            self.mett.channel_add_exists().inc();
            if series::dbg::dbg_chn(&conf.name()) {
                error!("logic error channel already exists {conf:?}");
            }
            Ok(())
        } else {
            let cid = self.cid_by_name_or_insert(conf.name())?;
            if self.channels.contains_key(&cid) {
                self.mett.channel_add_exists().inc();
                if series::dbg::dbg_chn(&conf.name()) {
                    error!("logic error channel already exists {conf:?}");
                }
                Ok(())
            } else {
                let conf = ChannelConf::new(conf, cssid);
                self.channels.insert(cid, conf);
                // TODO do not count, use separate queue for those channels.
                self.init_state_count += 1;
                Ok(())
            }
        }
    }

    pub fn channel_close(&mut self, name: String) {
        debug!("channel_close  {}", name);
        let tsnow = Instant::now();
        let stnow = SystemTime::now();
        let cid = if let Some(x) = self.cid_by_name.get(&name) {
            x.clone()
        } else {
            debug!("channel_close  {}   can not find channel", name);
            return;
        };
        self.cid_by_name.remove(&name);
        if let Some(conf) = self.channels.get_mut(&cid) {
            let item = ChannelStatusItem {
                ts: stnow,
                cssid: conf.state.cssid(),
                status: ChannelStatus::Closed(ChannelStatusClosedReason::ChannelRemove),
            };
            let qu = Self::channel_status_qu(&mut self.iqdqs);
            if conf.wrst.emit_channel_status_item(item, qu).is_err() {
                self.mett.logic_error().inc();
            }
            // TODO shutdown the internal writer structures.
            if let Some(cst) = conf.state.created_state() {
                if let Some(proto) = self.proto.as_mut() {
                    let ty = CaMsgTy::ChannelClose(ChannelClose {
                        sid: cst.sid.to_u32(),
                        cid: cid.0,
                    });
                    let item = CaMsg::from_ty_ts(ty, tsnow);
                    proto.push_out(item);
                }
            }
            {
                let mut it = self.cid_by_subid.extract_if(|_, v| *v == cid);
                if let Some((subid, _cid)) = it.next() {
                    it.count();
                    if let Some(cst) = conf.state.created_state() {
                        if let Some(proto) = self.proto.as_mut() {
                            let ty = CaMsgTy::EventCancel(EventCancel {
                                data_type: cst.ca_dbr_type,
                                data_count: cst.ca_dbr_count,
                                sid: cst.sid.to_u32(),
                                subid: subid.to_u32(),
                            });
                            let item = CaMsg::from_ty_ts(ty, tsnow);
                            proto.push_out(item);
                        }
                    }
                };
            }
        } else {
            debug!("channel_close  {}   no channel block", name);
        };
        {
            let it = self.cid_by_sid.extract_if(|_, v| *v == cid);
            it.count();
        }
        self.channels.remove(&cid);
        // TODO emit CaConn item to let CaConnSet know that we have closed the channel.
        // TODO may be too full
        let value = CaConnEventValue::ChannelRemoved(name);
        let item = CaConnEvent::new_now(value);
        self.ca_conn_event_out_queue.push_back(item);
    }

    fn channel_remove_by_name(&mut self, name: String) {
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
        let stnow = self.tmp_ts_poll;
        let status_qu = Self::channel_status_qu(&mut self.iqdqs);
        for (_cid, conf) in &mut self.channels {
            let item = ChannelStatusItem {
                ts: stnow,
                cssid: conf.state.cssid(),
                status: ChannelStatus::Closed(channel_reason.clone()),
            };
            if conf.wrst.emit_channel_status_item(item, status_qu).is_err() {
                self.mett.logic_error().inc();
            }
        }
        for (_cid, conf) in &mut self.channels {
            if series::dbg::dbg_chn(conf.conf.name()) {
                let js = serde_json::to_string(conf).unwrap();
                debug!("channel_state_on_shutdown  debug  {:?}", conf);
                debug!("channel_state_on_shutdown  json  {}", js);
            }
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
                ChannelState::FetchCaStatusSeries(st) => {
                    *chst = ChannelState::Ended(st.channel.cssid.clone());
                }
                ChannelState::MakingSeriesWriter(st) => {
                    *chst = ChannelState::Ended(st.channel.cssid.clone());
                }
                ChannelState::Writable(st2) => {
                    // TODO should call the proper channel-close handler which in turn emits the status item.
                    // Make sure I record the reason for the "Close": user command, IOC error, etc..
                    let cssid = st2.channel.cssid.clone();
                    *chst = ChannelState::Ended(cssid);
                }
                ChannelState::Error(..) => {
                    // Leave state unchanged.
                }
                ChannelState::Ended(_) => {}
                ChannelState::Closing(_) => {}
            }
        }
    }

    fn transition_to_polling(&mut self, subid: Subid, tsnow: Instant) -> Result<(), Error> {
        let cid = if let Some(x) = self.cid_by_subid.get(&subid) {
            *x
        } else {
            self.mett.unknown_subid().inc();
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

    fn handle_event_add_res(
        &mut self,
        ev: proto::EventAddRes,
        tsnow: Instant,
        tscaproto: Instant,
    ) -> Result<(), Error> {
        let subid = Subid(ev.subid);
        // TODO handle subid-not-found which can also be peer error:
        let cid = if let Some(x) = self.cid_by_subid.get(&subid) {
            *x
        } else {
            if self.thr_msg_poll.is_action() {
                self.mett.no_cid_for_subid().inc();
                // debug!("can not find cid for subid {subid:?}");
            }
            // return Err(Error::with_msg_no_trace());
            return Ok(());
        };
        let dbg_chn = dbg_chn_cid(cid, self);
        let (ch_s, ch_wrst, ch_conf) = if let Some(x) = self.channels.get_mut(&cid) {
            (&mut x.state, &mut x.wrst, &x.conf)
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
        if false && dbg_chn {
            trace!("handle_event_add_res  {:?}  {:?}", cid, ev);
        }
        match ch_s {
            ChannelState::Writable(st) => {
                if false && dbg_chn {
                    trace!("handle_event_add_res  Writable  {:?}  {:?}", cid, ev);
                }
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
                                    Monitoring2State::ReadPending(_) => {}
                                }
                                Some(x.subid.clone())
                            }
                            ReadingState::StopMonitoringForPolling(_) => {
                                self.mett.transition_to_polling_bad_state().inc();
                                None
                            }
                            ReadingState::Polling(_) => {
                                self.mett.transition_to_polling_already_in().inc();
                                None
                            }
                        };
                        if let Some(subid) = subid {
                            self.mett.transition_to_polling().inc();
                            self.transition_to_polling(subid, tsnow)?;
                        } else {
                            self.mett.transition_to_polling_bad_state().inc();
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
                            mon2state: Monitoring2State::Passive(Monitoring2PassiveState {
                                tsbeg: tsnow,
                                ts_silence_read_next: tsnow + Self::silence_read_next_ivl_rng(&mut self.rng),
                            }),
                            monitoring_event_last: Some(ev.clone()),
                            last_comparisons: VecDeque::new(),
                        });
                        let crst = &mut st.channel;
                        let writer = &mut st.writer;
                        let binwriter = &mut st.binwriter;
                        let iqdqs = &mut self.iqdqs;
                        let mett = &mut self.mett;
                        Self::event_add_ingest(
                            ev.payload_len,
                            ev.value,
                            ch_wrst,
                            crst,
                            writer,
                            binwriter,
                            iqdqs,
                            tsnow,
                            stnow,
                            tscaproto,
                            ch_conf.use_ioc_time(),
                            mett,
                            &mut self.rng,
                        )?;
                    }
                    ReadingState::Monitoring(st2) => {
                        match &mut st2.mon2state {
                            Monitoring2State::Passive(st3) => {
                                st3.tsbeg = tsnow;
                            }
                            Monitoring2State::ReadPending(_) => {
                                // Received EventAdd while still waiting for answer to explicit ReadNotify.
                                // This is fine.
                                self.mett.recv_event_add_while_wait_on_read_notify().inc();
                            }
                        }
                        let crst = &mut st.channel;
                        let writer = &mut st.writer;
                        let binwriter = &mut st.binwriter;
                        let iqdqs = &mut self.iqdqs;
                        let mett = &mut self.mett;
                        st2.monitoring_event_last = Some(ev.clone());
                        Self::event_add_ingest(
                            ev.payload_len,
                            ev.value,
                            ch_wrst,
                            crst,
                            writer,
                            binwriter,
                            iqdqs,
                            tsnow,
                            stnow,
                            tscaproto,
                            ch_conf.use_ioc_time(),
                            mett,
                            &mut self.rng,
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
            | ChannelState::FetchCaStatusSeries(_)
            | ChannelState::MakingSeriesWriter(_) => {
                self.mett.recv_read_notify_but_not_init_yet().inc();
            }
            ChannelState::Closing(_) | ChannelState::Ended(_) | ChannelState::Error(_) => {
                self.mett.recv_read_notify_but_no_longer_ready().inc();
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
            if self.thr_msg_poll.is_action() {
                self.mett.no_cid_for_subid().inc();
                // debug!("can not find cid for subid {subid:?}");
            }
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
                    if self.trace_channel_poll {
                        trace!("make poll idle from event add empty");
                    }
                    let ivl = Duration::from_millis(1000);
                    st.reading = ReadingState::Polling(PollingState {
                        tsbeg: tsnow,
                        poll_ivl: ivl,
                        tick: PollTickState::Idle(PollTickStateIdle { next: tsnow }),
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
                        Monitoring2State::ReadPending(_) => {}
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
        tsnow: Instant,
        tscaproto: Instant,
    ) -> Result<(), Error> {
        // trace!("handle_read_notify_res  {ev:?}");
        // TODO can not rely on the SID in the response.
        let sid_ev = Sid(ev.sid);
        let ioid = Ioid(ev.ioid);
        if let Some(pp) = self.handler_by_ioid.get_mut(&ioid) {
            if let Some(mut fut) = pp.take() {
                let camsg = CaMsg::from_ty_ts(CaMsgTy::ReadNotifyRes(ev), tscaproto);
                fut.as_mut().camsg(camsg, self)?;
                Ok(())
            } else {
                Err(Error::FutLogic)
            }
        } else {
            if let Some(cid) = self.read_ioids.get(&ioid) {
                let (ch_s, ch_wrst, ch_conf) = if let Some(x) = self.channels.get_mut(cid) {
                    (&mut x.state, &mut x.wrst, &x.conf)
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
                                PollTickState::Idle(_) => {
                                    self.mett.recv_read_notify_while_polling_idle().inc();
                                }
                                PollTickState::Wait(st3) => {
                                    if self.read_ioids.remove(&st3.ioid).is_some() {
                                        self.mett.ioid_read_done().inc();
                                    } else {
                                        self.mett.ioid_read_error_not_found().inc();
                                    }
                                    let dt = tsnow.saturating_duration_since(st3.since);
                                    self.mett.caget_lat().push_dur_100us(dt);
                                    let next = PollTickStateIdle::decide_next(st3.next_backup, st2.poll_ivl, tsnow);
                                    if self.trace_channel_poll {
                                        trace!("make next poll idle at {:?}   tsnow {:?}", next, tsnow);
                                    }
                                    st2.tick = PollTickState::Idle(PollTickStateIdle { next });
                                    let iqdqs = &mut self.iqdqs;
                                    let mett = &mut self.mett;
                                    Self::read_notify_res_for_write(
                                        ev,
                                        ch_wrst,
                                        st,
                                        iqdqs,
                                        stnow,
                                        tsnow,
                                        tscaproto,
                                        ch_conf.use_ioc_time(),
                                        mett,
                                        &mut self.rng,
                                    )?;
                                }
                            },
                            ReadingState::EnableMonitoring(_) => {
                                self.mett.recv_read_notify_while_enabling_monitoring().inc();
                            }
                            ReadingState::Monitoring(st2) => match &mut st2.mon2state {
                                Monitoring2State::Passive(st3) => {
                                    if self.read_ioids.remove(&ioid).is_some() {
                                        self.mett.ioid_read_done().inc();
                                        self.mett.recv_read_notify_state_passive_found_ioid().inc();
                                    } else {
                                        self.mett.ioid_read_error_not_found().inc();
                                    }
                                    st3.tsbeg = tsnow;
                                }
                                Monitoring2State::ReadPending(st3) => {
                                    // We don't check again for `since` here. That's done in timeout checking.
                                    // So we could be here a little beyond timeout but we don't care about that.
                                    if ioid != st3.ioid {
                                        // warn!("IOID mismatch ReadNotifyRes on Monitor Read Pending  {ioid:?}  {ioid2:?}");
                                        self.mett.recv_read_notify_state_read_pending_bad_ioid().inc();
                                    } else {
                                        self.mett.recv_read_notify_state_read_pending().inc();
                                    }
                                    let read_expected = if let Some(_cid) = self.read_ioids.remove(&ioid) {
                                        self.mett.ioid_read_done().inc();
                                        true
                                    } else {
                                        self.mett.ioid_read_error_not_found().inc();
                                        false
                                    };
                                    st2.mon2state = Monitoring2State::Passive(Monitoring2PassiveState {
                                        tsbeg: tsnow,
                                        ts_silence_read_next: tsnow + Self::silence_read_next_ivl_rng(&mut self.rng),
                                    });
                                    if read_expected {
                                        let item = ChannelStatusItem {
                                            ts: self.tmp_ts_poll,
                                            cssid: st.channel.cssid.clone(),
                                            status: ChannelStatus::MonitoringReadResultExpected,
                                        };
                                        ch_wrst
                                            .emit_channel_status_item(item, Self::channel_status_qu(&mut self.iqdqs))?;
                                    } else {
                                        let item = ChannelStatusItem {
                                            ts: self.tmp_ts_poll,
                                            cssid: st.channel.cssid.clone(),
                                            status: ChannelStatus::MonitoringReadResultUnexpected,
                                        };
                                        ch_wrst
                                            .emit_channel_status_item(item, Self::channel_status_qu(&mut self.iqdqs))?;
                                    }
                                    let iqdqs = &mut self.iqdqs;
                                    let mett = &mut self.mett;
                                    // NOTE we do not update the last value in this ev handler.
                                    {
                                        if let Some(lst) = st2.monitoring_event_last.as_ref() {
                                            // TODO compare with last monitoring value
                                            if ev.value.data == lst.value.data {
                                                if ev.value.meta == lst.value.meta {
                                                    st2.last_comparisons
                                                        .push_back((UtcDateTime::now(), MonitorReadCmp::Equal));
                                                } else {
                                                    st2.last_comparisons
                                                        .push_back((UtcDateTime::now(), MonitorReadCmp::DiffTime));
                                                }
                                            } else {
                                                st2.last_comparisons
                                                    .push_back((UtcDateTime::now(), MonitorReadCmp::DiffValue));
                                            }
                                        }
                                        while st2.last_comparisons.len() > 6 {
                                            st2.last_comparisons.pop_front();
                                        }
                                    }
                                    // TODO check ADEL to see if monitor should have fired.
                                    // But there is still a small chance that the monitor will just received slightly later.
                                    // More involved check would be to raise a flag, wait for the expected monitor for some
                                    // timeout, and if we get nothing error out.
                                    // TODO read-result-after-monitor-silence
                                    if false {
                                        Self::read_notify_res_for_write(
                                            ev,
                                            ch_wrst,
                                            st,
                                            iqdqs,
                                            stnow,
                                            tsnow,
                                            tscaproto,
                                            ch_conf.use_ioc_time(),
                                            mett,
                                            &mut self.rng,
                                        )?;
                                    }
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
                self.mett.unknown_ioid().inc();
            }
            Ok(())
        }
    }

    fn read_notify_res_for_write(
        ev: proto::ReadNotifyRes,
        wrst: &mut WriterStatus,
        st: &mut WritableState,
        iqdqs: &mut InsertDeques,
        stnow: SystemTime,
        tsnow: Instant,
        tscaproto: Instant,
        use_ioc_time: bool,
        mett: &mut CaConnMetrics,
        rng: &mut Xoshiro128PlusPlus,
    ) -> Result<(), Error> {
        let crst = &mut st.channel;
        let writer = &mut st.writer;
        let binwriter = &mut st.binwriter;
        Self::event_add_ingest(
            ev.payload_len,
            ev.value,
            wrst,
            crst,
            writer,
            binwriter,
            iqdqs,
            tsnow,
            stnow,
            tscaproto,
            use_ioc_time,
            mett,
            rng,
        )?;
        Ok(())
    }

    fn event_add_ingest(
        payload_len: u32,
        value: CaEventValue,
        wrst: &mut WriterStatus,
        crst: &mut CreatedState,
        writer: &mut CaRtWriter,
        binwriter: &mut BinWriter,
        iqdqs: &mut InsertDeques,
        tsnow: Instant,
        stnow: SystemTime,
        tscaproto: Instant,
        use_ioc_time: bool,
        mett: &mut CaConnMetrics,
        rng: &mut Xoshiro128PlusPlus,
    ) -> Result<(), Error> {
        {
            use ca_proto::ca::proto::CaMetaValue::*;
            match &value.meta {
                CaMetaTime(meta) => {
                    if meta.status != 0 {
                        let sid = writer.series();
                        debug!("{:?}  status {:3}  severity {:3}", sid, meta.status, meta.severity);
                    }
                }
                _ => {}
            }
        }
        trace_event_incoming!("event_add_ingest  payload_len {}  value {:?}", payload_len, value);
        crst.ts_alive_last = tsnow;
        crst.ts_activity_last = tsnow;
        crst.st_activity_last = stnow;
        crst.item_recv_ivl_ema.tick(tsnow);
        crst.recv_count += 1;
        crst.recv_bytes += payload_len as u64;
        crst.acc_recv.push_written(payload_len);
        // TODO should attach these counters already to Writable state.
        if crst.ts_recv_value_status_emit_next <= tsnow {
            crst.ts_recv_value_status_emit_next = tsnow + Self::recv_value_status_emit_ivl_rng(rng);
            // TODO was only for debugging
            if false {
                let item = ChannelStatusItem {
                    ts: stnow,
                    cssid: crst.cssid,
                    status: ChannelStatus::MonitoringSilenceReadUnchanged,
                };
                if wrst
                    .emit_channel_status_item(item, Self::channel_status_qu(iqdqs))
                    .is_err()
                {
                    mett.logic_error().inc();
                }
            }
        }
        let tsev_local = TsNano::from_system_time(stnow);
        {
            let ts = value.ts().ok_or_else(|| Error::MissingTimestamp)?;
            let ts_diff = ts.abs_diff(tsev_local.ns());
            let ts_diff_dur = Duration::from_nanos(ts_diff);
            mett.ca_ts_off().push_dur_100us(ts_diff_dur);
        }
        {
            let tsev = if use_ioc_time {
                if let Some(x) = value.ts() {
                    TsNano::from_ns(x)
                } else {
                    tsev_local
                }
            } else {
                tsev_local
            };
            Self::check_ev_value_data(&value.data, &writer.scalar_type())?;
            crst.muted_before = 0;
            crst.insert_item_ivl_ema.tick(tsnow);
            if USE_BIN_WRITER {
                binwriter.ingest(tsev, value.f32_for_binning(), iqdqs)?;
            }
            {
                let wres = writer.write(CaWriterValue::new(value, crst), tscaproto, tsev, iqdqs)?;
                crst.status_emit_count += wres.nstatus() as u64;
                if wres.st.accept {
                    crst.dw_st_last = stnow;
                    crst.acc_st.push_written(payload_len);
                }
                if wres.mt.accept {
                    crst.dw_mt_last = stnow;
                    crst.acc_mt.push_written(payload_len);
                }
                if wres.lt.accept {
                    crst.dw_lt_last = stnow;
                    crst.acc_lt.push_written(payload_len);
                }
            }
        }
        if false {
            // TODO record stats on drop with the new filter
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
        use ca_proto::ca::proto::CaDataScalarValue;
        use ca_proto::ca::proto::CaDataValue;
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
                                error!("see some unexpected version {n}  channel search may not work.");
                                Ready(Some(Ok(())))
                            } else {
                                if n != 13 {
                                    warn!("received peer version {n}");
                                }
                                self.state = CaConnState::PeerReady;
                                Ready(Some(Ok(())))
                            }
                        }
                        CaMsgTy::CreateChanRes(k) => {
                            warn!("got unexpected {k:?}",);
                            Ready(Some(Ok(())))
                        }
                        CaMsgTy::AccessRightsRes(k) => {
                            warn!("got unexpected {k:?}",);
                            Ready(Some(Ok(())))
                        }
                        k => {
                            warn!("got some other unhandled message: {k:?}");
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
                ChannelState::FetchCaStatusSeries(_) => {}
                ChannelState::MakingSeriesWriter(_) => {}
                ChannelState::Writable(st2) => match &mut st2.reading {
                    ReadingState::EnableMonitoring(_) => {}
                    ReadingState::Monitoring(st3) => match &st3.mon2state {
                        Monitoring2State::Passive(st4) => {
                            if st4.tsbeg + conf.conf.manual_poll_on_quiet_after() < tsnow {
                                trace_monitor_stale!("check_channels_state_poll  Monitoring2State::Passive  timeout");
                                self.mett.monitor_stale_read_begin().inc();
                                // TODO encapsulate and unify with Polling handler
                                let ioid = Ioid(self.ioid);
                                self.ioid = self.ioid.wrapping_add(1);
                                self.read_ioids
                                    .entry(ioid)
                                    .and_modify(|_| {
                                        self.mett.ioid_read_error_exists().inc();
                                    })
                                    .or_insert_with(|| {
                                        self.mett.ioid_read_begin().inc();
                                        st2.channel.cid.clone()
                                    });
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
                                st3.mon2state =
                                    Monitoring2State::ReadPending(Monitoring2ReadPendingState { tsbeg: tsnow, ioid });
                                self.mett.caget_issued().inc();
                                {
                                    let item = ChannelStatusItem {
                                        ts: self.tmp_ts_poll,
                                        cssid: st2.channel.cssid.clone(),
                                        status: ChannelStatus::MonitoringSilenceReadStart,
                                    };
                                    conf.wrst
                                        .emit_channel_status_item(item, Self::channel_status_qu(&mut self.iqdqs))?;
                                }
                            }
                        }
                        Monitoring2State::ReadPending(st4) => {
                            if st4.tsbeg + MONITOR_POLL_TIMEOUT < tsnow {
                                // Something is wrong with this channel.
                                // Maybe we lost connection, maybe the IOC went down, maybe there is a bug where only
                                // this or a subset of the subscribed channels no longer give updates.
                                self.mett.monitor_stale_read_timeout().inc();
                                let name = conf.conf.name();
                                trace_monitor_stale!(
                                    "channel monitor explicit read timeout  {}  ioid {:?}",
                                    name,
                                    st4.ioid
                                );
                                {
                                    let item = ChannelStatusItem {
                                        ts: self.tmp_ts_poll,
                                        cssid: st2.channel.cssid.clone(),
                                        status: ChannelStatus::MonitoringSilenceReadTimeout,
                                    };
                                    conf.wrst
                                        .emit_channel_status_item(item, Self::channel_status_qu(&mut self.iqdqs))?;
                                }
                                if false {
                                    // Here we try to close the channel at hand.

                                    // TODO need to define the transition from operating channel to inoperable channel in
                                    // a better and reusable way:
                                    // Do not go directly into error state: need to at least attempt to close the channel and wait/timeout for reply.

                                    let proto = self.proto.as_mut().ok_or(Error::NoProtocol)?;
                                    let ty = CaMsgTy::ChannelClose(ChannelClose {
                                        sid: st2.channel.sid.0,
                                        cid: st2.channel.cid.0,
                                    });
                                    let item = CaMsg::from_ty_ts(ty, tsnow);
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
                        PollTickState::Idle(st4) => {
                            if st4.next <= tsnow {
                                let ioid = Ioid(self.ioid);
                                self.ioid = self.ioid.wrapping_add(1);
                                self.read_ioids
                                    .entry(ioid)
                                    .and_modify(|_| {
                                        self.mett.ioid_read_error_exists().inc();
                                    })
                                    .or_insert_with(|| {
                                        self.mett.ioid_read_begin().inc();
                                        st2.channel.cid.clone()
                                    });
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
                                st3.tick = PollTickState::Wait(PollTickStateWait {
                                    next_backup: st4.next,
                                    since: tsnow,
                                    ioid,
                                });
                                self.mett.caget_issued().inc();
                            }
                        }
                        PollTickState::Wait(st4) => {
                            if st4.since + POLL_READ_TIMEOUT <= tsnow {
                                if self.read_ioids.remove(&st4.ioid).is_some() {
                                    self.mett.ioid_read_timeout().inc();
                                }
                                self.mett.caget_timeout().inc();
                                let next = PollTickStateIdle::decide_next(st4.next_backup, st3.poll_ivl, tsnow);
                                if self.trace_channel_poll {
                                    trace!("make poll idle after poll timeout  {next:?}");
                                }
                                st3.tick = PollTickState::Idle(PollTickStateIdle { next });
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
                self.mett.pong_timeout().inc();
                info!("pong timeout {}", self.remote_addr_dbg);
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
                    self.mett.ping_start().inc();
                    self.ioc_ping_start = Some(tsnow);
                    self.ioc_ping_last = Some(self.tmp_ts_poll);
                    let msg = CaMsg::from_ty_ts(CaMsgTy::Echo, tsnow);
                    proto.push_out(msg);
                } else {
                    self.mett.ping_no_proto().inc();
                    info!("can not ping {}  no proto", self.remote_addr_dbg);
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
                            Monitoring2State::ReadPending(_) => {
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
                    } else {
                        alive_count += 1;
                    }
                }
                _ => {}
            }
        }
        // TODO STATS
        // self.stats.channel_all_count.__set(self.channels.len() as _);
        // self.stats.channel_alive_count.__set(alive_count as _);
        // self.stats.channel_not_alive_count.__set(not_alive_count as _);
        Ok(())
    }

    // Can return:
    // Pending, error, work-done (pending state unknown), no-more-work-ever-again.
    fn handle_peer_ready(&mut self, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let mut ts1 = Instant::now();
        // TODO unify with Listen state where protocol gets polled as well.
        let ts2 = Instant::now();
        // TODO STATS
        // self.mett
        //     .time_check_channels_state_init
        //     .add((ts2.duration_since(ts1) * 1000).as_secs());
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
                        let (msgcom, ty) = camsg.into_parts();
                        match &ty {
                            CaMsgTy::Version => {
                                if !self.version_seen {
                                    self.version_seen = true;
                                    if self.non_version_seen {
                                        self.mett.ca_proto_version_later().inc();
                                    }
                                }
                            }
                            CaMsgTy::VersionRes(_) => {
                                if !self.version_seen {
                                    self.version_seen = true;
                                    if self.non_version_seen {
                                        self.mett.ca_proto_version_later().inc();
                                    }
                                }
                            }
                            _ => {
                                if !self.non_version_seen {
                                    self.non_version_seen = true;
                                }
                                if !self.version_seen {
                                    if !self.non_version_seen_emitted {
                                        self.non_version_seen_emitted = true;
                                        self.mett.ca_proto_no_version_as_first().inc();
                                    }
                                }
                            }
                        }
                        match ty {
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
                                trace4!("got EventAddRes  {:?}  cnt {}", msgcom.ts(), ev.data_count);
                                self.mett.event_add_res_recv().inc();
                                Self::handle_event_add_res(self, ev, tsnow, msgcom.ts())?
                            }
                            CaMsgTy::EventAddResEmpty(ev) => {
                                trace4!("got EventAddResEmpty  {:?}", msgcom.ts());
                                Self::handle_event_add_res_empty(self, ev, tsnow)?
                            }
                            CaMsgTy::ReadNotifyRes(ev) => Self::handle_read_notify_res(self, ev, tsnow, msgcom.ts())?,
                            CaMsgTy::Echo => {
                                if let Some(started) = self.ioc_ping_start {
                                    let dt = started.elapsed();
                                    self.mett.pong_recv_lat().push_dur_100us(dt);
                                } else {
                                    let addr = &self.remote_addr_dbg;
                                    warn!("received Echo even though we didn't asked for it  {}", addr);
                                }
                                self.ioc_pong_last = Some(self.tmp_ts_poll);
                                self.ioc_ping_next = tsnow + Self::ioc_ping_ivl_rng(&mut self.rng);
                                self.ioc_ping_start = None;
                                self.emit_channel_event_pong();
                            }
                            CaMsgTy::CreateChanFail(msg) => {
                                // TODO
                                // Here, must indicate that the address could be wrong!
                                // The channel status must be "Fail" so that ConnSet can decide to re-search.
                                // TODO how to transition the channel state? Any invariants or simply write to the map?
                                let cid = Cid(msg.cid);
                                if let Some(conf) = self.channels.get(&cid) {
                                    let name = conf.conf.name();
                                    if series::dbg::dbg_chn(&name) {
                                        info!("queue event to notice channel create fail {name}");
                                    }
                                    let name2 = name.to_string();
                                    let failinfo = format!("name {}  cid {}", name, cid);
                                    let item = CaConnEvent {
                                        ts: tsnow,
                                        value: CaConnEventValue::ChannelCreateFail(failinfo),
                                    };
                                    self.ca_conn_event_out_queue.push_back(item);
                                    warn!("CreateChanFail  {}  msg {:?}", name, msg);
                                    self.log_channel(&name2, format!("CreateChanFail cid {}", msg.cid));
                                    self.channel_remove_by_cid(cid);
                                } else {
                                    let failinfo = format!("unexpected cid {}", cid);
                                    let item = CaConnEvent {
                                        ts: tsnow,
                                        value: CaConnEventValue::ChannelCreateFail(failinfo),
                                    };
                                    self.ca_conn_event_out_queue.push_back(item);
                                    warn!("CreateChanFail  cid not known  msg {:?}", msg);
                                    self.channel_remove_by_cid(cid);
                                }
                            }
                            CaMsgTy::Error(msg) => {
                                warn!("CaConn sees: {msg:?}");
                            }
                            CaMsgTy::AccessRightsRes(msg) => {
                                if false {
                                    warn!("CaConn sees: {msg:?}");
                                }
                            }
                            CaMsgTy::VersionRes(_) => {
                                // TODO must check that version is only ever the first message.
                                if self.non_version_seen {
                                    self.mett.ca_proto_version_later().inc();
                                }
                                self.version_seen = true;
                            }
                            CaMsgTy::ChannelCloseRes(ty) => {
                                self.handle_channel_close_res(ty, tsnow)?;
                            }
                            _ => {
                                warn!("Received unexpected protocol message {:?} {:?}", msgcom, ty);
                            }
                        }
                    }
                    CaItem::Empty => {}
                }
                self.mett.ca_msg_recv().inc();
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
            insert_item_ivl_ema: IntervalEma::new(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: tsnow,
            muted_before: 0,
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
            name: conf.conf.name().into(),
            enum_str_table: None,
            status_emit_count: 0,
            ts_recv_value_status_emit_next: Instant::now(),
        };
        if series::dbg::dbg_chn(created_state.name()) {
            info!(
                "handle_create_chan_res  {:?}  {}",
                created_state.cid,
                created_state.name()
            );
        }
        match &scalar_type {
            ScalarType::Enum => {
                // TODO channel created, now fetch enum variants, later make writer
                let fut = enumfetch::EnumFetch::new(created_state, self);
                // TODO should always check if the slot is free.
                let ioid = fut.ioid();
                let x = Box::pin(fut);
                self.handler_by_ioid.insert(ioid, Some(x));
            }
            _ => {
                let backend = self.backend.clone();
                let channel_name = created_state.name().into();
                // TODO create a channel for the answer.
                // Keep only a certain max number of channels in-flight because have to poll on them.
                // TODO register the channel for the answer.
                let (tx, rx) = async_channel::bounded(8);
                let item = ChannelInfoQuery {
                    backend,
                    channel: channel_name,
                    kind: SeriesKind::CaStatus,
                    scalar_type: ScalarType::I16,
                    shape: Shape::Scalar,
                    tx: Box::pin(tx),
                };
                self.channel_info_query_qu.push_back(item);
                self.channel_info_query_res_rxs.push_back((Box::pin(rx), cid));
                *chst = ChannelState::FetchCaStatusSeries(MakingSeriesWriterState {
                    tsbeg: tsnow,
                    channel: created_state,
                    series_status: SeriesId::new(0),
                });
            }
        }
        Ok(())
    }

    fn handle_channel_close_res(&mut self, k: proto::ChannelCloseRes, tsnow: Instant) -> Result<(), Error> {
        let _ = tsnow;
        debug!("{:?}", k);
        Ok(())
    }

    fn handle_conn_state(&mut self, tsnow: Instant, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        loop {
            break match &mut self.state {
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
                                    let raw_fd = {
                                        use std::os::fd::AsRawFd;
                                        let raw_fd = tcp.as_raw_fd();
                                        raw_fd
                                    };
                                    self.mett.tcp_connected().inc();
                                    let addr = addr.clone();
                                    self.emit_connection_status_item(ConnectionStatusItem {
                                        ts: self.tmp_ts_poll,
                                        addr,
                                        status: ConnectionStatus::Established,
                                    })?;
                                    self.backoff_reset();
                                    let proto = CaProto::new(
                                        TcpAsyncWriteRead::from(tcp),
                                        Some(raw_fd),
                                        self.remote_addr_dbg.to_string(),
                                        self.opts.array_truncate,
                                    );
                                    self.state = CaConnState::Init;
                                    self.proto = Some(proto);
                                    Ok(Ready(Some(())))
                                }
                                Ok(Err(e)) => {
                                    use std::io::ErrorKind;
                                    debug!("error connect to {addr} {e}");
                                    let addr = addr.clone();
                                    self.emit_connection_status_item(ConnectionStatusItem {
                                        ts: self.tmp_ts_poll,
                                        addr,
                                        status: ConnectionStatus::ConnectError,
                                    })?;
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
                                    self.emit_connection_status_item(ConnectionStatusItem {
                                        ts: self.tmp_ts_poll,
                                        addr,
                                        status: ConnectionStatus::ConnectTimeout,
                                    })?;
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
                    if true {
                        // because of bad java clients which do not send a version, skip the handshake.
                        self.state = CaConnState::PeerReady;
                        continue;
                    } else {
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
                CaConnState::MetricsEmitted => Ok(Ready(None)),
            };
        }
    }

    fn loop_inner(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        let tsnow = Instant::now();
        let mut have_progress = false;
        let mut have_pending = false;
        for _ in 0..64 {
            self.mett.loop2_count().inc();
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
                        Pending => {
                            have_pending = true;
                            break;
                        }
                    },
                    Err(e) => return Err(e),
                }
            };
        }
        if have_progress {
            Ok(Ready(Some(())))
        } else if have_pending {
            Ok(Pending)
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
        let tsnow = Instant::now();
        if !self.is_shutdown() {
            self.ticker = Self::new_self_ticker(&mut self.rng);
            match self.ticker.poll_unpin(cx) {
                Poll::Pending => {}
                _ => {
                    return Err(Error::FutLogic);
                }
            }
            // cx.waker().wake_by_ref();
        }
        self.housekeeping_self();
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
            match self.tick_writers() {
                Ok(()) => {}
                Err(e) => {
                    error!("error in writers: {e}");
                }
            }
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
            CaConnState::MetricsEmitted => {}
        }
        self.iqdqs.housekeeping();
        if self.metrics_emit_last + METRICS_EMIT_IVL <= tsnow {
            self.metrics_emit_last = tsnow;
            self.metrics_emit();
        }
        Ok(())
    }

    fn housekeeping_self(&mut self) {}

    fn metrics_emit(&mut self) {
        if let Some(x) = self.proto.as_mut() {
            let fionread = if let Some(rawfd) = x.get_raw_socket_fd() {
                let mut v = 0;
                if unsafe { libc::ioctl(rawfd, libc::FIONREAD, &mut v) } == 0 {
                    Some(v as u32)
                } else {
                    None
                }
            } else {
                None
            };
            let mett = x.mett();
            mett.metrics_emit().inc();
            if let Some(fionread) = fionread {
                if fionread > self.fionread_last {
                    let diff = fionread - self.fionread_last;
                    self.fionread_last = fionread;
                    mett.fionread_inc().add(diff);
                } else if fionread < self.fionread_last {
                    let diff = self.fionread_last - fionread;
                    self.fionread_last = fionread;
                    mett.fionread_dec().add(diff);
                }
            }
            let m = mett.take_and_reset();
            self.mett.proto().ingest(m);
        }
        self.mett.metrics_emit().inc();
        let item = self.mett.take_and_reset();
        let item = CaConnEvent::new(Instant::now(), CaConnEventValue::Metrics(item));
        self.ca_conn_event_out_queue.push_back(item);
    }

    fn emit_channel_status(&mut self) -> Result<(), Error> {
        let stnow = SystemTime::now();
        let mut channel_statuses = BTreeMap::new();
        for (_, conf) in self.channels.iter() {
            let chst = &conf.state;
            let chinfo = chst.to_info(chst.cssid(), self.remote_addr_dbg, conf.conf.clone(), stnow, self);
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
            self.mett.out_queue_full().inc();
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
                                let series = st1.writer.series();
                                let item = Accounting {
                                    part: (series.id() & 0xff) as i32,
                                    ts: acc.beg,
                                    series,
                                    count: acc.usage().count() as _,
                                    bytes: acc.usage().bytes() as _,
                                };
                                self.iqdqs.emit_accounting_item(rt, item)?;
                            }
                            acc.reset(msp);
                        }
                    }
                    {
                        let acc = &mut ch.acc_recv;
                        if acc.beg != msp {
                            if acc.usage().count() != 0 {
                                let series = st1.writer.series();
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

    fn emit_channel_event_pong(&mut self) {
        for (_, ch) in self.channels.iter_mut() {
            match &mut ch.state {
                ChannelState::Init(_) => {}
                ChannelState::Creating(_) => {}
                ChannelState::FetchEnumDetails(_) => {}
                ChannelState::FetchCaStatusSeries(_) => {}
                ChannelState::MakingSeriesWriter(_) => {}
                ChannelState::Writable(st1) => {
                    let item = ChannelStatusItem {
                        ts: self.tmp_ts_poll,
                        cssid: st1.channel.cssid,
                        status: ChannelStatus::Pong,
                    };
                    let dt = self
                        .poll_tsnow
                        .saturating_duration_since(self.ts_channel_status_pong_last);
                    let qu = if dt >= CHANNEL_STATUS_PONG_QUIET {
                        self.ts_channel_status_pong_last = self.poll_tsnow;
                        Self::channel_status_qu(&mut self.iqdqs)
                    } else {
                        Self::channel_status_pong_qu(&mut self.iqdqs)
                    };
                    if ch.wrst.emit_channel_status_item(item, qu).is_err() {
                        self.mett.logic_error().inc();
                    }
                }
                ChannelState::Closing(_) => {}
                ChannelState::Error(_) => {}
                ChannelState::Ended(_) => {}
            }
        }
    }

    fn tick_writers(&mut self) -> Result<(), Error> {
        for (_, chconf) in &mut self.channels {
            let chst = &mut chconf.state;
            if let ChannelState::Writable(st2) = chst {
                let iqdqs = &mut self.iqdqs;
                st2.writer.tick(iqdqs)?;
                if USE_BIN_WRITER {
                    st2.binwriter.tick(iqdqs)?;
                }
            }
        }
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
        mut stats: FS,
        mett: &mut stats::mett::CaConnMetrics,
    ) -> Result<Poll<Option<()>>, Error>
    where
        Q: Unpin,
        FB: Fn(&mut VecDeque<T>) -> Option<Q>,
        FS: for<'a, 'b> FnMut(&'a Q, &'b mut stats::mett::CaConnMetrics),
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
                    stats(&item, mett);
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
        if qu.len() < qu.capacity() * 4 / 10 {
            qu.shrink_to(qu.capacity() * 7 / 10);
        }
        let sp = obj.$sp.as_mut();
        let mett = &mut obj.mett;
        match Self::attempt_flush_queue(qu, sp, $batcher, $loop_max, $cx, $id, $stats, mett) {
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
        if qu.len() < qu.capacity() * 4 / 10 {
            qu.shrink_to(qu.capacity() * 7 / 10);
        }
        let sp = obj.iqsp.as_mut().$sp();
        let mett = &mut obj.mett;
        match Self::attempt_flush_queue(qu, sp, $batcher, $loop_max, $cx, $id, $stats, mett) {
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
        self.mett.poll_fn_begin().inc();
        let mut reloops: u32 = 0;
        let ret = loop {
            let lts1 = Instant::now();

            self.mett.poll_loop_begin().inc();
            let qlen = self.iqdqs.len();
            if qlen >= self.opts.insert_queue_max * 2 / 3 {
                self.mett.insert_item_queue_pressure().inc();
            } else if qlen >= self.opts.insert_queue_max {
                self.mett.insert_item_queue_full().inc();
            }

            let mut have_pending = false;
            let mut have_progress = false;

            if let CaConnState::MetricsEmitted = self.state {
                break Ready(None);
            } else if let CaConnState::EndOfStream = self.state {
                self.mett.metrics_emit_final().inc();
                let mett = self.mett.take_and_reset();
                self.state = CaConnState::MetricsEmitted;
                break Ready(Some(CaConnEvent::new_now(CaConnEventValue::Metrics(mett))));
                // break Ready(None);
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
                flush_queue_dqs!(
                    self,
                    st_rf1_qu,
                    st_rf1_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "st_rf1_rx",
                    cx,
                    |item: &VecDeque<QueryItem>, mett: &mut stats::mett::CaConnMetrics| {
                        mett.iiq_batch_len().push_val(item.len() as u32);
                    }
                );

                flush_queue_dqs!(
                    self,
                    st_rf3_qu,
                    st_rf3_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "st_rf3_rx",
                    cx,
                    |item: &VecDeque<QueryItem>, mett: &mut stats::mett::CaConnMetrics| {
                        mett.iiq_batch_len().push_val(item.len() as u32);
                    }
                );

                flush_queue_dqs!(
                    self,
                    mt_rf3_qu,
                    mt_rf3_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "mt_rf3_rx",
                    cx,
                    |item: &VecDeque<QueryItem>, mett: &mut stats::mett::CaConnMetrics| {
                        mett.iiq_batch_len().push_val(item.len() as u32);
                    }
                );

                flush_queue_dqs!(
                    self,
                    lt_rf3_qu,
                    lt_rf3_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "lt_rf3_rx",
                    cx,
                    |item: &VecDeque<QueryItem>, mett: &mut stats::mett::CaConnMetrics| {
                        mett.iiq_batch_len().push_val(item.len() as u32);
                    }
                );

                flush_queue_dqs!(
                    self,
                    lt_rf3_lat5_qu,
                    lt_rf3_lat5_sp_pin,
                    send_batched::<256, _>,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "lt_rf3_lat5_rx",
                    cx,
                    |item: &VecDeque<QueryItem>, mett: &mut stats::mett::CaConnMetrics| {
                        mett.iiq_batch_len().push_val(item.len() as u32);
                    }
                );
            }

            let lts3 = Instant::now();

            if !self.is_shutdown() {
                flush_queue!(
                    self,
                    channel_info_query_qu,
                    channel_info_query_tx,
                    send_individual,
                    32,
                    (&mut have_progress, &mut have_pending),
                    "chinf",
                    cx,
                    |_, _| {}
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
            // TODO STATS
            self.mett.poll_op3_dt().push_dur_100us(dt);
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
                        self.mett.poll_reloop().inc();
                        reloops += 1;
                        continue;
                    } else if have_pending {
                        debug!("is_shutdown  NOT queues_out_flushed  pend  {}", self.remote_addr_dbg);
                        self.log_queues_summary();
                        self.mett.poll_pending().inc();
                        Pending
                    } else {
                        // TODO error
                        error!("shutting down, queues not flushed, no progress, no pending");
                        self.mett.logic_error().inc();
                        let e = Error::ShutdownWithQueuesNoProgressNoPending;
                        Ready(Some(CaConnEvent::err_now(e)))
                    }
                }
            } else {
                if have_progress {
                    if poll_ts1.elapsed() > Duration::from_millis(5) {
                        self.mett.poll_wake_break().inc();
                        cx.waker().wake_by_ref();
                        break Ready(Some(CaConnEvent::new(self.poll_tsnow, CaConnEventValue::None)));
                    } else {
                        self.mett.poll_reloop().inc();
                        reloops += 1;
                        continue;
                    }
                } else if have_pending {
                    self.mett.poll_pending().inc();
                    Pending
                } else {
                    self.mett.poll_no_progress_no_pending().inc();
                    let e = Error::NoProgressNoPending;
                    Ready(Some(CaConnEvent::err_now(e)))
                }
            };
        };
        let poll_ts2 = Instant::now();
        let dt = poll_ts2.saturating_duration_since(poll_ts1);
        if self.trace_channel_poll {
            self.mett.poll_all_dt().push_dur_100us(dt);
        }
        self.mett.poll_reloops().push_val(reloops);
        ret
    }
}

#[derive(Debug, Serialize)]
struct CaWriterValueState {
    series_data: SeriesId,
    series_status: SeriesId,
    last_accepted_ts: TsNano,
    last_accepted_val: Option<CaWriterValue>,
    msp_split_status: MspSplit,
    msp_split_data: MspSplit,
}

impl CaWriterValueState {
    fn new(series_status: SeriesId, series_data: SeriesId) -> Self {
        Self {
            series_data,
            series_status,
            last_accepted_ts: TsNano::from_ns(0),
            last_accepted_val: None,
            msp_split_status: MspSplit::new(1024 * 64, 1024 * 1024 * 10),
            msp_split_data: MspSplit::new(1024 * 64, 1024 * 1024 * 10),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct CaWriterValue(CaEventValue, Option<String>);

impl CaWriterValue {
    fn new(val: CaEventValue, crst: &CreatedState) -> Self {
        let valstr = match &val.data {
            CaDataValue::Scalar(val) => {
                use ca_proto::ca::proto::CaDataScalarValue;
                match val {
                    CaDataScalarValue::Enum(x) => {
                        let x = *x;
                        let table = crst.enum_str_table.as_ref();
                        let conv = table.map_or_else(
                            || String::from("missingstrings"),
                            |map| {
                                map.get(x as usize)
                                    .map_or_else(|| String::from("undefined"), String::from)
                            },
                        );
                        // trace!("CaWriterValue  convert enum  {}  {:?}", crst.name(), conv);
                        Some(conv)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        Self(val, valstr)
    }
}

impl EmittableType for CaWriterValue {
    type State = CaWriterValueState;

    fn ts(&self) -> TsNano {
        TsNano::from_ns(self.0.ts().unwrap_or(0))
    }

    fn has_change(&self, k: &Self) -> bool {
        if self.0.data != k.0.data {
            true
        } else if self.0.meta != k.0.meta {
            true
        } else {
            false
        }
    }

    fn byte_size(&self) -> u32 {
        self.0.data.byte_size()
    }

    fn into_query_item(
        mut self,
        ts_net: Instant,
        tsev: TsNano,
        state: &mut <Self as EmittableType>::State,
    ) -> serieswriter::writer::EmitRes {
        let mut items = serieswriter::writer::SmallVec::new();
        let diff_data = match state.last_accepted_val.as_ref() {
            Some(last) => self.0.data != last.0.data,
            None => true,
        };
        let diff_status = match state.last_accepted_val.as_ref() {
            Some(last) => match &last.0.meta {
                proto::CaMetaValue::CaMetaTime(last_meta) => match &self.0.meta {
                    proto::CaMetaValue::CaMetaTime(meta) => meta.status != last_meta.status,
                    _ => false,
                },
                _ => false,
            },
            None => true,
        };
        let ts = tsev;
        state.last_accepted_val = Some(self.clone());
        let byte_size = self.byte_size();
        if diff_data {
            // debug!("diff_data    emit {:?}", state.series_data);
            let (ts_msp, ts_lsp, ts_msp_chg) = state.msp_split_data.split(ts, self.byte_size());
            let data_value = {
                use ca_proto::ca::proto::CaDataValue;
                use scywr::iteminsertqueue::DataValue;
                let ret = match self.0.data {
                    CaDataValue::Scalar(val) => DataValue::Scalar({
                        use ca_proto::ca::proto::CaDataScalarValue;
                        use scywr::iteminsertqueue::ScalarValue;
                        match val {
                            CaDataScalarValue::I8(x) => ScalarValue::I8(x),
                            CaDataScalarValue::I16(x) => ScalarValue::I16(x),
                            CaDataScalarValue::I32(x) => ScalarValue::I32(x),
                            CaDataScalarValue::F32(x) => ScalarValue::F32(x),
                            CaDataScalarValue::F64(x) => ScalarValue::F64(x),
                            CaDataScalarValue::Enum(x) => ScalarValue::Enum(
                                x,
                                self.1.take().unwrap_or_else(|| {
                                    warn!("NoEnumStr");
                                    String::from("NoEnumStr")
                                }),
                            ),
                            CaDataScalarValue::String(x) => ScalarValue::String(x),
                            CaDataScalarValue::Bool(x) => ScalarValue::Bool(x),
                        }
                    }),
                    CaDataValue::Array(val) => DataValue::Array({
                        use ca_proto::ca::proto::CaDataArrayValue;
                        use scywr::iteminsertqueue::ArrayValue;
                        match val {
                            CaDataArrayValue::I8(x) => ArrayValue::I8(x),
                            CaDataArrayValue::I16(x) => ArrayValue::I16(x),
                            CaDataArrayValue::I32(x) => ArrayValue::I32(x),
                            CaDataArrayValue::F32(x) => ArrayValue::F32(x),
                            CaDataArrayValue::F64(x) => ArrayValue::F64(x),
                            CaDataArrayValue::Bool(x) => ArrayValue::Bool(x),
                        }
                    }),
                };
                ret
            };
            if ts_msp_chg {
                items.push(QueryItem::Msp(MspItem::new(
                    state.series_data.clone(),
                    ts_msp.to_ts_ms(),
                    ts_net,
                )));
            }
            let item = scywriiq::InsertItem {
                series: state.series_data.clone(),
                ts_msp: ts_msp.to_ts_ms(),
                ts_lsp,
                ts_net,
                val: data_value,
            };
            items.push(QueryItem::Insert(item));
        }
        let mut n_status = 0;
        if diff_status {
            use scywr::iteminsertqueue::DataValue;
            use scywr::iteminsertqueue::ScalarValue;
            match self.0.meta {
                proto::CaMetaValue::CaMetaTime(meta) => {
                    let (ts_msp, ts_lsp, ts_msp_chg) = state.msp_split_status.split(ts, 2);
                    if ts_msp_chg {
                        items.push(QueryItem::Msp(MspItem::new(
                            state.series_status.clone(),
                            ts_msp.to_ts_ms(),
                            ts_net,
                        )));
                    }
                    let data_value = DataValue::Scalar(ScalarValue::I16(meta.status as i16));
                    let item = scywriiq::InsertItem {
                        series: state.series_status.clone(),
                        ts_msp: ts_msp.to_ts_ms(),
                        ts_lsp,
                        ts_net,
                        val: data_value,
                    };
                    items.push(QueryItem::Insert(item));
                    n_status += 1;
                    // info!("diff_status  emit {:?}", state.series_status);
                }
                _ => {
                    // TODO must be able to return error here
                    warn!("diff_status logic error");
                }
            };
        }
        let ret = serieswriter::writer::EmitRes {
            items,
            bytes: byte_size,
            status: n_status,
        };
        ret
    }
}
