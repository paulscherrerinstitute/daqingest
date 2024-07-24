use crate::netbuf;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use log::*;
use netpod::timeunits::*;
use slidebuf::SlideBuf;
use stats::CaProtoStats;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use taskrun::tokio;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

#[derive(Debug, ThisError)]
#[cstm(name = "NetfetchCaProto")]
pub enum Error {
    NetBuf(#[from] netbuf::Error),
    SlideBuf(#[from] slidebuf::Error),
    #[error("BufferTooSmallForNeedMin({0}, {1})")]
    BufferTooSmallForNeedMin(usize, usize),
    IO(#[from] io::Error),
    BadSlice,
    BadCaDbrTypeId(u16),
    BadCaScalarTypeId(u16),
    GetValHelpInnerTypeMismatch,
    GetValHelpTodoWaveform,
    NotEnoughPayload,
    TodoConversionArray,
    CaProtoVersionMissing,
    NotEnoughPayloadTimeMetadata(usize),
    MismatchDbrTimeType,
    BadCaCount,
    CaCommandNotSupported(u16),
    ParseAttemptInDoneState,
    UnexpectedHeader,
    ExtendedHeaderBadCount,
    NoReadBufferSpace,
    NeitherPendingNorProgress,
    OutputBufferTooSmall,
    LogicError,
    BadPayload,
}

const CA_PROTO_VERSION: u32 = 13;
const EPICS_EPOCH_OFFSET: u64 = 631152000;
const PAYLOAD_LEN_MAX: u32 = 1024 * 1024 * 32;
const PROTO_INPUT_BUF_CAP: u32 = 1024 * 1024 * 40;

const TESTING_UNRESPONSIVE_TODO_REMOVE: bool = false;
const TESTING_EVENT_ADD_RES_MAX: u32 = 3;

const TESTING_PROTOCOL_ERROR_TODO_REMOVE: bool = false;
const TESTING_PROTOCOL_ERROR_AFTER_BYTES: u32 = 400;

#[derive(Debug)]
pub struct Search {
    pub id: u32,
    pub channel: String,
}

#[derive(Debug)]
pub struct SearchRes {
    pub addr: u32,
    pub tcp_port: u16,
    pub id: u32,
    pub proto_version: u16,
}

#[derive(Debug)]
pub struct ErrorCmd {
    pub cid: u32,
    pub eid: u32,
    pub msg: String,
}

#[derive(Debug)]
pub struct ClientNameRes {
    pub name: String,
}

#[derive(Debug)]
pub struct CreateChan {
    pub cid: u32,
    pub channel: String,
}

#[derive(Debug)]
pub struct CreateChanRes {
    pub data_type: u16,
    pub data_count: u32,
    pub cid: u32,
    pub sid: u32,
}

#[derive(Debug)]
pub struct CreateChanFail {
    pub cid: u32,
}

#[derive(Debug)]
pub struct AccessRightsRes {
    pub cid: u32,
    pub rights: u32,
}

#[derive(Debug)]
pub struct EventAdd {
    pub data_type: u16,
    pub data_count: u32,
    pub sid: u32,
    pub subid: u32,
}

#[derive(Debug)]
pub struct EventCancel {
    pub data_type: u16,
    pub data_count: u32,
    pub sid: u32,
    pub subid: u32,
}

#[derive(Debug)]
pub struct EventCancelRes {
    pub data_type: u16,
    pub sid: u32,
    pub subid: u32,
}

// TODO Clone is only used for testing purposes and should get removed later.
#[derive(Debug, Clone)]
pub struct EventAddRes {
    pub data_type: u16,
    pub data_count: u32,
    pub status: u32,
    pub subid: u32,
    pub payload_len: u32,
    pub value: CaEventValue,
}

#[derive(Debug, Clone)]
pub struct EventAddResEmpty {
    pub data_type: u16,
    pub sid: u32,
    pub subid: u32,
}

#[derive(Debug)]
pub struct ReadNotify {
    pub data_type: u16,
    pub data_count: u32,
    pub sid: u32,
    pub ioid: u32,
}

#[derive(Debug)]
pub struct ReadNotifyRes {
    pub data_type: u16,
    pub data_count: u32,
    pub sid: u32,
    pub ioid: u32,
    pub payload_len: u32,
    pub value: CaEventValue,
}

#[derive(Debug)]
pub struct ChannelClose {
    pub sid: u32,
    pub cid: u32,
}

#[derive(Debug)]
pub struct ChannelCloseRes {
    pub sid: u32,
    pub cid: u32,
}

// This message is only sent from server to client, on server's initiative.
#[derive(Debug)]
pub struct ChannelDisconnect {
    pub cid: u32,
}

#[derive(Debug)]
enum CaScalarType {
    I8,
    I16,
    I32,
    F32,
    F64,
    Enum,
    String,
}

#[derive(Debug)]
enum CaDbrMetaType {
    Plain,
    Status,
    Time,
    Ctrl,
}

#[derive(Debug)]
pub struct CaDbrType {
    meta: CaDbrMetaType,
    scalar_type: CaScalarType,
}

impl CaDbrType {
    pub fn from_ca_u16(k: u16) -> Result<Self, Error> {
        if k == 31 {
            let ret = CaDbrType {
                meta: CaDbrMetaType::Ctrl,
                scalar_type: CaScalarType::Enum,
            };
            return Ok(ret);
        }
        if k > 20 {
            return Err(Error::BadCaDbrTypeId(k));
        }
        let (meta, k) = if k >= 14 {
            (CaDbrMetaType::Time, k - 14)
        } else if k >= 7 {
            (CaDbrMetaType::Status, k - 7)
        } else {
            (CaDbrMetaType::Plain, k)
        };
        use CaScalarType::*;
        let scalar_type = match k {
            4 => I8,
            1 => I16,
            5 => I32,
            2 => F32,
            6 => F64,
            3 => Enum,
            0 => String,
            k => return Err(Error::BadCaScalarTypeId(k)),
        };
        Ok(CaDbrType { meta, scalar_type })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CaDataScalarValue {
    I8(i8),
    I16(i16),
    I32(i32),
    F32(f32),
    F64(f64),
    Enum(i16),
    String(String),
    // TODO remove, CA has no bool, make new enum for other use cases.
    Bool(bool),
}

impl CaDataScalarValue {
    fn byte_size(&self) -> u32 {
        match self {
            CaDataScalarValue::I8(_) => 1,
            CaDataScalarValue::I16(_) => 2,
            CaDataScalarValue::I32(_) => 4,
            CaDataScalarValue::F32(_) => 4,
            CaDataScalarValue::F64(_) => 8,
            CaDataScalarValue::Enum(_) => 2,
            CaDataScalarValue::String(v) => v.len() as u32,
            CaDataScalarValue::Bool(_) => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CaDataArrayValue {
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    // TODO remove, CA has no bool, make new enum for other use cases.
    Bool(Vec<bool>),
}

impl CaDataArrayValue {
    fn byte_size(&self) -> u32 {
        match self {
            CaDataArrayValue::I8(x) => 1 * x.len() as u32,
            CaDataArrayValue::I16(x) => 2 * x.len() as u32,
            CaDataArrayValue::I32(x) => 4 * x.len() as u32,
            CaDataArrayValue::F32(x) => 4 * x.len() as u32,
            CaDataArrayValue::F64(x) => 8 * x.len() as u32,
            CaDataArrayValue::Bool(x) => 1 * x.len() as u32,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CaDataValue {
    Scalar(CaDataScalarValue),
    Array(CaDataArrayValue),
}

impl CaDataValue {
    pub fn byte_size(&self) -> u32 {
        match self {
            CaDataValue::Scalar(x) => x.byte_size(),
            CaDataValue::Array(x) => x.byte_size(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaEventValue {
    pub data: CaDataValue,
    pub meta: CaMetaValue,
}

impl CaEventValue {
    // Timestamp ns from unix epoch.
    pub fn ts(&self) -> Option<u64> {
        match &self.meta {
            CaMetaValue::CaMetaTime(x) => {
                let ts = SEC * (x.ca_secs as u64 + EPICS_EPOCH_OFFSET) + x.ca_nanos as u64;
                Some(ts)
            }
            CaMetaValue::CaMetaVariants(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CaMetaValue {
    CaMetaTime(CaMetaTime),
    CaMetaVariants(CaMetaVariants),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaMetaTime {
    pub status: u16,
    pub severity: u16,
    pub ca_secs: u32,
    pub ca_nanos: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaMetaVariants {
    pub status: u16,
    pub severity: u16,
    pub variants: Vec<String>,
}

#[derive(Debug)]
pub enum CaMsgTy {
    Version,
    VersionRes(u16),
    Error(ErrorCmd),
    ClientName,
    ClientNameRes(ClientNameRes),
    HostName(String),
    Search(Search),
    SearchRes(SearchRes),
    CreateChan(CreateChan),
    CreateChanRes(CreateChanRes),
    CreateChanFail(CreateChanFail),
    AccessRightsRes(AccessRightsRes),
    EventAdd(EventAdd),
    EventAddRes(EventAddRes),
    EventAddResEmpty(EventAddResEmpty),
    EventCancel(EventCancel),
    EventCancelRes(EventCancelRes),
    ReadNotify(ReadNotify),
    ReadNotifyRes(ReadNotifyRes),
    ChannelClose(ChannelClose),
    ChannelCloseRes(ChannelCloseRes),
    ChannelDisconnect(ChannelDisconnect),
    Echo,
}

impl CaMsgTy {
    fn cmdid(&self) -> u16 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            Error(_) => 0x0b,
            ClientName => 0x14,
            ClientNameRes(_) => 0x14,
            HostName(_) => 0x15,
            Search(_) => 0x06,
            SearchRes(_) => 0x06,
            CreateChan(_) => 0x12,
            CreateChanRes(_) => 0x12,
            CreateChanFail(_) => 0x1a,
            AccessRightsRes(_) => 0x16,
            EventAdd(_) => 0x01,
            EventAddRes(_) => 0x01,
            // sic: the response to event-cancel is an event-add:
            EventAddResEmpty(_) => 0x01,
            EventCancel(_) => 0x02,
            // sic: the response to event-cancel is an event-add:
            EventCancelRes(_) => 0x01,
            ReadNotify(_) => 0x0f,
            ReadNotifyRes(_) => 0x0f,
            ChannelClose(_) => 0x0c,
            ChannelCloseRes(_) => 0x0c,
            ChannelDisconnect(_) => 0x1b,
            Echo => 0x17,
        }
    }

    fn len(&self) -> usize {
        if self.payload_len() <= 0x3ff0 && self.data_count() <= 0xffff {
            16 + self.payload_len()
        } else {
            24 + self.payload_len()
        }
    }

    fn payload_len(&self) -> usize {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            Error(x) => (16 + x.msg.len() + 1 + 7) / 8 * 8,
            ClientName => 0x10,
            ClientNameRes(x) => (x.name.len() + 1 + 7) / 8 * 8,
            HostName(x) => (x.len() + 1 + 7) / 8 * 8,
            Search(x) => (x.channel.len() + 1 + 7) / 8 * 8,
            SearchRes(_) => 8,
            CreateChan(x) => (x.channel.len() + 1 + 7) / 8 * 8,
            CreateChanRes(_) => 0,
            CreateChanFail(_) => 0,
            AccessRightsRes(_) => 0,
            EventAdd(_) => 16,
            EventAddRes(_) => {
                error!("should not attempt to serialize the response again");
                panic!();
            }
            EventAddResEmpty(_) => 0,
            EventCancel(_) => 0,
            EventCancelRes(_) => 0,
            ReadNotify(_) => 0,
            ReadNotifyRes(_) => {
                error!("should not attempt to serialize the response again");
                panic!();
            }
            ChannelClose(_) => 0,
            ChannelCloseRes(_) => 0,
            ChannelDisconnect(_) => 0,
            Echo => 0,
        }
    }

    fn data_type(&self) -> u16 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(n) => *n,
            Error(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName(_) => 0,
            Search(_) => {
                // Reply-flag
                1
            }
            SearchRes(x) => x.tcp_port,
            CreateChan(_) => 0,
            CreateChanRes(x) => x.data_type,
            CreateChanFail(_) => 0,
            AccessRightsRes(_) => 0,
            EventAdd(x) => x.data_type,
            EventAddRes(x) => x.data_type,
            EventAddResEmpty(x) => x.data_type,
            EventCancel(x) => x.data_type,
            EventCancelRes(x) => x.data_type,
            ReadNotify(x) => x.data_type,
            ReadNotifyRes(x) => x.data_type,
            ChannelClose(_) => 0,
            ChannelCloseRes(_) => 0,
            ChannelDisconnect(_) => 0,
            Echo => 0,
        }
    }

    fn data_count(&self) -> u32 {
        use CaMsgTy::*;
        match self {
            Version => CA_PROTO_VERSION,
            VersionRes(_) => 0,
            Error(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName(_) => 0,
            Search(_) => CA_PROTO_VERSION,
            SearchRes(_) => 0,
            CreateChan(_) => 0,
            CreateChanRes(..) => {
                panic!();
                // x.data_count as _
            }
            CreateChanFail(_) => 0,
            AccessRightsRes(_) => 0,
            EventAdd(x) => x.data_count,
            EventAddRes(..) => {
                panic!();
                // x.data_count as _
            }
            EventAddResEmpty(_) => 0,
            EventCancel(x) => x.data_count,
            EventCancelRes(..) => 0,
            ReadNotify(x) => x.data_count,
            ReadNotifyRes(..) => {
                panic!();
                // x.data_count as _
            }
            ChannelClose(_) => 0,
            ChannelCloseRes(_) => 0,
            ChannelDisconnect(_) => 0,
            Echo => 0,
        }
    }

    fn param1(&self) -> u32 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            Error(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName(_) => 0,
            Search(e) => e.id,
            SearchRes(x) => x.addr,
            CreateChan(x) => x.cid,
            CreateChanRes(x) => x.cid,
            CreateChanFail(x) => x.cid,
            AccessRightsRes(x) => x.cid,
            EventAdd(x) => x.sid,
            EventAddRes(x) => x.status,
            EventAddResEmpty(x) => x.sid,
            EventCancel(x) => x.sid,
            EventCancelRes(x) => x.sid,
            ReadNotify(x) => x.sid,
            ReadNotifyRes(x) => x.sid,
            ChannelClose(x) => x.sid,
            ChannelCloseRes(x) => x.sid,
            ChannelDisconnect(x) => x.cid,
            Echo => 0,
        }
    }

    fn param2(&self) -> u32 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            Error(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName(_) => 0,
            Search(e) => e.id,
            SearchRes(x) => x.id,
            CreateChan(_) => CA_PROTO_VERSION as _,
            CreateChanRes(x) => x.sid,
            CreateChanFail(_) => 0,
            AccessRightsRes(x) => x.rights,
            EventAdd(x) => x.subid,
            EventAddRes(x) => x.subid,
            EventAddResEmpty(x) => x.subid,
            EventCancel(x) => x.subid,
            EventCancelRes(x) => x.subid,
            ReadNotify(x) => x.ioid,
            ReadNotifyRes(x) => x.ioid,
            ChannelClose(x) => x.cid,
            ChannelCloseRes(x) => x.cid,
            ChannelDisconnect(_) => 0,
            Echo => 0,
        }
    }

    fn place_payload_into(&self, buf: &mut [u8]) {
        use CaMsgTy::*;
        match self {
            Version => {}
            VersionRes(_) => {}
            // Specs: error cmd only from server to client.
            Error(_) => todo!(),
            ClientName => {
                // TODO allow variable client name.
                let s = "daqingest".as_bytes();
                let n = s.len();
                buf.fill(0);
                buf[..n].copy_from_slice(s);
            }
            ClientNameRes(_) => {
                error!("should not attempt to write ClientNameRes");
                panic!();
            }
            HostName(name) => {
                let s = name.as_bytes();
                let n = s.len();
                buf.fill(0);
                buf[..n].copy_from_slice(s);
            }
            Search(e) => {
                for x in &mut buf[..] {
                    *x = 0;
                }
                let d = e.channel.as_bytes();
                if buf.len() < d.len() + 1 {
                    error!("bad buffer given for search payload {} vs {}", buf.len(), d.len());
                    panic!();
                }
                buf[0..d.len()].copy_from_slice(&d[0..d.len()]);
            }
            SearchRes(_) => {
                error!("should not attempt to write SearchRes");
                panic!();
            }
            CreateChan(x) => {
                for x in &mut buf[..] {
                    *x = 0;
                }
                let d = x.channel.as_bytes();
                if buf.len() < d.len() + 1 {
                    error!("bad buffer given for create chan payload {} vs {}", buf.len(), d.len());
                    panic!();
                }
                buf[0..d.len()].copy_from_slice(&d[0..d.len()]);
            }
            CreateChanRes(_) => {}
            CreateChanFail(_) => {}
            AccessRightsRes(_) => {}
            EventAdd(_) => {
                // Using flags DBE_ARCHIVE, DBE_ALARM, DBE_PROPERTY.
                let flags = 0b1110;
                buf.copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, flags, 0, 0]);
            }
            EventAddRes(_) => {}
            EventAddResEmpty(_) => {}
            EventCancel(_) => {}
            EventCancelRes(_) => {}
            ReadNotify(_) => {}
            ReadNotifyRes(_) => {}
            ChannelClose(_) => {}
            ChannelCloseRes(_) => {}
            ChannelDisconnect(_) => {}
            Echo => {}
        }
    }
}

macro_rules! convert_scalar_value {
    ($st:ty, $var:ident, $buf:expr) => {{
        type ST = $st;
        const STL: usize = std::mem::size_of::<ST>();
        if $buf.len() < STL {
            return Err(Error::NotEnoughPayload);
        }
        let v = ST::from_be_bytes($buf[..STL].try_into().map_err(|_| Error::BadSlice)?);
        CaDataValue::Scalar(CaDataScalarValue::$var(v))
    }};
}

macro_rules! convert_scalar_enum_value {
    ($st:ty, $buf:expr) => {{
        type ST = $st;
        const STL: usize = std::mem::size_of::<ST>();
        if $buf.len() < STL {
            return Err(Error::NotEnoughPayload);
        }
        let v = ST::from_be_bytes($buf[..STL].try_into().map_err(|_| Error::BadSlice)?);
        CaDataValue::Scalar(CaDataScalarValue::Enum(v))
    }};
}

macro_rules! convert_wave_value {
    ($st:ty, $var:ident, $n:expr, $buf:expr) => {{
        type ST = $st;
        const STL: usize = std::mem::size_of::<ST>();
        let nn = $n.min($buf.len() / STL);
        let mut a = Vec::with_capacity(nn);
        // TODO should optimize?
        let mut bb = &$buf[..];
        for _ in 0..nn {
            let v = ST::from_be_bytes(bb[..STL].try_into().map_err(|_| Error::BadSlice)?);
            bb = &bb[STL..];
            a.push(v);
        }
        CaDataValue::Array(CaDataArrayValue::$var(a))
    }};
}

#[derive(Debug)]
pub struct CaMsg {
    pub ty: CaMsgTy,
    pub ts: Instant,
}

impl CaMsg {
    pub fn from_ty_ts(ty: CaMsgTy, ts: Instant) -> Self {
        Self { ty, ts }
    }

    fn len(&self) -> usize {
        self.ty.len()
    }

    fn place_into(&self, buf: &mut [u8]) {
        if self.ty.payload_len() <= 0x3ff0 && self.ty.data_count() <= 0xffff {
            let pls = self.ty.payload_len() as u16;
            let cnt = self.ty.data_count() as u16;
            let t = self.ty.cmdid().to_be_bytes();
            buf[0] = t[0];
            buf[1] = t[1];
            let t = pls.to_be_bytes();
            buf[2] = t[0];
            buf[3] = t[1];
            let t = self.ty.data_type().to_be_bytes();
            buf[4] = t[0];
            buf[5] = t[1];
            let t = cnt.to_be_bytes();
            buf[6] = t[0];
            buf[7] = t[1];
            let t = self.ty.param1().to_be_bytes();
            buf[8] = t[0];
            buf[9] = t[1];
            buf[10] = t[2];
            buf[11] = t[3];
            let t = self.ty.param2().to_be_bytes();
            buf[12] = t[0];
            buf[13] = t[1];
            buf[14] = t[2];
            buf[15] = t[3];
            self.ty.place_payload_into(&mut buf[16..]);
        } else {
            let pls = self.ty.payload_len();
            let cnt = self.ty.data_count();
            let t = self.ty.cmdid().to_be_bytes();
            buf[0] = t[0];
            buf[1] = t[1];
            buf[2] = 0xff;
            buf[3] = 0xff;
            let t = self.ty.data_type().to_be_bytes();
            buf[4] = t[0];
            buf[5] = t[1];
            buf[6] = 0x00;
            buf[7] = 0x00;
            let t = self.ty.param1().to_be_bytes();
            buf[8] = t[0];
            buf[9] = t[1];
            buf[10] = t[2];
            buf[11] = t[3];
            let t = self.ty.param2().to_be_bytes();
            buf[12] = t[0];
            buf[13] = t[1];
            buf[14] = t[2];
            buf[15] = t[3];
            let t = pls.to_be_bytes();
            buf[16] = t[0];
            buf[17] = t[1];
            buf[18] = t[2];
            buf[19] = t[3];
            let t = cnt.to_be_bytes();
            buf[20] = t[0];
            buf[21] = t[1];
            buf[22] = t[2];
            buf[23] = t[3];
            self.ty.place_payload_into(&mut buf[24..]);
        }
    }

    fn ca_scalar_value(scalar_type: &CaScalarType, buf: &[u8]) -> Result<CaDataValue, Error> {
        let val = match scalar_type {
            CaScalarType::I8 => convert_scalar_value!(i8, I8, buf),
            CaScalarType::I16 => convert_scalar_value!(i16, I16, buf),
            CaScalarType::I32 => convert_scalar_value!(i32, I32, buf),
            CaScalarType::F32 => convert_scalar_value!(f32, F32, buf),
            CaScalarType::F64 => convert_scalar_value!(f64, F64, buf),
            CaScalarType::Enum => convert_scalar_enum_value!(i16, buf),
            CaScalarType::String => {
                // TODO constrain string length to the CA `data_count`.
                let mut ixn = buf.len();
                for (i, &c) in buf.iter().enumerate() {
                    if c == 0 {
                        ixn = i;
                        break;
                    }
                }
                //info!("try to read string from payload len {} ixn {}", buf.len(), ixn);
                let v = String::from_utf8_lossy(&buf[..ixn]);
                CaDataValue::Scalar(CaDataScalarValue::String(v.into()))
            }
        };
        Ok(val)
    }

    fn ca_wave_value(scalar_type: &CaScalarType, n: usize, buf: &[u8]) -> Result<CaDataValue, Error> {
        let val = match scalar_type {
            CaScalarType::I8 => convert_wave_value!(i8, I8, n, buf),
            CaScalarType::I16 => convert_wave_value!(i16, I16, n, buf),
            CaScalarType::I32 => convert_wave_value!(i32, I32, n, buf),
            CaScalarType::F32 => convert_wave_value!(f32, F32, n, buf),
            CaScalarType::F64 => convert_wave_value!(f64, F64, n, buf),
            CaScalarType::String => CaDataValue::Scalar(CaDataScalarValue::String("todo-array-string".into())),
            _ => {
                warn!("TODO conversion array {scalar_type:?}");
                return Err(Error::TodoConversionArray);
            }
        };
        Ok(val)
    }

    pub fn from_proto_infos(
        hi: &HeadInfo,
        payload: &[u8],
        tsnow: Instant,
        array_truncate: usize,
    ) -> Result<Self, Error> {
        let msg = match hi.cmdid {
            0x00 => CaMsg::from_ty_ts(CaMsgTy::VersionRes(hi.data_count() as u16), tsnow),
            0x0b => {
                let mut s = String::new();
                s.extend(format!("{:?}", &payload[..payload.len().min(16)]).chars());
                if payload.len() >= 17 {
                    s.extend("  msg: ".chars());
                    s.extend(String::from_utf8_lossy(&payload[17..payload.len() - 1]).chars());
                }
                let e = ErrorCmd {
                    cid: hi.param1,
                    eid: hi.param2,
                    msg: s,
                };
                CaMsg::from_ty_ts(CaMsgTy::Error(e), tsnow)
            }
            0x06 => {
                if hi.payload_len() != 8 {
                    warn!("protocol error: search result is expected with fixed payload size 8");
                }
                if hi.data_count() != 0 {
                    warn!("protocol error: search result is expected with data count 0");
                }
                if payload.len() < 2 {
                    return Err(Error::CaProtoVersionMissing);
                }
                let proto_version = u16::from_be_bytes(payload[0..2].try_into().map_err(|_| Error::BadSlice)?);
                let ty = CaMsgTy::SearchRes(SearchRes {
                    tcp_port: hi.data_type,
                    addr: hi.param1,
                    id: hi.param2,
                    proto_version,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x01 => {
                if payload.len() < 12 {
                    if payload.len() == 0 {
                        if hi.data_count() != 0 {
                            // TODO according to protocol, this should not happen. Count for metrics.
                        }
                        let ty = CaMsgTy::EventAddResEmpty(EventAddResEmpty {
                            data_type: hi.data_type,
                            sid: hi.param1,
                            subid: hi.param2,
                        });
                        return Ok(CaMsg::from_ty_ts(ty, tsnow));
                    } else {
                        error!("EventAddRes but bad header {hi:?}");
                        return Err(Error::NotEnoughPayloadTimeMetadata(payload.len()));
                    }
                }
                let value = Self::extract_ca_data_value(hi, payload, array_truncate)?;
                let d = EventAddRes {
                    data_type: hi.data_type,
                    data_count: hi.data_count() as _,
                    status: hi.param1,
                    subid: hi.param2,
                    payload_len: hi.payload_len() as u32,
                    value,
                };
                let ty = CaMsgTy::EventAddRes(d);
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x0c => {
                if payload.len() != 0 {
                    return Err(Error::BadPayload);
                }
                let ty = CaMsgTy::ChannelCloseRes(ChannelCloseRes {
                    sid: hi.param1,
                    cid: hi.param2,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x0f => {
                if payload.len() == 8 {
                    let v = u64::from_be_bytes(payload.try_into().map_err(|_| Error::BadSlice)?);
                    debug!("Payload as u64: {v}");
                    let v = i64::from_be_bytes(payload.try_into().map_err(|_| Error::BadSlice)?);
                    debug!("Payload as i64: {v}");
                    let v = f64::from_be_bytes(payload.try_into().map_err(|_| Error::BadSlice)?);
                    debug!("Payload as f64: {v}");
                }
                let value = Self::extract_ca_data_value(hi, payload, array_truncate)?;
                let ty = CaMsgTy::ReadNotifyRes(ReadNotifyRes {
                    data_type: hi.data_type,
                    data_count: hi.data_count() as _,
                    sid: hi.param1,
                    ioid: hi.param2,
                    payload_len: hi.payload_len() as u32,
                    value,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x12 => {
                let ty = CaMsgTy::CreateChanRes(CreateChanRes {
                    data_type: hi.data_type,
                    // TODO what am I supposed to use here in case of extended header?
                    data_count: hi.data_count() as _,
                    cid: hi.param1,
                    sid: hi.param2,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x16 => {
                let ty = CaMsgTy::AccessRightsRes(AccessRightsRes {
                    cid: hi.param1,
                    rights: hi.param2,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x17 => {
                let ty = CaMsgTy::Echo;
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x1a => {
                // TODO use different structs for request and response:
                let ty = CaMsgTy::CreateChanFail(CreateChanFail { cid: hi.param1 });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            0x14 => {
                let name = std::ffi::CString::new(payload)
                    .map(|s| s.into_string().unwrap_or_else(|e| format!("{e:?}")))
                    .unwrap_or_else(|e| format!("{e:?}"));
                CaMsg::from_ty_ts(CaMsgTy::ClientNameRes(ClientNameRes { name }), tsnow)
            }
            // TODO make response type for host name:
            0x15 => CaMsg::from_ty_ts(CaMsgTy::HostName("TODOx5288".into()), tsnow),
            x => return Err(Error::CaCommandNotSupported(x)),
        };
        Ok(msg)
    }

    fn extract_ca_data_value(hi: &HeadInfo, payload: &[u8], array_truncate: usize) -> Result<CaEventValue, Error> {
        use netpod::Shape;
        let ca_dbr_ty = CaDbrType::from_ca_u16(hi.data_type)?;
        let ca_sh = Shape::from_ca_count(hi.data_count() as _).map_err(|_| {
            error!("BadCaCount  {hi:?}");
            Error::BadCaCount
        })?;
        let (meta, data_offset) = match &ca_dbr_ty.meta {
            CaDbrMetaType::Plain => return Err(Error::MismatchDbrTimeType),
            CaDbrMetaType::Status => return Err(Error::MismatchDbrTimeType),
            CaDbrMetaType::Time => {
                let status = u16::from_be_bytes(payload[0..2].try_into().map_err(|_| Error::BadSlice)?);
                let severity = u16::from_be_bytes(payload[2..4].try_into().map_err(|_| Error::BadSlice)?);
                let ca_secs = u32::from_be_bytes(payload[4..8].try_into().map_err(|_| Error::BadSlice)?);
                let ca_nanos = u32::from_be_bytes(payload[8..12].try_into().map_err(|_| Error::BadSlice)?);
                let meta = CaMetaValue::CaMetaTime(CaMetaTime {
                    status,
                    severity,
                    ca_secs,
                    ca_nanos,
                });
                (meta, 12)
            }
            CaDbrMetaType::Ctrl => {
                let status = u16::from_be_bytes(payload[0..2].try_into().map_err(|_| Error::BadSlice)?);
                let severity = u16::from_be_bytes(payload[2..4].try_into().map_err(|_| Error::BadSlice)?);
                let varcnt = u16::from_be_bytes(payload[4..6].try_into().map_err(|_| Error::BadSlice)?);
                if varcnt > 16 {
                    return Err(Error::BadCaCount);
                }
                // let s = String::from_utf8_lossy(&payload[6..6 + 26 * 16]);
                let mut variants = Vec::new();
                for i in 0..varcnt {
                    let p = (6 + 26 * i) as usize;
                    let s1 = std::ffi::CStr::from_bytes_until_nul(&payload[p..p + 26])
                        .map_or(String::from("encodingerror"), |x| {
                            x.to_str().map_or(String::from("encodingerror"), |x| x.to_string())
                        });
                    let s1 = if s1.len() >= 26 {
                        String::from("toolongerror")
                    } else {
                        s1
                    };
                    variants.push(s1);
                }
                // info!("enum variants debug  {varcnt}  {s}  {variants:?}");
                let meta = CaMetaValue::CaMetaVariants(CaMetaVariants {
                    status,
                    severity,
                    variants,
                });
                (meta, 2 + 2 + 2 + 26 * 16)
            }
        };
        let meta_padding = match ca_dbr_ty.meta {
            CaDbrMetaType::Plain => 0,
            CaDbrMetaType::Status => match ca_dbr_ty.scalar_type {
                CaScalarType::I8 => 1,
                CaScalarType::I16 => 0,
                CaScalarType::I32 => 0,
                CaScalarType::F32 => 0,
                CaScalarType::F64 => 4,
                CaScalarType::Enum => 0,
                CaScalarType::String => 0,
            },
            CaDbrMetaType::Time => match ca_dbr_ty.scalar_type {
                CaScalarType::I8 => 3,
                CaScalarType::I16 => 2,
                CaScalarType::I32 => 0,
                CaScalarType::F32 => 0,
                CaScalarType::F64 => 4,
                CaScalarType::Enum => 2,
                CaScalarType::String => 0,
            },
            CaDbrMetaType::Ctrl => match ca_dbr_ty.scalar_type {
                CaScalarType::I8 => 1,
                CaScalarType::I16 => 0,
                CaScalarType::I32 => 0,
                CaScalarType::F32 => 0,
                CaScalarType::F64 => 0,
                CaScalarType::Enum => 0,
                CaScalarType::String => 0,
            },
        };
        let valbuf = &payload[data_offset + meta_padding..];
        let value = match ca_sh {
            Shape::Scalar => Self::ca_scalar_value(&ca_dbr_ty.scalar_type, valbuf)?,
            Shape::Wave(n) => Self::ca_wave_value(&ca_dbr_ty.scalar_type, (n as usize).min(array_truncate), valbuf)?,
            Shape::Image(_, _) => {
                error!("Can not handle image from channel access");
                err::todoval()
            }
        };
        let value = CaEventValue { data: value, meta };
        Ok(value)
    }
}

#[derive(Debug)]
pub enum CaItem {
    Empty,
    Msg(CaMsg),
}

#[derive(Clone, Debug)]
pub struct HeadInfo {
    cmdid: u16,
    payload_size: u32,
    data_type: u16,
    data_count: u32,
    param1: u32,
    param2: u32,
    is_ext: bool,
}

impl HeadInfo {
    pub fn from_netbuf(buf: &mut SlideBuf) -> Result<Self, Error> {
        let command = buf.read_u16_be()?;
        let payload_size = buf.read_u16_be()? as u32;
        let data_type = buf.read_u16_be()?;
        let data_count = buf.read_u16_be()? as u32;
        let param1 = buf.read_u32_be()?;
        let param2 = buf.read_u32_be()?;
        let hi = HeadInfo {
            cmdid: command,
            payload_size,
            data_type,
            data_count,
            param1,
            param2,
            is_ext: false,
        };
        Ok(hi)
    }

    fn with_ext(mut self, payload: u32, datacount: u32) -> Self {
        self.is_ext = true;
        self.payload_size = payload;
        self.data_count = datacount;
        self
    }

    pub fn cmdid(&self) -> u16 {
        self.cmdid
    }

    pub fn payload_len(&self) -> u32 {
        self.payload_size
    }

    pub fn data_count(&self) -> u32 {
        self.data_count
    }

    // only for debug purpose
    pub fn param2(&self) -> u32 {
        self.param2
    }
}

#[derive(Debug)]
enum CaState {
    StdHead,
    ExtHead(HeadInfo),
    Payload(HeadInfo),
    Done,
}

impl CaState {
    fn need_min(&self) -> usize {
        use CaState::*;
        match self {
            StdHead => 16,
            ExtHead(_) => 8,
            Payload(k) => k.payload_len() as usize,
            Done => 123,
        }
    }
}

pub trait AsyncWriteRead: AsyncWrite + AsyncRead + Send + 'static {}

impl<T> AsyncWriteRead for T where T: AsyncWrite + AsyncRead + Send + 'static {}

pub struct CaProto {
    tcp: Pin<Box<dyn AsyncWriteRead>>,
    tcp_eof: bool,
    remote_name: String,
    state: CaState,
    buf: SlideBuf,
    outbuf: SlideBuf,
    out: VecDeque<CaMsg>,
    array_truncate: usize,
    stats: Arc<CaProtoStats>,
    resqu: VecDeque<CaItem>,
    event_add_res_cnt: u32,
    bytes_recv_testing: u32,
}

impl CaProto {
    pub fn new<T: AsyncWriteRead>(
        tcp: T,
        remote_name: String,
        array_truncate: usize,
        stats: Arc<CaProtoStats>,
    ) -> Self {
        Self {
            tcp: Box::pin(tcp),
            tcp_eof: false,
            remote_name,
            state: CaState::StdHead,
            buf: SlideBuf::new(PROTO_INPUT_BUF_CAP as usize),
            outbuf: SlideBuf::new(1024 * 256),
            out: VecDeque::new(),
            array_truncate,
            stats,
            resqu: VecDeque::with_capacity(256),
            event_add_res_cnt: 0,
            bytes_recv_testing: 0,
        }
    }

    pub fn proto_out_len(&self) -> usize {
        self.out.len()
    }

    pub fn push_out(&mut self, item: CaMsg) {
        self.out.push_back(item);
    }

    fn out_msg_buf(&mut self) -> Option<(&CaMsg, &mut [u8])> {
        if let Some(item) = self.out.front() {
            match self.outbuf.available_writable_area(item.len()) {
                Ok(buf) => Some((item, buf)),
                Err(_) => {
                    // TODO is this the correct behavior?
                    None
                }
            }
        } else {
            None
        }
    }

    fn attempt_output(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<usize, Error>> {
        use Poll::*;
        let this = self.as_mut().get_mut();
        let w = &mut this.tcp;
        let b = this.outbuf.data();
        let w = Pin::new(w);
        match w.poll_write(cx, b) {
            Ready(k) => match k {
                Ok(k) => match self.outbuf.adv(k) {
                    Ok(()) => {
                        self.stats.out_bytes().add(k as u64);
                        Ready(Ok(k))
                    }
                    Err(e) => {
                        error!("advance error {:?}", e);
                        Ready(Err(e.into()))
                    }
                },
                Err(e) => {
                    error!("output write error {:?}", e);
                    Ready(Err(e.into()))
                }
            },
            Pending => Pending,
        }
    }

    fn loop_body(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        use Poll::*;
        let mut have_pending = false;
        let mut have_progress = false;
        let tsnow = Instant::now();
        {
            let g = self.outbuf.len();
            self.stats.outbuf_len().ingest(g as u32);
        }
        while let Some((msg, buf)) = self.out_msg_buf() {
            let msglen = msg.len();
            if msglen > buf.len() {
                break;
            }
            msg.place_into(&mut buf[..msglen]);
            self.outbuf.wadv(msglen)?;
            self.out.pop_front();
            self.stats.out_msg_placed().inc();
        }
        while self.outbuf.len() != 0 {
            match Self::attempt_output(self.as_mut(), cx)? {
                Ready(n) => {
                    if n == 0 {
                        let e = Error::LogicError;
                        return Err(e);
                    }
                    have_progress = true;
                }
                Pending => {
                    have_pending = true;
                    break;
                }
            }
        }
        let need_min = self.state.need_min();
        {
            let cap = self.buf.cap();
            if cap < need_min {
                let e = Error::BufferTooSmallForNeedMin(cap, need_min);
                warn!("{e}");
                return Err(e);
            }
        }
        loop {
            if self.tcp_eof {
                break;
            }
            let this = self.as_mut().get_mut();
            let tcp = Pin::new(&mut this.tcp);
            let buf = this.buf.available_writable_area(need_min)?;
            let mut rbuf = ReadBuf::new(buf);
            if rbuf.remaining() == 0 {
                return Err(Error::NoReadBufferSpace);
            }
            break match tcp.poll_read(cx, &mut rbuf) {
                Ready(k) => match k {
                    Ok(()) => {
                        let nf = rbuf.filled().len();
                        if nf == 0 {
                            debug!("peer done  {:?}  {:?}", self.remote_name, self.state);
                            self.tcp_eof = true;
                        } else {
                            if false {
                                debug!("received {} bytes", rbuf.filled().len());
                                let t = rbuf.filled().len().min(32);
                                debug!("received data  {:?}", &rbuf.filled()[0..t]);
                            }
                            if TESTING_PROTOCOL_ERROR_TODO_REMOVE {
                                self.bytes_recv_testing = self.bytes_recv_testing.saturating_add(nf as u32);
                                if self.bytes_recv_testing <= TESTING_PROTOCOL_ERROR_AFTER_BYTES {
                                    self.buf.wadv(nf)?;
                                } else {
                                    let nr =
                                        (self.bytes_recv_testing - TESTING_PROTOCOL_ERROR_AFTER_BYTES).min(nf as u32);
                                    self.buf.wadv(nf - nr as usize)?;
                                    for _ in 0..nr {
                                        self.buf.put_u8(0x55)?;
                                    }
                                }
                            } else {
                                self.buf.wadv(nf)?;
                            }
                            have_progress = true;
                            self.stats.tcp_recv_bytes().add(nf as _);
                            self.stats.tcp_recv_count().inc();
                            continue;
                        }
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                },
                Pending => {
                    have_pending = true;
                }
            };
        }
        while self.resqu.len() < self.resqu.capacity() {
            if self.buf.len() >= self.state.need_min() {
                if let Some(item) = self.parse_item(tsnow)? {
                    self.resqu.push_back(item);
                } else {
                    // Nothing to do
                }
                have_progress = true;
            } else {
                break;
            }
        }
        if have_progress {
            Ok(Ready(()))
        } else if have_pending {
            Ok(Pending)
        } else {
            if self.tcp_eof {
                self.state = CaState::Done;
                Ok(Ready(()))
            } else {
                Err(Error::NeitherPendingNorProgress)
            }
        }
    }

    fn parse_item(&mut self, tsnow: Instant) -> Result<Option<CaItem>, Error> {
        match &self.state {
            CaState::StdHead => {
                let hi = HeadInfo::from_netbuf(&mut self.buf)?;
                if hi.cmdid > 26 {
                    // TODO count as logic error
                    self.stats.protocol_issue().inc();
                }
                if hi.payload_size == 0xffff {
                    self.state = CaState::ExtHead(hi);
                    Ok(None)
                } else {
                    self.stats.payload_size().ingest(hi.payload_len() as u32);
                    if hi.payload_size == 0 {
                        self.state = CaState::StdHead;
                        let msg = CaMsg::from_proto_infos(&hi, &[], tsnow, self.array_truncate)?;
                        Ok(Some(CaItem::Msg(msg)))
                    } else {
                        self.state = CaState::Payload(hi);
                        Ok(None)
                    }
                }
            }
            CaState::ExtHead(hi) => {
                let payload_size = self.buf.read_u32_be()?;
                let data_count = self.buf.read_u32_be()?;
                self.stats.payload_size().ingest(hi.payload_len() as u32);
                if payload_size > PAYLOAD_LEN_MAX {
                    self.stats.payload_ext_very_large().inc();
                    if false {
                        warn!(
                            "ExtHead  data_type {}  payload_size {payload_size}  data_count {data_count}",
                            hi.data_type
                        );
                    }
                }
                if payload_size <= 0x3ff0 {
                    // NOTE can happen even with zero payload, just because data-count exceeds u16.
                    self.stats.payload_ext_but_small().inc();
                    if false {
                        warn!(
                            "ExtHead  data_type {}  payload_size {payload_size}  data_count {data_count}",
                            hi.data_type
                        );
                    }
                }
                let hi = hi.clone().with_ext(payload_size, data_count);
                self.state = CaState::Payload(hi);
                Ok(None)
            }
            CaState::Payload(hi) => {
                let g = self.buf.read_bytes(hi.payload_len() as usize)?;
                let msg = CaMsg::from_proto_infos(hi, g, tsnow, self.array_truncate)?;
                // data-count is only reasonable for event messages
                let ret = match &msg.ty {
                    CaMsgTy::EventAddRes(..) => {
                        self.stats.data_count().ingest(hi.data_count() as u32);
                        if TESTING_UNRESPONSIVE_TODO_REMOVE {
                            if self.event_add_res_cnt < TESTING_EVENT_ADD_RES_MAX {
                                self.event_add_res_cnt += 1;
                                Ok(Some(CaItem::Msg(msg)))
                            } else {
                                Ok(None)
                            }
                        } else {
                            self.event_add_res_cnt += 1;
                            Ok(Some(CaItem::Msg(msg)))
                        }
                    }
                    _ => Ok(Some(CaItem::Msg(msg))),
                };
                self.state = CaState::StdHead;
                ret
            }
            CaState::Done => Err(Error::ParseAttemptInDoneState),
        }
    }
}

impl Stream for CaProto {
    type Item = Result<CaItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(item) = self.resqu.pop_front() {
                Ready(Some(Ok(item)))
            } else if let CaState::Done = self.state {
                Ready(None)
            } else {
                let k = Self::loop_body(self.as_mut(), cx);
                match k {
                    Ok(Ready(())) => continue,
                    Ok(Pending) => Pending,
                    Err(e) => {
                        self.state = CaState::Done;
                        Ready(Some(Err(e)))
                    }
                }
            };
        }
    }
}
