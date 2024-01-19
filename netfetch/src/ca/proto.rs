use crate::netbuf;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use log::*;
use netpod::timeunits::*;
use slidebuf::SlideBuf;
use stats::CaProtoStats;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddrV4;
use std::num::NonZeroU16;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use taskrun::tokio;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;

#[derive(Debug, ThisError)]
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
}

const CA_PROTO_VERSION: u16 = 13;
const EPICS_EPOCH_OFFSET: u64 = 631152000;

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
    pub data_count: u16,
    pub sid: u32,
    pub subid: u32,
}

#[derive(Debug)]
pub struct EventCancel {
    pub data_type: u16,
    pub data_count: u16,
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
    pub data_count: u16,
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
}

#[derive(Debug)]
pub struct CaDbrType {
    meta: CaDbrMetaType,
    scalar_type: CaScalarType,
}

impl CaDbrType {
    pub fn from_ca_u16(k: u16) -> Result<Self, Error> {
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

#[derive(Clone, Debug)]
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

impl From<CaDataScalarValue> for scywr::iteminsertqueue::ScalarValue {
    fn from(val: CaDataScalarValue) -> Self {
        use scywr::iteminsertqueue::ScalarValue;
        match val {
            CaDataScalarValue::I8(x) => ScalarValue::I8(x),
            CaDataScalarValue::I16(x) => ScalarValue::I16(x),
            CaDataScalarValue::I32(x) => ScalarValue::I32(x),
            CaDataScalarValue::F32(x) => ScalarValue::F32(x),
            CaDataScalarValue::F64(x) => ScalarValue::F64(x),
            CaDataScalarValue::Enum(x) => ScalarValue::Enum(x),
            CaDataScalarValue::String(x) => ScalarValue::String(x),
            CaDataScalarValue::Bool(x) => ScalarValue::Bool(x),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CaDataArrayValue {
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    // TODO remove, CA has no bool, make new enum for other use cases.
    Bool(Vec<bool>),
}

impl From<CaDataArrayValue> for scywr::iteminsertqueue::ArrayValue {
    fn from(val: CaDataArrayValue) -> Self {
        use scywr::iteminsertqueue::ArrayValue;
        match val {
            CaDataArrayValue::I8(x) => ArrayValue::I8(x),
            CaDataArrayValue::I16(x) => ArrayValue::I16(x),
            CaDataArrayValue::I32(x) => ArrayValue::I32(x),
            CaDataArrayValue::F32(x) => ArrayValue::F32(x),
            CaDataArrayValue::F64(x) => ArrayValue::F64(x),
            CaDataArrayValue::Bool(x) => ArrayValue::Bool(x),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CaDataValue {
    Scalar(CaDataScalarValue),
    Array(CaDataArrayValue),
}

impl From<CaDataValue> for scywr::iteminsertqueue::DataValue {
    fn from(value: CaDataValue) -> Self {
        use scywr::iteminsertqueue::DataValue;
        match value {
            CaDataValue::Scalar(x) => DataValue::Scalar(x.into()),
            CaDataValue::Array(x) => DataValue::Array(x.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CaEventValue {
    pub ts: u64,
    pub status: u16,
    pub severity: u16,
    pub data: CaDataValue,
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
            Echo => 0x17,
        }
    }

    fn len(&self) -> usize {
        16 + self.payload_len()
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
            Echo => 0,
        }
    }

    fn data_count(&self) -> u16 {
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
            CreateChanRes(x) => {
                panic!();
                x.data_count as _
            }
            CreateChanFail(_) => 0,
            AccessRightsRes(_) => 0,
            EventAdd(x) => x.data_count,
            EventAddRes(x) => {
                panic!();
                x.data_count as _
            }
            EventAddResEmpty(_) => 0,
            EventCancel(x) => x.data_count,
            EventCancelRes(x) => 0,
            ReadNotify(x) => x.data_count,
            ReadNotifyRes(x) => {
                panic!();
                x.data_count as _
            }
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
                // TODO allow to customize the mask. Test if it works.
                buf.copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0e, 0, 0]);
            }
            EventAddRes(_) => {}
            EventAddResEmpty(_) => {}
            EventCancel(_) => {}
            EventCancelRes(_) => {}
            ReadNotify(_) => {}
            ReadNotifyRes(_) => {}
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
        //info!("place_into  given {} bytes buffer", buf.len());
        if self.ty.payload_len() > 0x4000 - 16 {
            error!("TODO emit for larger payloads");
            panic!();
        } else {
            let t = self.ty.cmdid().to_be_bytes();
            buf[0] = t[0];
            buf[1] = t[1];
            let t = (self.ty.payload_len() as u16).to_be_bytes();
            buf[2] = t[0];
            buf[3] = t[1];
            let t = self.ty.data_type().to_be_bytes();
            buf[4] = t[0];
            buf[5] = t[1];
            let t = self.ty.data_count().to_be_bytes();
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
        }
    }

    fn ca_scalar_value(scalar_type: &CaScalarType, buf: &[u8]) -> Result<CaDataValue, Error> {
        let val = match scalar_type {
            CaScalarType::I8 => convert_scalar_value!(i8, I8, buf),
            CaScalarType::I16 => convert_scalar_value!(i16, I16, buf),
            CaScalarType::I32 => convert_scalar_value!(i32, I32, buf),
            CaScalarType::F32 => convert_scalar_value!(f32, F32, buf),
            CaScalarType::F64 => convert_scalar_value!(f64, F64, buf),
            CaScalarType::Enum => convert_scalar_value!(i16, I16, buf),
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
            0x00 => CaMsg::from_ty_ts(CaMsgTy::VersionRes(hi.data_count), tsnow),
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
            20 => {
                let name = std::ffi::CString::new(payload)
                    .map(|s| s.into_string().unwrap_or_else(|e| format!("{e:?}")))
                    .unwrap_or_else(|e| format!("{e:?}"));
                CaMsg::from_ty_ts(CaMsgTy::ClientNameRes(ClientNameRes { name }), tsnow)
            }
            // TODO make response type for host name:
            21 => CaMsg::from_ty_ts(CaMsgTy::HostName("TODOx5288".into()), tsnow),
            6 => {
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
            18 => {
                let ty = CaMsgTy::CreateChanRes(CreateChanRes {
                    data_type: hi.data_type,
                    // TODO what am I supposed to use here in case of extended header?
                    data_count: hi.data_count() as _,
                    cid: hi.param1,
                    sid: hi.param2,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            22 => {
                // TODO use different structs for request and response:
                let ty = CaMsgTy::AccessRightsRes(AccessRightsRes {
                    cid: hi.param1,
                    rights: hi.param2,
                });
                CaMsg::from_ty_ts(ty, tsnow)
            }
            26 => {
                // TODO use different structs for request and response:
                let ty = CaMsgTy::CreateChanFail(CreateChanFail { cid: hi.param1 });
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
            0x11 => CaMsg::from_ty_ts(CaMsgTy::Echo, tsnow),
            x => return Err(Error::CaCommandNotSupported(x)),
        };
        Ok(msg)
    }

    fn extract_ca_data_value(hi: &HeadInfo, payload: &[u8], array_truncate: usize) -> Result<CaEventValue, Error> {
        use netpod::Shape;
        let ca_dbr_ty = CaDbrType::from_ca_u16(hi.data_type)?;
        if let CaDbrMetaType::Time = ca_dbr_ty.meta {
        } else {
            return Err(Error::MismatchDbrTimeType);
        }
        let ca_status = u16::from_be_bytes(payload[0..2].try_into().map_err(|_| Error::BadSlice)?);
        let ca_severity = u16::from_be_bytes(payload[2..4].try_into().map_err(|_| Error::BadSlice)?);
        let ca_secs = u32::from_be_bytes(payload[4..8].try_into().map_err(|_| Error::BadSlice)?);
        let ca_nanos = u32::from_be_bytes(payload[8..12].try_into().map_err(|_| Error::BadSlice)?);
        let ca_sh = Shape::from_ca_count(hi.data_count() as _).map_err(|_| {
            error!("BadCaCount  {hi:?}");
            Error::BadCaCount
        })?;
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
        };
        let valbuf = &payload[12 + meta_padding..];
        let value = match ca_sh {
            Shape::Scalar => Self::ca_scalar_value(&ca_dbr_ty.scalar_type, valbuf)?,
            Shape::Wave(n) => Self::ca_wave_value(&ca_dbr_ty.scalar_type, (n as usize).min(array_truncate), valbuf)?,
            Shape::Image(_, _) => {
                error!("Can not handle image from channel access");
                err::todoval()
            }
        };
        let ts = SEC * (ca_secs as u64 + EPICS_EPOCH_OFFSET) + ca_nanos as u64;
        let value = CaEventValue {
            ts,
            status: ca_status,
            severity: ca_severity,
            data: value,
        };
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
    payload_size: u16,
    data_type: u16,
    data_count: u16,
    param1: u32,
    param2: u32,
    ext_payload_size: u32,
    ext_data_count: u32,
}

impl HeadInfo {
    pub fn from_netbuf(buf: &mut SlideBuf) -> Result<Self, Error> {
        let command = buf.read_u16_be()?;
        let payload_size = buf.read_u16_be()?;
        let data_type = buf.read_u16_be()?;
        let data_count = buf.read_u16_be()?;
        let param1 = buf.read_u32_be()?;
        let param2 = buf.read_u32_be()?;
        let hi = HeadInfo {
            cmdid: command,
            payload_size,
            data_type,
            data_count,
            param1,
            param2,
            ext_payload_size: 0,
            ext_data_count: 0,
        };
        Ok(hi)
    }

    fn with_ext(mut self, payload: u32, datacount: u32) -> Self {
        self.ext_payload_size = payload;
        self.ext_data_count = datacount;
        self
    }

    pub fn cmdid(&self) -> u16 {
        self.cmdid
    }

    pub fn payload_len(&self) -> usize {
        if self.payload_size == 0xffff {
            self.ext_payload_size as _
        } else {
            self.payload_size as _
        }
    }

    pub fn data_count(&self) -> usize {
        if self.payload_size == 0xffff {
            self.ext_data_count as _
        } else {
            self.data_count as _
        }
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
            Payload(k) => k.payload_len(),
            Done => 123,
        }
    }
}

pub struct CaProto {
    tcp: TcpStream,
    remote_addr_dbg: SocketAddrV4,
    state: CaState,
    buf: SlideBuf,
    outbuf: SlideBuf,
    out: VecDeque<CaMsg>,
    array_truncate: usize,
    logged_proto_error_for_cid: HashMap<u32, bool>,
    stats: Arc<CaProtoStats>,
    resqu: VecDeque<CaItem>,
}

impl CaProto {
    pub fn new(tcp: TcpStream, remote_addr_dbg: SocketAddrV4, array_truncate: usize, stats: Arc<CaProtoStats>) -> Self {
        Self {
            tcp,
            remote_addr_dbg,
            state: CaState::StdHead,
            buf: SlideBuf::new(1024 * 1024 * 8),
            outbuf: SlideBuf::new(1024 * 128),
            out: VecDeque::new(),
            array_truncate,
            logged_proto_error_for_cid: HashMap::new(),
            stats,
            resqu: VecDeque::with_capacity(256),
        }
    }

    pub fn proto_out_len(&self) -> usize {
        self.out.len()
    }

    pub fn push_out(&mut self, item: CaMsg) {
        self.out.push_back(item);
    }

    fn inpbuf_conn(&mut self, need_min: usize) -> Result<(&mut TcpStream, ReadBuf), Error> {
        let buf = self.buf.available_writable_area(need_min)?;
        let buf = ReadBuf::new(buf);
        Ok((&mut self.tcp, buf))
    }

    fn outbuf_conn(&mut self) -> (&mut TcpStream, &[u8]) {
        (&mut self.tcp, self.outbuf.data())
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
        let (w, b) = self.outbuf_conn();
        let w = Pin::new(w);
        match w.poll_write(cx, b) {
            Ready(k) => match k {
                Ok(k) => match self.outbuf.adv(k) {
                    Ok(()) => Ready(Ok(k)),
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
        'l1: while self.out.len() != 0 {
            while let Some((msg, buf)) = self.out_msg_buf() {
                let msglen = msg.len();
                if msglen > buf.len() {
                    error!("got output buffer but too small");
                    break;
                } else {
                    msg.place_into(&mut buf[..msglen]);
                    self.outbuf.wadv(msglen)?;
                    self.out.pop_front();
                }
            }
            while self.outbuf.len() != 0 {
                match Self::attempt_output(self.as_mut(), cx)? {
                    Ready(n) => {
                        if n != 0 {
                            have_progress = true;
                        } else {
                            // Should not occur to begin with. TODO restructure.
                            break 'l1;
                        }
                    }
                    Pending => {
                        have_pending = true;
                        break 'l1;
                    }
                }
            }
        }
        'l1: while self.outbuf.len() != 0 {
            match Self::attempt_output(self.as_mut(), cx)? {
                Ready(n) => {
                    if n != 0 {
                        have_progress = true;
                    } else {
                        // Should not occur to begin with. TODO restructure.
                        break 'l1;
                    }
                }
                Pending => {
                    have_pending = true;
                    break 'l1;
                }
            }
        }
        let need_min = self.state.need_min();
        if self.buf.cap() < need_min {
            self.state = CaState::Done;
            let e = Error::BufferTooSmallForNeedMin(self.buf.cap(), self.state.need_min());
            return Err(e);
        }
        if self.buf.len() < need_min {
            let (w, mut rbuf) = self.inpbuf_conn(need_min)?;
            if rbuf.remaining() == 0 {
                return Err(Error::NoReadBufferSpace);
            }
            let w = Pin::new(w);
            match w.poll_read(cx, &mut rbuf) {
                Ready(k) => match k {
                    Ok(()) => {
                        let nf = rbuf.filled().len();
                        if nf == 0 {
                            info!(
                                "EOF  peer  {:?}  {:?}  {:?}",
                                self.tcp.peer_addr(),
                                self.remote_addr_dbg,
                                self.state
                            );
                            // TODO may need another state, if not yet done when input is EOF.
                            self.state = CaState::Done;
                        } else {
                            if false {
                                info!("received {} bytes", rbuf.filled().len());
                                let t = rbuf.filled().len().min(32);
                                info!("received data  {:?}", &rbuf.filled()[0..t]);
                            }
                            match self.buf.wadv(nf) {
                                Ok(()) => {
                                    have_progress = true;
                                    self.stats.tcp_recv_bytes().add(nf as _);
                                    self.stats.tcp_recv_count().inc();
                                }
                                Err(e) => {
                                    error!("netbuf wadv fail  nf {nf}");
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                },
                Pending => {
                    have_pending = true;
                }
            }
        }
        while self.resqu.len() < self.resqu.capacity() {
            if let Some(item) = self.parse_item(tsnow)? {
                have_progress = true;
                self.resqu.push_back(item);
            } else {
                break;
            }
        }
        if have_progress {
            Ok(Ready(()))
        } else if have_pending {
            Ok(Pending)
        } else {
            Err(Error::NeitherPendingNorProgress)
        }
    }

    fn parse_item(&mut self, tsnow: Instant) -> Result<Option<CaItem>, Error> {
        if self.buf.len() < self.state.need_min() {
            return Ok(None);
        }
        match &self.state {
            CaState::StdHead => {
                let hi = HeadInfo::from_netbuf(&mut self.buf)?;
                if hi.cmdid == 1 || hi.cmdid == 15 {
                    let sid = hi.param1;
                    if hi.payload_size == 0xffff {
                        if hi.data_count != 0 {
                            warn!("protocol error: {hi:?}");
                            return Err(Error::ExtendedHeaderBadCount);
                        }
                    }
                    if hi.payload_size == 0xffff {
                    } else if hi.payload_size > 16368 {
                        self.stats.payload_std_too_large().inc();
                    }
                }
                if hi.cmdid > 26 {
                    // TODO count as logic error
                    self.stats.protocol_issue().inc();
                }
                if hi.payload_size == 0xffff {
                    self.state = CaState::ExtHead(hi);
                    Ok(None)
                } else {
                    // For extended messages, ingest on receive of extended header
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
                if payload_size > 1024 * 1024 * 32 {
                    self.stats.payload_ext_very_large().inc();
                    if false {
                        warn!(
                            "ExtHead  data_type {}  payload_size {payload_size}  data_count {data_count}",
                            hi.data_type
                        );
                    }
                }
                if payload_size <= 16368 {
                    self.stats.payload_ext_but_small().inc();
                    warn!(
                        "ExtHead  data_type {}  payload_size {payload_size}  data_count {data_count}",
                        hi.data_type
                    );
                }
                let hi = hi.clone().with_ext(payload_size, data_count);
                self.state = CaState::Payload(hi);
                Ok(None)
            }
            CaState::Payload(hi) => {
                let g = self.buf.read_bytes(hi.payload_len())?;
                let msg = CaMsg::from_proto_infos(hi, g, tsnow, self.array_truncate)?;
                // data-count is only reasonable for event messages
                if let CaMsgTy::EventAddRes(e) = &msg.ty {
                    self.stats.data_count().ingest(hi.data_count() as u32);
                }
                self.state = CaState::StdHead;
                Ok(Some(CaItem::Msg(msg)))
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
                    Err(e) => Ready(Some(Err(e))),
                }
            };
        }
    }
}
