use crate::netbuf::NetBuf;
use err::Error;
use futures_util::{pin_mut, Stream};
use log::*;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

const CA_PROTO_VERSION: u16 = 13;

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
    pub data_count: u16,
    pub cid: u32,
    pub sid: u32,
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
pub struct EventAddRes {
    pub data_type: u16,
    pub data_count: u16,
    pub status: u32,
    pub subid: u32,
    pub value: CaDataValue,
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
    pub data_count: u16,
    pub sid: u32,
    pub ioid: u32,
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

#[derive(Clone, Debug)]
pub enum CaDataScalarValue {
    I8(i8),
    I16(i16),
    I32(i32),
    F32(f32),
    F64(f64),
    Enum(i16),
    String(String),
}

#[derive(Clone, Debug)]
pub enum CaDataValue {
    Scalar(CaDataScalarValue),
    Array,
}

impl CaScalarType {
    fn from_ca_u16(k: u16) -> Result<Self, Error> {
        use CaScalarType::*;
        let ret = match k {
            4 => I8,
            1 => I16,
            5 => I32,
            2 => F32,
            6 => F64,
            3 => Enum,
            0 => String,
            k => return Err(Error::with_msg_no_trace(format!("bad dbr type id: {k}"))),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
pub enum CaMsgTy {
    Version,
    VersionRes(u16),
    ClientName,
    ClientNameRes(ClientNameRes),
    HostName,
    Search(Search),
    SearchRes(SearchRes),
    CreateChan(CreateChan),
    CreateChanRes(CreateChanRes),
    AccessRightsRes(AccessRightsRes),
    EventAdd(EventAdd),
    EventAddRes(EventAddRes),
    ReadNotify(ReadNotify),
    ReadNotifyRes(ReadNotifyRes),
}

impl CaMsgTy {
    fn cmdid(&self) -> u16 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            ClientName => 20,
            ClientNameRes(_) => 20,
            HostName => 21,
            Search(_) => 6,
            SearchRes(_) => 6,
            CreateChan(_) => 18,
            CreateChanRes(_) => 18,
            AccessRightsRes(_) => 22,
            EventAdd(_) => 1,
            EventAddRes(_) => 1,
            ReadNotify(_) => 15,
            ReadNotifyRes(_) => 15,
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
            ClientName => 8,
            ClientNameRes(x) => (x.name.len() + 1 + 7) / 8 * 8,
            HostName => 8,
            Search(x) => (x.channel.len() + 1 + 7) / 8 * 8,
            SearchRes(_) => 8,
            CreateChan(x) => (x.channel.len() + 1 + 7) / 8 * 8,
            CreateChanRes(_) => 0,
            AccessRightsRes(_) => 0,
            EventAdd(_) => 16,
            EventAddRes(_) => {
                error!("should not attempt to serialize the response again");
                panic!();
            }
            ReadNotify(_) => 0,
            ReadNotifyRes(_) => {
                error!("should not attempt to serialize the response again");
                panic!();
            }
        }
    }

    fn data_type(&self) -> u16 {
        use CaMsgTy::*;
        match self {
            Version => CA_PROTO_VERSION,
            VersionRes(n) => *n,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(_) => {
                // Reply-flag
                1
            }
            SearchRes(x) => x.tcp_port,
            CreateChan(_) => 0,
            CreateChanRes(x) => x.data_type,
            AccessRightsRes(_) => 0,
            EventAdd(x) => x.data_type,
            EventAddRes(x) => x.data_type,
            ReadNotify(x) => x.data_type,
            ReadNotifyRes(x) => x.data_type,
        }
    }

    fn data_count(&self) -> u16 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(_) => CA_PROTO_VERSION,
            SearchRes(_) => 0,
            CreateChan(_) => 0,
            CreateChanRes(x) => x.data_count,
            AccessRightsRes(_) => 0,
            EventAdd(x) => x.data_count,
            EventAddRes(x) => x.data_count,
            ReadNotify(x) => x.data_count,
            ReadNotifyRes(x) => x.data_count,
        }
    }

    fn param1(&self) -> u32 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(e) => e.id,
            SearchRes(x) => x.addr,
            CreateChan(x) => x.cid,
            CreateChanRes(x) => x.cid,
            AccessRightsRes(x) => x.cid,
            EventAdd(x) => x.sid,
            EventAddRes(x) => x.status,
            ReadNotify(x) => x.sid,
            ReadNotifyRes(x) => x.sid,
        }
    }

    fn param2(&self) -> u32 {
        use CaMsgTy::*;
        match self {
            Version => 0,
            VersionRes(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(e) => e.id,
            SearchRes(x) => x.id,
            CreateChan(_) => CA_PROTO_VERSION as _,
            CreateChanRes(x) => x.sid,
            AccessRightsRes(x) => x.rights,
            EventAdd(x) => x.subid,
            EventAddRes(x) => x.subid,
            ReadNotify(x) => x.ioid,
            ReadNotifyRes(x) => x.ioid,
        }
    }

    fn place_payload_into(&self, buf: &mut [u8]) {
        use CaMsgTy::*;
        match self {
            Version => {}
            VersionRes(_) => {}
            ClientName => {
                // TODO allow variable client name. Null-extend always to 8 byte align.
                buf.copy_from_slice(b"SA10\0\0\0\0");
            }
            ClientNameRes(_) => {
                error!("should not attempt to write ClientNameRes");
                panic!();
            }
            HostName => {
                // TODO allow variable host name. Null-extend always to 8 byte align.
                buf.copy_from_slice(b"SA10\0\0\0\0");
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
                unsafe { std::ptr::copy(&d[0] as _, &mut buf[0] as _, d.len()) };
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
                unsafe { std::ptr::copy(&d[0] as _, &mut buf[0] as _, d.len()) };
            }
            CreateChanRes(_) => {}
            AccessRightsRes(_) => {}
            EventAdd(_) => {
                // TODO allow to customize the mask. Test if it works.
                buf.copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 0, 0]);
            }
            EventAddRes(_) => {}
            ReadNotify(_) => {}
            ReadNotifyRes(_) => {}
        }
    }
}

#[derive(Debug)]
pub struct CaMsg {
    pub ty: CaMsgTy,
}

impl CaMsg {
    fn len(&self) -> usize {
        self.ty.len()
    }

    fn place_into(&self, buf: &mut [u8]) {
        info!("place_into  given {} bytes buffer", buf.len());
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

    pub fn from_proto_infos(hi: &HeadInfo, payload: &[u8]) -> Result<Self, Error> {
        let msg = match hi.cmdid {
            0 => CaMsg {
                ty: CaMsgTy::VersionRes(hi.data_count),
            },
            20 => {
                let name = std::ffi::CString::new(payload)
                    .map(|s| s.into_string().unwrap_or_else(|e| format!("{e:?}")))
                    .unwrap_or_else(|e| format!("{e:?}"));
                CaMsg {
                    ty: CaMsgTy::ClientNameRes(ClientNameRes { name }),
                }
            }
            // TODO make response type for host name:
            21 => CaMsg { ty: CaMsgTy::HostName },
            6 => {
                if hi.payload_size != 8 {
                    warn!("protocol error: search result is expected with fixed payload size 8");
                }
                if hi.data_count != 0 {
                    warn!("protocol error: search result is expected with data count 0");
                }
                if payload.len() < 2 {
                    return Err(Error::with_msg_no_trace("server did not include protocol version"));
                }
                let proto_version = u16::from_be_bytes(payload[0..2].try_into()?);
                CaMsg {
                    ty: CaMsgTy::SearchRes(SearchRes {
                        tcp_port: hi.data_type,
                        addr: hi.param1,
                        id: hi.param2,
                        proto_version,
                    }),
                }
            }
            18 => {
                CaMsg {
                    // TODO use different structs for request and response:
                    ty: CaMsgTy::CreateChanRes(CreateChanRes {
                        data_type: hi.data_type,
                        data_count: hi.data_count,
                        cid: hi.param1,
                        sid: hi.param2,
                    }),
                }
            }
            22 => {
                CaMsg {
                    // TODO use different structs for request and response:
                    ty: CaMsgTy::AccessRightsRes(AccessRightsRes {
                        cid: hi.param1,
                        rights: hi.param2,
                    }),
                }
            }
            1 => {
                let ca_st = CaScalarType::from_ca_u16(hi.data_type)?;
                let value = match ca_st {
                    CaScalarType::F64 => {
                        if payload.len() < 2 {
                            return Err(Error::with_msg_no_trace(format!(
                                "not enough payload for enum {}",
                                payload.len()
                            )));
                        }
                        let v = f64::from_be_bytes(payload.try_into()?);
                        CaDataValue::Scalar(CaDataScalarValue::F64(v))
                    }
                    CaScalarType::Enum => {
                        if payload.len() < 2 {
                            return Err(Error::with_msg_no_trace(format!(
                                "not enough payload for enum {}",
                                payload.len()
                            )));
                        }
                        let v = i16::from_be_bytes(payload[..2].try_into()?);
                        CaDataValue::Scalar(CaDataScalarValue::I16(v))
                    }
                    CaScalarType::String => {
                        let mut ixn = payload.len();
                        for (i, &c) in payload.iter().enumerate() {
                            if c == 0 {
                                ixn = i;
                                break;
                            }
                        }
                        //info!("try to read string from payload len {} ixn {}", payload.len(), ixn);
                        let v = String::from_utf8_lossy(&payload[..ixn]);
                        CaDataValue::Scalar(CaDataScalarValue::String(v.into()))
                    }
                    _ => {
                        warn!("TODO handle {ca_st:?}");
                        return Err(Error::with_msg_no_trace(format!("can not yet handle type {ca_st:?}")));
                    }
                };
                let d = EventAddRes {
                    data_type: hi.data_type,
                    data_count: hi.data_count,
                    status: hi.param1,
                    subid: hi.param2,
                    value,
                };
                CaMsg {
                    ty: CaMsgTy::EventAddRes(d),
                }
            }
            15 => {
                if payload.len() == 8 {
                    let v = u64::from_be_bytes(payload.try_into()?);
                    info!("Payload as u64: {v}");
                    let v = i64::from_be_bytes(payload.try_into()?);
                    info!("Payload as i64: {v}");
                    let v = f64::from_be_bytes(payload.try_into()?);
                    info!("Payload as f64: {v}");
                } else {
                    info!(
                        "payload string  {:?}  payload {:?}",
                        String::from_utf8_lossy(&payload[..payload.len().min(12)]),
                        &payload[..payload.len().min(12)],
                    );
                }
                CaMsg {
                    // TODO use different structs for request and response:
                    ty: CaMsgTy::ReadNotifyRes(ReadNotifyRes {
                        data_type: hi.data_type,
                        data_count: hi.data_count,
                        sid: hi.param1,
                        ioid: hi.param2,
                    }),
                }
            }
            x => return Err(Error::with_msg_no_trace(format!("unsupported ca command {}", x))),
        };
        Ok(msg)
    }
}

#[derive(Debug)]
pub enum CaItem {
    Empty,
    Msg(CaMsg),
}

impl CaItem {
    fn empty() -> Self {
        CaItem::Empty
    }
}

#[derive(Clone, Debug)]
pub struct HeadInfo {
    cmdid: u16,
    payload_size: u16,
    data_type: u16,
    data_count: u16,
    param1: u32,
    param2: u32,
}

impl HeadInfo {
    pub fn from_netbuf(buf: &mut NetBuf) -> Result<Self, Error> {
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
        };
        Ok(hi)
    }

    pub fn payload(&self) -> usize {
        self.payload_size as _
    }
}

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
            Payload(k) => k.payload_size as _,
            Done => 123,
        }
    }
}

pub struct CaProto {
    tcp: TcpStream,
    state: CaState,
    buf: NetBuf,
    outbuf: NetBuf,
    out: VecDeque<CaMsg>,
}

impl CaProto {
    pub fn new(tcp: TcpStream) -> Self {
        Self {
            tcp,
            state: CaState::StdHead,
            buf: NetBuf::new(1024 * 128),
            outbuf: NetBuf::new(1024 * 128),
            out: VecDeque::new(),
        }
    }

    pub fn push_out(&mut self, item: CaMsg) {
        self.out.push_back(item);
    }

    fn inpbuf_conn(&mut self, need_min: usize) -> (&mut TcpStream, ReadBuf) {
        (&mut self.tcp, self.buf.read_buf_for_fill(need_min))
    }

    fn outbuf_conn(&mut self) -> (&mut TcpStream, &[u8]) {
        (&mut self.tcp, self.outbuf.data())
    }

    fn out_msg_buf(&mut self) -> Option<(&CaMsg, &mut [u8])> {
        if let Some(item) = self.out.front() {
            info!("attempt to serialize outgoing message  msg {:?}", item);
            if let Ok(buf) = self.outbuf.write_buf(item.len()) {
                Some((item, buf))
            } else {
                error!("output buffer too small for message");
                None
            }
        } else {
            None
        }
    }

    fn attempt_output(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        let (w, b) = self.outbuf_conn();
        pin_mut!(w);
        match w.poll_write(cx, b) {
            Ready(k) => match k {
                Ok(k) => {
                    info!("sent {} bytes  {:?}", k, &self.outbuf.data()[..k]);
                    match self.outbuf.adv(k) {
                        Ok(()) => Ready(Ok(())),
                        Err(e) => {
                            error!("advance error {:?}", e);
                            Ready(Err(e))
                        }
                    }
                }
                Err(e) => {
                    error!("output write error {:?}", e);
                    Ready(Err(e.into()))
                }
            },
            Pending => Pending,
        }
    }

    fn loop_body(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Option<Poll<CaItem>>, Error> {
        use Poll::*;
        if self.out.len() != 0 || self.outbuf.len() != 0 {
            info!("loop_body  out {}  outbuf {}", self.out.len(), self.outbuf.len());
        }
        let output_res_1: Option<Poll<()>> = 'll1: loop {
            if self.out.len() == 0 {
                break None;
            }
            while let Some((msg, buf)) = self.out_msg_buf() {
                msg.place_into(buf);
                self.out.pop_front();
            }
            while self.outbuf.len() > 0 {
                match Self::attempt_output(self.as_mut(), cx)? {
                    Ready(()) => {}
                    Pending => {
                        break 'll1 Some(Pending);
                    }
                }
            }
        };
        let output_res_2: Option<Poll<()>> = if let Some(Pending) = output_res_1 {
            Some(Pending)
        } else {
            loop {
                if self.outbuf.len() == 0 {
                    break None;
                }
                match Self::attempt_output(self.as_mut(), cx)? {
                    Ready(()) => {}
                    Pending => break Some(Pending),
                }
            }
        };
        let need_min = self.state.need_min();
        let read_res = {
            if self.buf.cap() < need_min {
                self.state = CaState::Done;
                let e = Error::with_msg_no_trace(format!(
                    "buffer too small for need_min  {}  {}",
                    self.buf.cap(),
                    self.state.need_min()
                ));
                Err(e)
            } else if self.buf.len() < need_min {
                let (w, mut rbuf) = self.inpbuf_conn(need_min);
                pin_mut!(w);
                match w.poll_read(cx, &mut rbuf) {
                    Ready(k) => match k {
                        Ok(()) => {
                            let nf = rbuf.filled().len();
                            if nf == 0 {
                                info!("EOF");
                                // TODO may need another state, if not yet done when input is EOF.
                                self.state = CaState::Done;
                                Ok(Some(Ready(CaItem::empty())))
                            } else {
                                if false {
                                    info!("received {} bytes", rbuf.filled().len());
                                    let t = rbuf.filled().len().min(32);
                                    info!("received data  {:?}", &rbuf.filled()[0..t]);
                                }
                                match self.buf.wadv(nf) {
                                    Ok(()) => Ok(Some(Ready(CaItem::empty()))),
                                    Err(e) => {
                                        error!("netbuf wadv fail  nf {nf}");
                                        Err(e)
                                    }
                                }
                            }
                        }
                        Err(e) => Err(e.into()),
                    },
                    Pending => Ok(Some(Pending)),
                }
            } else {
                Ok(None)
            }
        }?;
        let parse_res: Option<CaItem> = self.parse_item()?;
        match (output_res_2, read_res, parse_res) {
            (_, _, Some(item)) => Ok(Some(Ready(item))),
            (Some(Pending), _, _) => Ok(Some(Pending)),
            (_, Some(Pending), _) => Ok(Some(Pending)),
            (_, None, None) => {
                // TODO constrain how often we can go to this case consecutively.
                Ok(None)
            }
            (_, Some(_), None) => Ok(None),
        }
    }

    fn parse_item(&mut self) -> Result<Option<CaItem>, Error> {
        loop {
            if self.buf.len() < self.state.need_min() {
                break Ok(None);
            }
            break match &self.state {
                CaState::StdHead => {
                    let hi = HeadInfo::from_netbuf(&mut self.buf)?;
                    if hi.cmdid == 6 || hi.cmdid > 26 || hi.data_type > 10 || hi.payload_size > 40 {
                        warn!("StdHead  {hi:?}");
                    }
                    if hi.payload_size == 0xffff && hi.data_count == 0 {
                        self.state = CaState::ExtHead(hi);
                        Ok(None)
                    } else {
                        if hi.payload_size == 0 {
                            self.state = CaState::StdHead;
                            let msg = CaMsg::from_proto_infos(&hi, &[])?;
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
                    warn!("ExtHead  payload_size {payload_size}  data_count {data_count}");
                    if payload_size == 0 {
                        let msg = CaMsg::from_proto_infos(hi, &[])?;
                        self.state = CaState::StdHead;
                        Ok(Some(CaItem::Msg(msg)))
                    } else {
                        self.state = CaState::Payload(hi.clone());
                        Ok(None)
                    }
                }
                CaState::Payload(hi) => {
                    let g = self.buf.read_bytes(hi.payload_size as _)?;
                    let msg = CaMsg::from_proto_infos(hi, g)?;
                    self.state = CaState::StdHead;
                    Ok(Some(CaItem::Msg(msg)))
                }
                CaState::Done => Err(Error::with_msg_no_trace("attempt to parse in Done state")),
            };
        }
    }
}

impl Stream for CaProto {
    type Item = Result<CaItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let CaState::Done = self.state {
            return Ready(None);
        } else {
            loop {
                break match Self::loop_body(self.as_mut(), cx) {
                    Ok(Some(Ready(k))) => Ready(Some(Ok(k))),
                    Ok(Some(Pending)) => Pending,
                    Ok(None) => continue,
                    Err(e) => Ready(Some(Err(e))),
                };
            }
        }
    }
}
