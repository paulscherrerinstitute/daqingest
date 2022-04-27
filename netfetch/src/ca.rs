use crate::netbuf::NetBuf;
use async_channel::{bounded, Receiver};
use bytes::{BufMut, BytesMut};
use err::{ErrStr, Error};
use futures_util::{pin_mut, FutureExt, Stream, StreamExt};
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    cmd: u16,
    payload_len: u16,
    type_type: u16,
    data_len: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FetchItem {
    Log(String),
    Message(Message),
}

pub async fn ca_connect_1() -> Result<Receiver<Result<FetchItem, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx2 = tx.clone();
    tokio::task::spawn(
        async move {
            let mut conn = tokio::net::TcpStream::connect("S30CB06-CVME-LLRF2.psi.ch:5064").await?;
            let (mut inp, mut out) = conn.split();
            tx.send(Ok(FetchItem::Log(format!("connected")))).await.errstr()?;
            let mut buf = [0; 64];

            let mut b2 = BytesMut::with_capacity(128);
            b2.put_u16(0x00);
            b2.put_u16(0);
            b2.put_u16(0);
            b2.put_u16(0xb);
            b2.put_u32(0);
            b2.put_u32(0);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await.errstr()?;
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await
                .errstr()?;

            // Search to get cid:
            let chn = b"SATCB01-DBPM220:Y2";
            b2.clear();
            b2.put_u16(0x06);
            b2.put_u16((16 + chn.len()) as u16);
            b2.put_u16(0x00);
            b2.put_u16(0x0b);
            b2.put_u32(0x71803472);
            b2.put_u32(0x71803472);
            b2.put_slice(chn);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await.errstr()?;
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await
                .errstr()?;

            Ok::<_, Error>(())
        }
        .then({
            move |item| async move {
                match item {
                    Ok(_) => {}
                    Err(e) => {
                        tx2.send(Ok(FetchItem::Log(format!("Seeing error: {:?}", e))))
                            .await
                            .errstr()?;
                    }
                }
                Ok::<_, Error>(())
            }
        }),
    );
    Ok(rx)
}

pub struct CaConnectOpts {
    pub source: String,
    pub channel_name: String,
}

pub async fn ca_connect_2(opts: CaConnectOpts) -> Result<(), Error> {
    let (tx, rx) = bounded::<Result<FetchItem, Error>>(16);
    let tx2 = tx.clone();
    tokio::task::spawn(
        async move {
            let mut conn = tokio::net::TcpStream::connect(&opts.source).await?;
            let (mut inp, mut out) = conn.split();
            tx.send(Ok(FetchItem::Log(format!("connected")))).await.errstr()?;
            let mut buf = [0; 64];

            let mut b2 = BytesMut::with_capacity(128);
            b2.put_u16(0x00);
            b2.put_u16(0);
            b2.put_u16(0);
            b2.put_u16(0xb);
            b2.put_u32(0);
            b2.put_u32(0);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await.errstr()?;
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await
                .errstr()?;

            // Search to get cid:
            let chn = opts.channel_name.as_bytes();
            b2.clear();
            b2.put_u16(0x06);
            b2.put_u16((16 + chn.len()) as u16);
            b2.put_u16(0x00);
            b2.put_u16(0x0b);
            b2.put_u32(0x71803472);
            b2.put_u32(0x71803472);
            b2.put_slice(chn);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await.errstr()?;
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await
                .errstr()?;

            Ok::<_, Error>(())
        }
        .then({
            move |item| async move {
                match item {
                    Ok(_) => {}
                    Err(e) => {
                        tx2.send(Ok(FetchItem::Log(format!("Seeing error: {:?}", e))))
                            .await
                            .errstr()?;
                    }
                }
                Ok::<_, Error>(())
            }
        }),
    );
    loop {
        match rx.recv().await {
            Ok(item) => {
                info!("got item: {item:?}");
            }
            Err(e) => {
                error!("can no longer receive from queue: {e:?}");
                break;
            }
        }
    }
    Ok(())
}

pub async fn ca_connect_3(opts: CaConnectOpts) -> Result<(), Error> {
    let tcp = TcpStream::connect(&opts.source).await?;
    let mut conn = CaConn::new(tcp, opts.channel_name);
    while let Some(_item) = conn.next().await {}
    Ok(())
}

enum CaConnState {
    Init,
    Listen,
    Done,
}

struct CaConn {
    state: CaConnState,
    proto: CaProto,
    ioid: u32,
    // tmp: simply try to communicate with a given channel:
    channel: String,
}

impl CaConn {
    fn new(tcp: TcpStream, channel: String) -> Self {
        Self {
            state: CaConnState::Init,
            proto: CaProto::new(tcp),
            ioid: 0,
            channel,
        }
    }
}

const CA_PROTO_VERSION: u16 = 13;

impl Stream for CaConn {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &self.state {
                CaConnState::Init => {
                    let msg = CaMsg {
                        ty: CaMsgTy::Version(CA_PROTO_VERSION),
                    };
                    self.proto.out.push_back(msg);
                    let msg = CaMsg {
                        ty: CaMsgTy::ClientName,
                    };
                    self.proto.out.push_back(msg);
                    let msg = CaMsg { ty: CaMsgTy::HostName };
                    self.proto.out.push_back(msg);
                    self.state = CaConnState::Listen;
                    continue;
                }
                CaConnState::Listen => match self.proto.poll_next_unpin(cx) {
                    Ready(Some(k)) => {
                        match k {
                            Ok(k) => match k {
                                CaItem::Empty => Ready(Some(Ok(()))),
                                CaItem::Msg(msg) => match msg.ty {
                                    CaMsgTy::Version(n) => {
                                        if n < 12 || n > 13 {
                                            error!("See some unexpected version {n}  channel search may not work.");
                                            Ready(Some(Ok(())))
                                        } else {
                                            info!("Received peer version {n}");
                                            if false {
                                                let msg = CaMsg {
                                                    ty: CaMsgTy::Search(Search {
                                                        id: 501,
                                                        channel: self.channel.clone(),
                                                    }),
                                                };
                                                self.proto.out.push_back(msg);
                                            }
                                            let msg = CaMsg {
                                                ty: CaMsgTy::CreateChan(CreateChan {
                                                    cid: 1700,
                                                    channel: self.channel.clone(),
                                                }),
                                            };
                                            self.proto.out.push_back(msg);
                                            Ready(Some(Ok(())))
                                        }
                                    }
                                    CaMsgTy::CreateChanRes(k) => {
                                        info!("Channel created");
                                        let msg = CaMsg {
                                            ty: CaMsgTy::ReadNotify(ReadNotify {
                                                sid: k.sid,
                                                data_type: k.data_type,
                                                data_count: k.data_count,
                                                ioid: self.ioid,
                                            }),
                                        };
                                        self.ioid += 1;
                                        self.proto.out.push_back(msg);
                                        let msg = CaMsg {
                                            ty: CaMsgTy::EventAdd(EventAdd {
                                                sid: k.sid,
                                                data_type: k.data_type,
                                                data_count: k.data_count,
                                                subid: self.ioid,
                                            }),
                                        };
                                        self.ioid += 1;
                                        self.proto.out.push_back(msg);
                                        Ready(Some(Ok(())))
                                    }
                                    CaMsgTy::ReadNotify(_) => {
                                        // TODO create a generic container for the data updates.
                                        Ready(Some(Ok(())))
                                    }
                                    CaMsgTy::EventAddRes(_) => {
                                        // TODO create a generic container for the data updates.
                                        Ready(Some(Ok(())))
                                    }
                                    k => {
                                        info!("Got some other unhandled message: {k:?}");
                                        Ready(Some(Ok(())))
                                    }
                                },
                            },
                            Err(e) => {
                                error!("got error item from CaProto {e:?}");
                                Ready(Some(Ok(())))
                            }
                        }
                    }
                    Ready(None) => {
                        self.state = CaConnState::Done;
                        continue;
                    }
                    Pending => Pending,
                },
                CaConnState::Done => Ready(None),
            };
        }
    }
}

#[derive(Debug)]
enum CaItem {
    Empty,
    Msg(CaMsg),
}

impl CaItem {
    fn empty() -> Self {
        CaItem::Empty
    }
}

#[derive(Debug)]
struct Search {
    id: u32,
    channel: String,
}

#[derive(Debug)]
struct ClientNameRes {
    name: String,
}

#[derive(Debug)]
struct CreateChan {
    cid: u32,
    channel: String,
}

#[derive(Debug)]
struct CreateChanRes {
    data_type: u16,
    data_count: u16,
    cid: u32,
    sid: u32,
}

#[derive(Debug)]
struct AccessRightsRes {
    cid: u32,
    rights: u32,
}

#[derive(Debug)]
struct EventAdd {
    data_type: u16,
    data_count: u16,
    sid: u32,
    subid: u32,
}

#[derive(Debug)]
struct EventAddRes {
    data_type: u16,
    data_count: u16,
    status: u32,
    subid: u32,
}

#[derive(Debug)]
struct ReadNotify {
    data_type: u16,
    data_count: u16,
    sid: u32,
    ioid: u32,
}

#[derive(Debug)]
struct ReadNotifyRes {
    data_type: u16,
    data_count: u16,
    sid: u32,
    ioid: u32,
}

#[derive(Debug)]
enum CaScalarType {
    I8,
    I16,
    I32,
    F32,
    F64,
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
            k => return Err(Error::with_msg_no_trace(format!("bad dbr type id: {k}"))),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
struct CaPayloadType {
    #[allow(unused)]
    scalar_type: CaScalarType,
}

#[derive(Debug)]
enum CaMsgTy {
    Version(u16),
    ClientName,
    ClientNameRes(ClientNameRes),
    HostName,
    Search(Search),
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
            Version(_) => 0,
            ClientName => 20,
            ClientNameRes(_) => 20,
            HostName => 21,
            Search(_) => 6,
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
            Version(_) => 0,
            ClientName => 8,
            ClientNameRes(x) => (7 + x.name.len()) / 8 * 8,
            HostName => 8,
            Search(s) => (7 + s.channel.len()) / 8 * 8,
            CreateChan(x) => (7 + x.channel.len()) / 8 * 8,
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
            Version(n) => *n,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(_) => {
                // Reply-flag
                1
            }
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
            Version(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(_) => CA_PROTO_VERSION,
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
            Version(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(e) => e.id,
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
            Version(_) => 0,
            ClientName => 0,
            ClientNameRes(_) => 0,
            HostName => 0,
            Search(e) => e.id,
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
            Version(_) => {}
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
                    error!("bad buffer given");
                    panic!();
                }
                unsafe { std::ptr::copy(&d[0] as _, &mut buf[0] as _, d.len()) };
            }
            CreateChan(x) => {
                for x in &mut buf[..] {
                    *x = 0;
                }
                let d = x.channel.as_bytes();
                if buf.len() < d.len() + 1 {
                    error!("bad buffer given");
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
struct CaMsg {
    ty: CaMsgTy,
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

    fn from_proto_infos(hi: &HeadInfo, payload: &[u8]) -> Result<Self, Error> {
        let msg = match hi.cmdid {
            0 => CaMsg {
                ty: CaMsgTy::Version(hi.data_count),
            },
            20 => {
                let name = std::ffi::CString::new(payload)
                    .map(|s| s.into_string().unwrap_or_else(|e| format!("{e:?}")))
                    .unwrap_or_else(|e| format!("{e:?}"));
                CaMsg {
                    ty: CaMsgTy::ClientNameRes(ClientNameRes { name }),
                }
            }
            21 => CaMsg { ty: CaMsgTy::HostName },
            6 => CaMsg {
                ty: CaMsgTy::Search(Search {
                    id: 88002,
                    channel: String::new(),
                }),
            },
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
                match ca_st {
                    CaScalarType::F64 => {
                        // TODO handle wrong payload sizer in more distinct way.
                        let v = f64::from_be_bytes(payload.try_into()?);
                        info!("Payload as f64: {v}");
                    }
                    _ => {
                        warn!("TODO handle {ca_st:?}");
                    }
                }
                let d = EventAddRes {
                    data_type: hi.data_type,
                    data_count: hi.data_count,
                    status: hi.param1,
                    subid: hi.param2,
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

#[derive(Clone, Debug)]
struct HeadInfo {
    cmdid: u16,
    payload_size: u16,
    data_type: u16,
    data_count: u16,
    param1: u32,
    param2: u32,
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

struct CaProto {
    tcp: TcpStream,
    state: CaState,
    buf: NetBuf,
    outbuf: NetBuf,
    out: VecDeque<CaMsg>,
}

impl CaProto {
    fn new(tcp: TcpStream) -> Self {
        Self {
            tcp,
            state: CaState::StdHead,
            buf: NetBuf::new(1024 * 128),
            outbuf: NetBuf::new(1024 * 128),
            out: VecDeque::new(),
        }
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
                    let command = self.buf.read_u16_be()?;
                    let payload_size = self.buf.read_u16_be()?;
                    let data_type = self.buf.read_u16_be()?;
                    let data_count = self.buf.read_u16_be()?;
                    let param1 = self.buf.read_u32_be()?;
                    let param2 = self.buf.read_u32_be()?;
                    let hi = HeadInfo {
                        cmdid: command,
                        payload_size,
                        data_type,
                        data_count,
                        param1,
                        param2,
                    };
                    if hi.cmdid > 26 || hi.data_type > 10 || hi.payload_size > 8 {
                        warn!("StdHead  {hi:?}");
                    }
                    if payload_size == 0xffff && data_count == 0 {
                        self.state = CaState::ExtHead(hi);
                        Ok(None)
                    } else {
                        if payload_size == 0 {
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
