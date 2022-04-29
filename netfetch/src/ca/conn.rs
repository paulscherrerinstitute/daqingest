use super::proto::{CaItem, CaMsg, CaMsgTy, CaProto};
use crate::ca::proto::{CreateChan, EventAdd, ReadNotify};
use err::Error;
use futures_util::{Stream, StreamExt};
use log::*;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::net::TcpStream;

#[derive(Debug)]
enum ChannelError {
    NoSuccess,
}

#[derive(Debug)]
struct EventedState {
    ts_last: Instant,
}

#[derive(Debug)]
enum MonitoringState {
    AddingEvent,
    Evented(EventedState),
    Reading,
    Read,
    Muted,
}

#[derive(Debug)]
struct CreatedState {
    cid: u32,
    sid: u32,
    ts_created: Instant,
    state: MonitoringState,
}

#[derive(Debug)]
enum ChannelState {
    NotCreated,
    Creating { cid: u32, ts_beg: Instant },
    Created(CreatedState),
    Error(ChannelError),
}

enum CaConnState {
    Init,
    Listen,
    PeerReady,
    Done,
}

struct IdStore {
    next: u32,
}

impl IdStore {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> u32 {
        let ret = self.next;
        self.next += 1;
        ret
    }
}

pub struct CaConn {
    state: CaConnState,
    proto: CaProto,
    cid_store: IdStore,
    ioid_store: IdStore,
    subid_store: IdStore,
    channels: BTreeMap<u32, ChannelState>,
    cid_by_name: BTreeMap<String, u32>,
    cid_by_subid: BTreeMap<u32, u32>,
    name_by_cid: BTreeMap<u32, String>,
    poll_count: usize,
}

impl CaConn {
    pub fn new(tcp: TcpStream) -> Self {
        Self {
            state: CaConnState::Init,
            proto: CaProto::new(tcp),
            cid_store: IdStore::new(),
            ioid_store: IdStore::new(),
            subid_store: IdStore::new(),
            channels: BTreeMap::new(),
            cid_by_name: BTreeMap::new(),
            cid_by_subid: BTreeMap::new(),
            name_by_cid: BTreeMap::new(),
            poll_count: 0,
        }
    }

    pub fn channel_add(&mut self, channel: String) {
        let cid = self.cid_by_name(&channel);
        if self.channels.contains_key(&cid) {
        } else {
            self.channels.insert(cid, ChannelState::NotCreated);
        }
    }

    fn cid_by_name(&mut self, name: &str) -> u32 {
        if let Some(cid) = self.cid_by_name.get(name) {
            *cid
        } else {
            let cid = self.cid_store.next();
            self.cid_by_name.insert(name.into(), cid);
            self.name_by_cid.insert(cid, name.into());
            cid
        }
    }

    fn name_by_cid(&self, cid: u32) -> Option<&str> {
        self.name_by_cid.get(&cid).map(|x| x.as_str())
    }
}

impl Stream for CaConn {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.poll_count += 1;
        if self.poll_count > 30 {
            error!("TODO CaConn reached poll_count limit");
            return Ready(None);
        }
        loop {
            break match &self.state {
                CaConnState::Init => {
                    let msg = CaMsg { ty: CaMsgTy::Version };
                    self.proto.push_out(msg);
                    let msg = CaMsg {
                        ty: CaMsgTy::ClientName,
                    };
                    self.proto.push_out(msg);
                    let msg = CaMsg { ty: CaMsgTy::HostName };
                    self.proto.push_out(msg);
                    self.state = CaConnState::Listen;
                    continue;
                }
                CaConnState::Listen => match self.proto.poll_next_unpin(cx) {
                    Ready(Some(k)) => match k {
                        Ok(k) => match k {
                            CaItem::Empty => {
                                info!("CaItem::Empty");
                                Ready(Some(Ok(())))
                            }
                            CaItem::Msg(msg) => match msg.ty {
                                CaMsgTy::VersionRes(n) => {
                                    if n < 12 || n > 13 {
                                        error!("See some unexpected version {n}  channel search may not work.");
                                        Ready(Some(Ok(())))
                                    } else {
                                        info!("Received peer version {n}");
                                        self.state = CaConnState::PeerReady;
                                        continue;
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
                            Ready(Some(Ok(())))
                        }
                    },
                    Ready(None) => {
                        warn!("CaProto is done");
                        self.state = CaConnState::Done;
                        continue;
                    }
                    Pending => Pending,
                },
                CaConnState::PeerReady => {
                    // TODO unify with Listen state where protocol gets polled as well.
                    let mut msgs_tmp = vec![];
                    // TODO profile, efficient enough?
                    let keys: Vec<u32> = self.channels.keys().map(|x| *x).collect();
                    for cid in keys {
                        match self.channels[&cid] {
                            ChannelState::NotCreated => {
                                let name = self
                                    .name_by_cid(cid)
                                    .ok_or_else(|| Error::with_msg_no_trace("name for cid not known"));
                                let name = match name {
                                    Ok(k) => k,
                                    Err(e) => return Ready(Some(Err(e))),
                                };
                                info!("Sending CreateChan for {}", name);
                                let msg = CaMsg {
                                    ty: CaMsgTy::CreateChan(CreateChan {
                                        cid,
                                        channel: name.into(),
                                    }),
                                };
                                msgs_tmp.push(msg);
                                // TODO handle not-found error:
                                let ch_s = self.channels.get_mut(&cid).unwrap();
                                *ch_s = ChannelState::Creating {
                                    cid,
                                    ts_beg: Instant::now(),
                                };
                            }
                            _ => {}
                        }
                    }
                    let mut do_wake_again = false;
                    if msgs_tmp.len() > 0 {
                        info!("msgs_tmp.len() {}", msgs_tmp.len());
                        do_wake_again = true;
                    }
                    // TODO be careful to not overload outgoing message queue.
                    for msg in msgs_tmp {
                        self.proto.push_out(msg);
                    }
                    let res = match self.proto.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => {
                            match k {
                                CaItem::Msg(k) => match k.ty {
                                    CaMsgTy::SearchRes(k) => {
                                        let a = k.addr.to_be_bytes();
                                        let addr = format!("{}.{}.{}.{}:{}", a[0], a[1], a[2], a[3], k.tcp_port);
                                        info!("Search result indicates server address: {addr}");
                                    }
                                    CaMsgTy::CreateChanRes(k) => {
                                        // TODO handle cid-not-found which can also indicate peer error.
                                        let cid = k.cid;
                                        let name = self.name_by_cid(cid);
                                        info!("Channel created for {name:?} now register for events");
                                        let subid = self.subid_store.next();
                                        self.cid_by_subid.insert(subid, cid);
                                        let msg = CaMsg {
                                            ty: CaMsgTy::EventAdd(EventAdd {
                                                sid: k.sid,
                                                data_type: k.data_type,
                                                data_count: k.data_count,
                                                subid,
                                            }),
                                        };
                                        self.proto.push_out(msg);
                                        do_wake_again = true;
                                        // TODO handle not-found error:
                                        let ch_s = self.channels.get_mut(&k.cid).unwrap();
                                        *ch_s = ChannelState::Created(CreatedState {
                                            cid: k.cid,
                                            sid: k.sid,
                                            ts_created: Instant::now(),
                                            state: MonitoringState::AddingEvent,
                                        });
                                        info!(
                                            "Channel is created  cid {}  sid {}  name {}",
                                            k.cid, k.sid, self.name_by_cid[&k.cid]
                                        );
                                    }
                                    CaMsgTy::EventAddRes(k) => {
                                        // TODO handle subid-not-found which can also be peer error:
                                        let cid = *self.cid_by_subid.get(&k.subid).unwrap();
                                        // TODO get rid of the string clone when I don't want the log output any longer:
                                        let name: String = self.name_by_cid(cid).unwrap().into();
                                        // TODO handle not-found error:
                                        let ch_s = self.channels.get_mut(&cid).unwrap();
                                        match ch_s {
                                            ChannelState::Created(st) => {
                                                match st.state {
                                                    MonitoringState::AddingEvent => {
                                                        info!("Confirmation {name} is subscribed.");
                                                        // TODO get ts from faster common source:
                                                        st.state = MonitoringState::Evented(EventedState {
                                                            ts_last: Instant::now(),
                                                        });
                                                    }
                                                    MonitoringState::Evented(ref mut st) => {
                                                        // TODO get ts from faster common source:
                                                        st.ts_last = Instant::now();
                                                    }
                                                    _ => {
                                                        warn!("bad state? not always, could be late message.");
                                                    }
                                                }
                                            }
                                            _ => {
                                                warn!("unexpected state: EventAddRes while having {ch_s:?}");
                                            }
                                        }
                                    }
                                    _ => {}
                                },
                                _ => {}
                            }
                            Ready(Some(Ok(())))
                        }
                        Ready(Some(Err(e))) => {
                            error!("CaProto yields error: {e:?}");
                            Ready(Some(Err(e)))
                        }
                        Ready(None) => {
                            warn!("CaProto is done");
                            self.state = CaConnState::Done;
                            Ready(Some(Ok(())))
                        }
                        Pending => Pending,
                    };
                    if do_wake_again {
                        info!("do_wake_again");
                        cx.waker().wake_by_ref();
                    }
                    res
                }
                CaConnState::Done => Ready(None),
            };
        }
    }
}
