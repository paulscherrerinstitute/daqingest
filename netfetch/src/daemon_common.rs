use crate::ca::conn::CaConnEvent;
use crate::ca::findioc::FindIocRes;
use err::Error;
use serde::Serialize;
use std::collections::VecDeque;
use std::net::SocketAddrV4;

#[derive(Clone, Debug, Serialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct Channel {
    id: String,
}

impl Channel {
    pub fn new(id: String) -> Self {
        Self { id }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug)]
pub enum DaemonEvent {
    TimerTick,
    ChannelAdd(Channel),
    ChannelRemove(Channel),
    SearchDone(Result<VecDeque<FindIocRes>, Error>),
    CaConnEvent(SocketAddrV4, CaConnEvent),
    Shutdown,
}

impl DaemonEvent {
    pub fn summary(&self) -> String {
        use DaemonEvent::*;
        match self {
            TimerTick => format!("TimerTick"),
            ChannelAdd(x) => format!("ChannelAdd {x:?}"),
            ChannelRemove(x) => format!("ChannelRemove {x:?}"),
            SearchDone(_x) => format!("SearchDone"),
            CaConnEvent(_a, b) => {
                use crate::ca::conn::CaConnEventValue::*;
                match &b.value {
                    None => format!("CaConnEvent/None"),
                    EchoTimeout => format!("CaConnEvent/EchoTimeout"),
                    ConnCommandResult(_) => format!("CaConnEvent/ConnCommandResult"),
                    EndOfStream => format!("CaConnEvent/EndOfStream"),
                }
            }
            Shutdown => format!("Shutdown"),
        }
    }
}
