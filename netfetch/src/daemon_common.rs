use crate::ca::connset::CaConnSetItem;
use async_channel::Sender;
use serde::Serialize;

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

#[derive(Debug, Clone)]
pub enum DaemonEvent {
    TimerTick(u32, Sender<u32>),
    ChannelAdd(Channel),
    ChannelRemove(Channel),
    CaConnSetItem(CaConnSetItem),
    Shutdown,
}

impl DaemonEvent {
    pub fn summary(&self) -> String {
        use DaemonEvent::*;
        match self {
            TimerTick(_, _) => format!("TimerTick"),
            ChannelAdd(x) => format!("ChannelAdd {x:?}"),
            ChannelRemove(x) => format!("ChannelRemove {x:?}"),
            CaConnSetItem(_) => format!("CaConnSetItem"),
            Shutdown => format!("Shutdown"),
        }
    }
}
