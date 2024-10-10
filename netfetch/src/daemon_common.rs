use crate::ca::connset::CaConnSetItem;
use crate::conf::ChannelConfig;
use async_channel::Sender;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct ChannelName {
    name: String,
}

impl ChannelName {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub enum DaemonEvent {
    TimerTick(u32, Sender<u32>),
    ChannelAdd(ChannelConfig, crate::ca::conn::CmdResTx),
    ChannelRemove(ChannelName),
    CaConnSetItem(CaConnSetItem),
    Shutdown,
    ConfigReload(async_channel::Sender<u64>),
}

impl DaemonEvent {
    pub fn summary(&self) -> String {
        use DaemonEvent::*;
        match self {
            TimerTick(_, _) => format!("TimerTick"),
            ChannelAdd(x, _) => format!("ChannelAdd {x:?}"),
            ChannelRemove(x) => format!("ChannelRemove {x:?}"),
            CaConnSetItem(_) => format!("CaConnSetItem"),
            Shutdown => format!("Shutdown"),
            ConfigReload(..) => format!("ConfigReload"),
        }
    }
}
