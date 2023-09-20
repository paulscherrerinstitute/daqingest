use crate::ca::conn::ChannelStateInfo;
use crate::daemon_common::Channel;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use std::collections::BTreeMap;
use std::net::SocketAddrV4;
use std::time::Instant;
use std::time::SystemTime;

pub const CHANNEL_STATUS_DUMMY_SCALAR_TYPE: i32 = i32::MIN + 1;

#[derive(Debug)]
pub enum CaConnStateValue {
    Fresh,
    HadFeedback,
    Shutdown { since: Instant },
}

#[derive(Debug)]
pub struct CaConnState {
    pub last_feedback: Instant,
    pub value: CaConnStateValue,
}

impl CaConnState {
    pub fn new(value: CaConnStateValue) -> Self {
        Self {
            last_feedback: Instant::now(),
            value,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum ConnectionStateValue {
    Unknown,
    ChannelStateInfo(ChannelStateInfo),
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionState {
    #[serde(with = "humantime_serde")]
    pub updated: SystemTime,
    pub value: ConnectionStateValue,
}

#[derive(Debug, Clone, Serialize)]
pub enum WithAddressState {
    Unassigned {
        //#[serde(with = "serde_Instant")]
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    Assigned(ConnectionState),
}

#[derive(Debug, Clone, Serialize)]
pub enum WithStatusSeriesIdStateInner {
    UnknownAddress {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    SearchPending {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    WithAddress {
        addr: SocketAddrV4,
        state: WithAddressState,
    },
    NoAddress {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct WithStatusSeriesIdState {
    pub inner: WithStatusSeriesIdStateInner,
}

#[derive(Debug, Clone, Serialize)]
pub enum ActiveChannelState {
    Init {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    WaitForStatusSeriesId {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    WithStatusSeriesId {
        status_series_id: ChannelStatusSeriesId,
        state: WithStatusSeriesIdState,
    },
}

#[derive(Debug, Clone, Serialize)]
pub enum ChannelStateValue {
    Active(ActiveChannelState),
    ToRemove { addr: Option<SocketAddrV4> },
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelState {
    pub value: ChannelStateValue,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelStateMap {
    map: BTreeMap<Channel, ChannelState>,
}

impl ChannelStateMap {
    pub fn new() -> Self {
        Self { map: BTreeMap::new() }
    }

    pub fn inner(&mut self) -> &mut BTreeMap<Channel, ChannelState> {
        &mut self.map
    }
}
