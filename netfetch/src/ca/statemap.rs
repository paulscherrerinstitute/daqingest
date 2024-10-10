use crate::ca::conn::ChannelStateInfo;
use crate::conf::ChannelConfig;
use crate::daemon_common::ChannelName;
use dashmap::DashMap;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use serieswriter::fixgridwriter::ChannelStatusSeriesWriter;
use serieswriter::fixgridwriter::ChannelStatusWriteState;
use std::collections::btree_map::RangeMut;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::ops::RangeBounds;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

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
    pub health_update_count: usize,
    pub value: ConnectionStateValue,
}

#[derive(Debug, Clone, Serialize)]
pub enum WithAddressState {
    Unassigned {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    Assigned(ConnectionState),
}

#[derive(Debug, Clone, Serialize)]
pub struct UnassignedState {
    #[serde(with = "humantime_serde")]
    since: SystemTime,
    #[serde(with = "serde_helper::serde_Instant")]
    unused_since_ts: Instant,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnassigningForConfigChangeState {
    pub config_new: ChannelConfig,
    pub addr: SocketAddr,
    #[serde(with = "serde_helper::serde_Instant")]
    pub since: Instant,
}

#[derive(Debug, Clone, Serialize)]
pub enum WithStatusSeriesIdStateInner {
    AddrSearchPending {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    WithAddress {
        addr: SocketAddrV4,
        state: WithAddressState,
    },
    UnknownAddress {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    NoAddress {
        #[serde(with = "humantime_serde")]
        since: SystemTime,
    },
    MaybeWrongAddress(MaybeWrongAddressState),
    UnassigningForConfigChange(UnassigningForConfigChangeState),
}

#[derive(Debug, Clone, Serialize)]
pub struct MaybeWrongAddressState {
    #[serde(with = "humantime_serde")]
    pub since: SystemTime,
    pub backoff_dt: Duration,
}

impl MaybeWrongAddressState {
    pub fn new(since: SystemTime, backoff_cnt: u32) -> Self {
        let f = 1. + 10. * (backoff_cnt as f32 / 4.).tanh();
        let dtms = 4e3_f32 * f;
        Self {
            since,
            backoff_dt: Duration::from_millis(dtms as u64),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WithStatusSeriesIdState {
    pub cssid: ChannelStatusSeriesId,
    pub addr_find_backoff: u32,
    pub inner: WithStatusSeriesIdStateInner,
    #[serde(serialize_with = "serde_ser_channel_status_writer")]
    pub writer_status: Option<ChannelStatusSeriesWriter>,
    #[serde(skip)]
    pub writer_status_state: Option<ChannelStatusWriteState>,
}

// Need Clone because we use the state tree for metrics output
// TODO use a new info struct
impl Clone for WithStatusSeriesIdState {
    fn clone(&self) -> Self {
        Self {
            cssid: self.cssid.clone(),
            addr_find_backoff: self.addr_find_backoff.clone(),
            inner: self.inner.clone(),
            writer_status: None,
            writer_status_state: None,
        }
    }
}

fn serde_ser_channel_status_writer<S>(_: &Option<ChannelStatusSeriesWriter>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_none()
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
    WithStatusSeriesId(WithStatusSeriesIdState),
}

#[derive(Debug, Clone, Serialize)]
pub enum ChannelStateValue {
    Active(ActiveChannelState),
    ToRemove { addr: Option<SocketAddrV4> },
    InitDummy,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelState {
    pub value: ChannelStateValue,
    pub config: ChannelConfig,
    pub touched: u8,
}

impl ChannelState {
    // TODO remove when no longer needed
    pub fn is_dummy(&self) -> bool {
        if let ChannelStateValue::InitDummy = self.value {
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelStateMap {
    map: BTreeMap<ChannelName, ChannelState>,
    #[serde(skip)]
    map2: HashMap<ChannelName, ChannelState>,
    // TODO implement same interface via dashmap and compare
    #[serde(skip)]
    map3: DashMap<ChannelName, ChannelState>,
}

impl ChannelStateMap {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            map2: HashMap::new(),
            map3: DashMap::new(),
        }
    }

    pub fn insert(&mut self, k: ChannelName, v: ChannelState) -> Option<ChannelState> {
        self.map.insert(k, v)
    }

    pub fn get_mut(&mut self, k: &ChannelName) -> Option<&mut ChannelState> {
        self.map.get_mut(k)
    }

    pub fn get_mut_or_dummy_init(&mut self, k: &ChannelName) -> &mut ChannelState {
        if !self.map.contains_key(k) {
            let dummy = ChannelState {
                value: ChannelStateValue::InitDummy,
                config: ChannelConfig::dummy(),
                touched: 0,
            };
            self.map.insert(k.clone(), dummy);
        }
        self.map.get_mut(k).unwrap()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChannelName, &ChannelState)> {
        self.map.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&ChannelName, &mut ChannelState)> {
        self.map.iter_mut()
    }

    pub fn iter_mut_dash(&mut self) -> ChannelStateIter {
        todo!()
    }

    pub fn range_mut<R>(&mut self, range: R) -> RangeMut<ChannelName, ChannelState>
    where
        R: RangeBounds<ChannelName>,
    {
        self.map.range_mut(range)
    }

    pub fn remove(&mut self, k: &ChannelName) -> Option<ChannelState> {
        self.map.remove(k)
    }
}

pub struct ChannelStateIter<'a> {
    _m1: &'a u32,
}
