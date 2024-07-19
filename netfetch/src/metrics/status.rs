use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ConnSetCmd;
use crate::conf::ChannelConfig;
use async_channel::Sender;
use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::SystemTime;

#[derive(Debug, Serialize)]
pub struct ChannelStates {
    running_since: DateTime<Utc>,
    // #[serde(with = "humantime_serde")]
    // running_since_2: SystemTime,
    channels: BTreeMap<String, ChannelState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageUsage {
    count: u64,
    bytes: u64,
}

impl StorageUsage {
    pub fn new() -> Self {
        Self { count: 0, bytes: 0 }
    }

    pub fn reset(&mut self) {
        self.count = 0;
        self.bytes = 0;
    }

    pub fn push_written(&mut self, payload_len: u32) {
        self.count += 1;
        self.bytes += 16 + payload_len as u64;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[derive(Debug, Serialize)]
struct ChannelState {
    ioc_address: Option<SocketAddr>,
    connection: ConnectionState,
    archiving_configuration: ChannelConfig,
    recv_count: u64,
    recv_bytes: u64,
    #[serde(with = "humantime_serde", skip_serializing_if = "system_time_epoch")]
    recv_last: SystemTime,
    #[serde(with = "humantime_serde", skip_serializing_if = "system_time_epoch")]
    write_st_last: SystemTime,
    #[serde(with = "humantime_serde", skip_serializing_if = "system_time_epoch")]
    write_mt_last: SystemTime,
    #[serde(with = "humantime_serde", skip_serializing_if = "system_time_epoch")]
    write_lt_last: SystemTime,
    #[serde(with = "humantime_serde", skip_serializing_if = "system_time_epoch")]
    updated: SystemTime,
    private: StatePrivate,
}

#[derive(Debug, Serialize)]
struct StatePrivate {
    status_emit_count: u64,
}

impl Default for StatePrivate {
    fn default() -> Self {
        Self { status_emit_count: 0 }
    }
}

fn system_time_epoch(x: &SystemTime) -> bool {
    *x == SystemTime::UNIX_EPOCH
}

#[derive(Debug, Serialize)]
enum ConnectionState {
    Connecting,
    Unreachable,
    Disconnected,
    Connected,
    Error,
}

// ChannelStatusesResponse
// BTreeMap<String, ChannelState>
pub async fn channel_states(params: HashMap<String, String>, tx: Sender<CaConnSetEvent>) -> axum::Json<ChannelStates> {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    let limit = params.get("limit").and_then(|x| x.parse().ok()).unwrap_or(40);
    let (tx2, rx2) = async_channel::bounded(1);
    let req = ChannelStatusesRequest { name, limit, tx: tx2 };
    let item = CaConnSetEvent::ConnSetCmd(ConnSetCmd::ChannelStatuses(req));
    // TODO handle error
    tx.send(item).await.unwrap();
    let res = rx2.recv().await.unwrap();
    let mut states = ChannelStates {
        running_since: Utc::now(),
        channels: BTreeMap::new(),
    };
    for (k, st1) in res.channels_ca_conn_set {
        use crate::ca::statemap::ChannelStateValue;
        match st1.value {
            ChannelStateValue::Active(st2) => {
                use crate::ca::statemap::ActiveChannelState;
                match st2 {
                    ActiveChannelState::Init { .. } => {
                        let chst = ChannelState {
                            ioc_address: None,
                            connection: ConnectionState::Connecting,
                            archiving_configuration: st1.config,
                            recv_count: 0,
                            recv_bytes: 0,
                            recv_last: SystemTime::UNIX_EPOCH,
                            write_st_last: SystemTime::UNIX_EPOCH,
                            write_mt_last: SystemTime::UNIX_EPOCH,
                            write_lt_last: SystemTime::UNIX_EPOCH,
                            updated: SystemTime::UNIX_EPOCH,
                            private: StatePrivate::default(),
                        };
                        states.channels.insert(k, chst);
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        let chst = ChannelState {
                            ioc_address: None,
                            connection: ConnectionState::Connecting,
                            archiving_configuration: st1.config,
                            recv_count: 0,
                            recv_bytes: 0,
                            recv_last: SystemTime::UNIX_EPOCH,
                            write_st_last: SystemTime::UNIX_EPOCH,
                            write_mt_last: SystemTime::UNIX_EPOCH,
                            write_lt_last: SystemTime::UNIX_EPOCH,
                            updated: SystemTime::UNIX_EPOCH,
                            private: StatePrivate::default(),
                        };
                        states.channels.insert(k, chst);
                    }
                    ActiveChannelState::WithStatusSeriesId(st3) => {
                        use crate::ca::statemap::WithStatusSeriesIdStateInner;
                        match st3.inner {
                            WithStatusSeriesIdStateInner::AddrSearchPending { .. } => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Connecting,
                                    archiving_configuration: st1.config,
                                    recv_count: 0,
                                    recv_bytes: 0,
                                    recv_last: SystemTime::UNIX_EPOCH,
                                    write_st_last: SystemTime::UNIX_EPOCH,
                                    write_mt_last: SystemTime::UNIX_EPOCH,
                                    write_lt_last: SystemTime::UNIX_EPOCH,
                                    updated: SystemTime::UNIX_EPOCH,
                                    private: StatePrivate::default(),
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::WithAddress { addr, state: st4 } => {
                                use crate::ca::statemap::WithAddressState;
                                match st4 {
                                    WithAddressState::Unassigned { .. } => {
                                        let chst = ChannelState {
                                            ioc_address: Some(SocketAddr::V4(addr)),
                                            connection: ConnectionState::Connecting,
                                            archiving_configuration: st1.config,
                                            recv_count: 0,
                                            recv_bytes: 0,
                                            recv_last: SystemTime::UNIX_EPOCH,
                                            write_st_last: SystemTime::UNIX_EPOCH,
                                            write_mt_last: SystemTime::UNIX_EPOCH,
                                            write_lt_last: SystemTime::UNIX_EPOCH,
                                            updated: SystemTime::UNIX_EPOCH,
                                            private: StatePrivate::default(),
                                        };
                                        states.channels.insert(k, chst);
                                    }
                                    WithAddressState::Assigned(st5) => {
                                        use crate::ca::statemap::ConnectionStateValue;
                                        match st5.value {
                                            ConnectionStateValue::Unknown => {
                                                let chst = ChannelState {
                                                    ioc_address: Some(SocketAddr::V4(addr)),
                                                    connection: ConnectionState::Connecting,
                                                    archiving_configuration: st1.config,
                                                    recv_count: 0,
                                                    recv_bytes: 0,
                                                    recv_last: SystemTime::UNIX_EPOCH,
                                                    write_st_last: SystemTime::UNIX_EPOCH,
                                                    write_mt_last: SystemTime::UNIX_EPOCH,
                                                    write_lt_last: SystemTime::UNIX_EPOCH,
                                                    updated: SystemTime::UNIX_EPOCH,
                                                    private: StatePrivate::default(),
                                                };
                                                states.channels.insert(k, chst);
                                            }
                                            ConnectionStateValue::ChannelStateInfo(st6) => {
                                                let recv_count = st6.recv_count.unwrap_or(0);
                                                let recv_bytes = st6.recv_bytes.unwrap_or(0);
                                                let private = StatePrivate {
                                                    status_emit_count: st6.status_emit_count,
                                                };
                                                use crate::ca::conn::ChannelConnectedInfo;
                                                match st6.channel_connected_info {
                                                    ChannelConnectedInfo::Disconnected => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Disconnected,
                                                            // TODO config is stored in two places
                                                            // conf: st6.conf,
                                                            archiving_configuration: st1.config,
                                                            recv_count,
                                                            recv_bytes,
                                                            recv_last: st6.recv_last,
                                                            write_st_last: st6.write_st_last,
                                                            write_mt_last: st6.write_mt_last,
                                                            write_lt_last: st6.write_lt_last,
                                                            updated: st6.stnow,
                                                            private,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Connecting => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Connecting,
                                                            archiving_configuration: st1.config,
                                                            recv_count,
                                                            recv_bytes,
                                                            recv_last: st6.recv_last,
                                                            write_st_last: st6.write_st_last,
                                                            write_mt_last: st6.write_mt_last,
                                                            write_lt_last: st6.write_lt_last,
                                                            updated: st6.stnow,
                                                            private,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Connected => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Connected,
                                                            archiving_configuration: st1.config,
                                                            recv_count,
                                                            recv_bytes,
                                                            recv_last: st6.recv_last,
                                                            write_st_last: st6.write_st_last,
                                                            write_mt_last: st6.write_mt_last,
                                                            write_lt_last: st6.write_lt_last,
                                                            updated: st6.stnow,
                                                            private,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Error => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Error,
                                                            archiving_configuration: st1.config,
                                                            recv_count,
                                                            recv_bytes,
                                                            recv_last: st6.recv_last,
                                                            write_st_last: st6.write_st_last,
                                                            write_mt_last: st6.write_mt_last,
                                                            write_lt_last: st6.write_lt_last,
                                                            updated: st6.stnow,
                                                            private,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Connecting,
                                    archiving_configuration: st1.config,
                                    recv_count: 0,
                                    recv_bytes: 0,
                                    recv_last: SystemTime::UNIX_EPOCH,
                                    write_st_last: SystemTime::UNIX_EPOCH,
                                    write_mt_last: SystemTime::UNIX_EPOCH,
                                    write_lt_last: SystemTime::UNIX_EPOCH,
                                    updated: SystemTime::UNIX_EPOCH,
                                    private: StatePrivate::default(),
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::NoAddress { .. } => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Unreachable,
                                    archiving_configuration: st1.config,
                                    recv_count: 0,
                                    recv_bytes: 0,
                                    recv_last: SystemTime::UNIX_EPOCH,
                                    write_st_last: SystemTime::UNIX_EPOCH,
                                    write_mt_last: SystemTime::UNIX_EPOCH,
                                    write_lt_last: SystemTime::UNIX_EPOCH,
                                    updated: SystemTime::UNIX_EPOCH,
                                    private: StatePrivate::default(),
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::MaybeWrongAddress(..) => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Unreachable,
                                    archiving_configuration: st1.config,
                                    recv_count: 0,
                                    recv_bytes: 0,
                                    recv_last: SystemTime::UNIX_EPOCH,
                                    write_st_last: SystemTime::UNIX_EPOCH,
                                    write_mt_last: SystemTime::UNIX_EPOCH,
                                    write_lt_last: SystemTime::UNIX_EPOCH,
                                    updated: SystemTime::UNIX_EPOCH,
                                    private: StatePrivate::default(),
                                };
                                states.channels.insert(k, chst);
                            }
                        }
                    }
                }
            }
            ChannelStateValue::ToRemove { .. } => {}
        }
    }
    axum::Json(states)
}
