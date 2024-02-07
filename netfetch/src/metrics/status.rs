use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ConnSetCmd;
use async_channel::Sender;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Serialize)]
pub struct ChannelStates {
    channels: BTreeMap<String, ChannelState>,
}

#[derive(Debug, Serialize)]
struct ChannelState {
    ioc_address: Option<SocketAddr>,
    connection: ConnectionState,
    archive_settings: ArchiveSettings,
}

#[derive(Debug, Serialize)]
enum ConnectionState {
    Connecting,
    Unreachable,
    Disconnected,
    Connected,
    Error,
}

#[derive(Debug, Serialize)]
struct ArchiveSettings {
    short_term: Option<String>,
    medium_term: Option<String>,
    long_term: Option<String>,
}

impl ArchiveSettings {
    fn dummy() -> Self {
        Self {
            short_term: None,
            medium_term: None,
            long_term: None,
        }
    }
}

// ChannelStatusesResponse
// BTreeMap<String, ChannelState>
pub async fn channel_states(params: HashMap<String, String>, tx: Sender<CaConnSetEvent>) -> axum::Json<ChannelStates> {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    let limit = params
        .get("limit")
        .map(|x| x.parse().ok())
        .unwrap_or(None)
        .unwrap_or(40);
    let (tx2, rx2) = async_channel::bounded(1);
    let req = ChannelStatusesRequest { name, limit, tx: tx2 };
    let item = CaConnSetEvent::ConnSetCmd(ConnSetCmd::ChannelStatuses(req));
    // TODO handle error
    tx.send(item).await.unwrap();
    let res = rx2.recv().await.unwrap();
    let mut states = ChannelStates {
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
                            archive_settings: ArchiveSettings::dummy(),
                        };
                        states.channels.insert(k, chst);
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        let chst = ChannelState {
                            ioc_address: None,
                            connection: ConnectionState::Connecting,
                            archive_settings: ArchiveSettings::dummy(),
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
                                    archive_settings: ArchiveSettings::dummy(),
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
                                            archive_settings: ArchiveSettings::dummy(),
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
                                                    archive_settings: ArchiveSettings::dummy(),
                                                };
                                                states.channels.insert(k, chst);
                                            }
                                            ConnectionStateValue::ChannelStateInfo(st6) => {
                                                use crate::ca::conn::ChannelConnectedInfo;
                                                match st6.channel_connected_info {
                                                    ChannelConnectedInfo::Disconnected => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Disconnected,
                                                            archive_settings: ArchiveSettings::dummy(),
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Connecting => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Connecting,
                                                            archive_settings: ArchiveSettings::dummy(),
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Connected => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Connected,
                                                            archive_settings: ArchiveSettings::dummy(),
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Error => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Error,
                                                            archive_settings: ArchiveSettings::dummy(),
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
                                    archive_settings: ArchiveSettings::dummy(),
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::NoAddress { .. } => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Unreachable,
                                    archive_settings: ArchiveSettings::dummy(),
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::MaybeWrongAddress(..) => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Unreachable,
                                    archive_settings: ArchiveSettings::dummy(),
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
