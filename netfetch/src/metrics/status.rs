use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ConnSetCmd;
use crate::conf::ChannelConfig;
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
    archiving_configuration: ChannelConfig,
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
                            archiving_configuration: st1.config,
                        };
                        states.channels.insert(k, chst);
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        let chst = ChannelState {
                            ioc_address: None,
                            connection: ConnectionState::Connecting,
                            archiving_configuration: st1.config,
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
                                                            // TODO config is stored in two places
                                                            // conf: st6.conf,
                                                            archiving_configuration: st1.config,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Connecting => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Connecting,
                                                            archiving_configuration: st1.config,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Connected => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Connected,
                                                            archiving_configuration: st1.config,
                                                        };
                                                        states.channels.insert(k, chst);
                                                    }
                                                    ChannelConnectedInfo::Error => {
                                                        let chst = ChannelState {
                                                            ioc_address: Some(SocketAddr::V4(addr)),
                                                            connection: ConnectionState::Error,
                                                            archiving_configuration: st1.config,
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
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::NoAddress { .. } => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Unreachable,
                                    archiving_configuration: st1.config,
                                };
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::MaybeWrongAddress(..) => {
                                let chst = ChannelState {
                                    ioc_address: None,
                                    connection: ConnectionState::Unreachable,
                                    archiving_configuration: st1.config,
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
