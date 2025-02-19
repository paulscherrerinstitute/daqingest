use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ConnSetCmd;
use crate::conf::ChannelConfig;
use crate::conf::ChannelConfigForStatesApi;
use async_channel::Sender;
use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::SystemTime;

autoerr::create_error_v1!(
    name(Error, "StatusError"),
    enum variants {
        Internal,
    },
);

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
    archiving_configuration: ChannelConfigForStatesApi,
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
    #[serde(with = "humantime_serde")]
    pong_last: Option<SystemTime>,
    private: StatePrivate,
}

impl ChannelState {
    fn connecting(config: ChannelConfig) -> Self {
        Self::connecting_addr(config, None, ConnectionState::Connecting)
    }

    fn connecting_addr(config: ChannelConfig, ioc_address: Option<SocketAddr>, connst: ConnectionState) -> Self {
        Self {
            private: StatePrivate::default(config.config_file_basename()),
            ioc_address,
            connection: connst,
            archiving_configuration: config.into(),
            recv_count: 0,
            recv_bytes: 0,
            recv_last: SystemTime::UNIX_EPOCH,
            write_st_last: SystemTime::UNIX_EPOCH,
            write_mt_last: SystemTime::UNIX_EPOCH,
            write_lt_last: SystemTime::UNIX_EPOCH,
            updated: SystemTime::UNIX_EPOCH,
            pong_last: None,
        }
    }

    fn with_chst(config: ChannelConfig, chst: crate::ca::conn::ChannelStateInfo) -> Self {
        let private = StatePrivate {
            status_emit_count: chst.status_emit_count,
            config_file_basename: config.config_file_basename().into(),
        };
        let connst = {
            use crate::ca::conn::ChannelConnectedInfo::*;
            match chst.channel_connected_info {
                Disconnected => ConnectionState::Disconnected,
                Connecting => ConnectionState::Connecting,
                Connected => ConnectionState::Connected,
                Error => ConnectionState::Error,
            }
        };
        Self {
            ioc_address: Some(SocketAddr::V4(chst.addr)),
            connection: connst,
            // TODO config is stored in two places
            // conf: chst.conf,
            archiving_configuration: config.into(),
            recv_count: chst.recv_count.unwrap_or(0),
            recv_bytes: chst.recv_bytes.unwrap_or(0),
            recv_last: chst.recv_last,
            write_st_last: chst.write_st_last,
            write_mt_last: chst.write_mt_last,
            write_lt_last: chst.write_lt_last,
            updated: chst.stnow,
            pong_last: chst.pong_last,
            private,
        }
    }
}

#[derive(Debug, Serialize)]
struct StatePrivate {
    status_emit_count: u64,
    config_file_basename: String,
}

impl StatePrivate {
    fn default(config_file_basename: &str) -> Self {
        Self {
            status_emit_count: 0,
            config_file_basename: config_file_basename.into(),
        }
    }
}

fn system_time_epoch(x: &SystemTime) -> bool {
    *x == SystemTime::UNIX_EPOCH
}

#[derive(Debug, Serialize)]
enum Unreachable {
    NoAddress,
    MaybeWrongAddress,
}

#[derive(Debug, Serialize)]
enum ConnectionState {
    Connecting,
    Unreachable(Unreachable),
    Disconnected,
    Connected,
    Error,
}

pub async fn error_handler_test() -> Result<axum::Json<ChannelStates>, axum::Json<String>> {
    Err(axum::Json(format!("test error message")))
}

// ChannelStatusesResponse
// BTreeMap<String, ChannelState>
pub async fn channel_states(
    params: HashMap<String, String>,
    tx: Sender<CaConnSetEvent>,
) -> Result<axum::Json<ChannelStates>, axum::Json<String>> {
    match channel_states_try(params, tx).await {
        Ok(x) => Ok(x),
        Err(e) => Err(axum::Json(e.to_string())),
    }
}

async fn channel_states_try(
    params: HashMap<String, String>,
    tx: Sender<CaConnSetEvent>,
) -> Result<axum::Json<ChannelStates>, Error> {
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
                        let chst = ChannelState::connecting(st1.config);
                        states.channels.insert(k, chst);
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        let chst = ChannelState::connecting(st1.config);
                        states.channels.insert(k, chst);
                    }
                    ActiveChannelState::WithStatusSeriesId(st3) => {
                        use crate::ca::statemap::WithStatusSeriesIdStateInner;
                        match st3.inner {
                            WithStatusSeriesIdStateInner::AddrSearchPending { .. } => {
                                let chst = ChannelState::connecting(st1.config);
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::WithAddress { addr, state: st4 } => {
                                use crate::ca::statemap::WithAddressState;
                                let addr2 = SocketAddr::V4(addr);
                                match st4 {
                                    WithAddressState::Unassigned { .. } => {
                                        let chst = ChannelState::connecting_addr(
                                            st1.config,
                                            Some(addr2),
                                            ConnectionState::Connecting,
                                        );
                                        states.channels.insert(k, chst);
                                    }
                                    WithAddressState::Assigned(st5) => {
                                        use crate::ca::statemap::ConnectionStateValue;
                                        match st5.value {
                                            ConnectionStateValue::Unknown => {
                                                let chst = ChannelState::connecting_addr(
                                                    st1.config,
                                                    Some(addr2),
                                                    ConnectionState::Connecting,
                                                );
                                                states.channels.insert(k, chst);
                                            }
                                            ConnectionStateValue::ChannelStateInfo(st6) => {
                                                let chst = ChannelState::with_chst(st1.config, st6);
                                                states.channels.insert(k, chst);
                                            }
                                        }
                                    }
                                }
                            }
                            WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                                let chst = ChannelState::connecting(st1.config);
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::NoAddress { .. } => {
                                let chst = ChannelState::connecting_addr(
                                    st1.config,
                                    None,
                                    ConnectionState::Unreachable(Unreachable::NoAddress),
                                );
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::MaybeWrongAddress(..) => {
                                let chst = ChannelState::connecting_addr(
                                    st1.config,
                                    None,
                                    ConnectionState::Unreachable(Unreachable::MaybeWrongAddress),
                                );
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::UnassigningForConfigChange(_) => {
                                let chst = ChannelState::connecting_addr(st1.config, None, ConnectionState::Connecting);
                                states.channels.insert(k, chst);
                            }
                            WithStatusSeriesIdStateInner::AddrSearchPlanned { .. } => {
                                let chst = ChannelState::connecting(st1.config);
                                states.channels.insert(k, chst);
                            }
                        }
                    }
                }
            }
            ChannelStateValue::ToRemove { .. } => {}
            ChannelStateValue::InitDummy { .. } => {}
        }
    }
    Ok(axum::Json(states))
}
