pub mod conn;
pub mod proto;
pub mod store;

use self::conn::FindIocStream;
use self::store::DataStore;
use crate::zmtp::ErrConv;
use conn::CaConn;
use err::Error;
use futures_util::StreamExt;
use log::*;
use scylla::batch::Consistency;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
struct ChannelConfig {
    channels: Vec<String>,
    search: Vec<String>,
    addr_bind: Ipv4Addr,
    addr_conn: Ipv4Addr,
    whitelist: String,
    blacklist: String,
    max_simul: Option<usize>,
    timeout: Option<u64>,
    #[serde(default)]
    abort_after_search: u32,
}

pub struct ListenFromFileOpts {
    pub config: PathBuf,
}

pub async fn parse_config(config: PathBuf) -> Result<CaConnectOpts, Error> {
    let mut file = OpenOptions::new().read(true).open(config).await?;
    let mut buf = vec![];
    file.read_to_end(&mut buf).await?;
    let mut conf: ChannelConfig =
        serde_yaml::from_slice(&buf).map_err(|e| Error::with_msg_no_trace(format!("{:?}", e)))?;
    let re1 = regex::Regex::new(&conf.whitelist)?;
    let re2 = regex::Regex::new(&conf.blacklist)?;
    conf.channels = conf
        .channels
        .into_iter()
        .filter(|ch| {
            if let Some(_cs) = re1.captures(&ch) {
                //let m = cs.get(1).unwrap();
                true
            } else if re2.is_match(&ch) {
                false
            } else {
                true
            }
        })
        .collect();
    Ok(CaConnectOpts {
        channels: conf.channels,
        search: conf.search,
        addr_bind: conf.addr_bind,
        addr_conn: conf.addr_conn,
        max_simul: conf.max_simul.unwrap_or(113),
        timeout: conf.timeout.unwrap_or(2000),
        abort_after_search: conf.abort_after_search,
    })
}

pub struct CaConnectOpts {
    pub channels: Vec<String>,
    pub search: Vec<String>,
    pub addr_bind: Ipv4Addr,
    pub addr_conn: Ipv4Addr,
    pub max_simul: usize,
    pub timeout: u64,
    pub abort_after_search: u32,
}

async fn resolve_address(addr_str: &str) -> Result<SocketAddrV4, Error> {
    const PORT_DEFAULT: u16 = 5064;
    let ac = match addr_str.parse::<SocketAddrV4>() {
        Ok(k) => k,
        Err(_) => match addr_str.parse::<Ipv4Addr>() {
            Ok(k) => SocketAddrV4::new(k, PORT_DEFAULT),
            Err(e) => match tokio::net::lookup_host(&addr_str).await {
                Ok(k) => {
                    let vs: Vec<_> = k
                        .filter_map(|x| match x {
                            SocketAddr::V4(k) => Some(k),
                            SocketAddr::V6(_) => None,
                        })
                        .collect();
                    if let Some(k) = vs.first() {
                        *k
                    } else {
                        error!("Can not understand name for {:?}  {:?}", addr_str, vs);
                        return Err(e.into());
                    }
                }
                Err(e) => {
                    error!("{e:?}");
                    return Err(e.into());
                }
            },
        },
    };
    Ok(ac)
}

pub async fn ca_search_2(opts: ListenFromFileOpts) -> Result<(), Error> {
    let facility = "scylla";
    let opts = parse_config(opts.config).await?;
    let scy = scylla::SessionBuilder::new()
        .known_node("sf-nube-11:19042")
        .default_consistency(Consistency::Quorum)
        .use_keyspace("ks1", true)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu = scy
        .prepare("insert into ioc_by_channel (facility, channel, searchaddr, addr) values (?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let mut addrs = vec![];
    for s in &opts.search {
        let x = resolve_address(s).await?;
        addrs.push(x);
    }
    let mut finder = FindIocStream::new(addrs);
    for ch in &opts.channels {
        finder.push(ch.into());
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_millis(100000000))
        .unwrap();
    let mut i1 = 0;
    loop {
        let k = tokio::time::timeout_at(deadline, finder.next()).await;
        let item = match k {
            Ok(Some(k)) => k,
            Ok(None) => {
                info!("Search stream exhausted");
                break;
            }
            Err(_) => {
                warn!("timed out");
                break;
            }
        };
        let item = match item {
            Ok(k) => k,
            Err(e) => {
                error!("ca_search_2 {e:?}");
                continue;
            }
        };
        for item in item {
            scy.execute(
                &qu,
                (
                    facility,
                    &item.channel,
                    item.src.to_string(),
                    item.addr.map(|x| x.to_string()),
                ),
            )
            .await
            .err_conv()?;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        i1 += 1;
        if i1 > 500 {
            i1 = 0;
            info!("{}", finder.quick_state());
        }
    }
    Ok(())
}

pub async fn ca_connect(opts: ListenFromFileOpts) -> Result<(), Error> {
    let facility = "scylla";
    let opts = parse_config(opts.config).await?;
    let scy = scylla::SessionBuilder::new()
        .known_node("sf-nube-11:19042")
        .default_consistency(Consistency::Quorum)
        .use_keyspace("ks1", true)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let scy = Arc::new(scy);
    info!("FIND IOCS");
    let qu_find_addr = scy
        .prepare("select addr from ioc_by_channel where facility = ? and channel = ?")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let mut channels_by_host = BTreeMap::new();
    for (ix, ch) in opts.channels.iter().enumerate() {
        let res = scy
            .execute(&qu_find_addr, (facility, ch))
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        if res.rows_num().unwrap() == 0 {
            error!("can not find address of channel {}", ch);
        } else {
            let (addr,) = res.first_row_typed::<(String,)>().unwrap();
            let addr: SocketAddrV4 = addr.parse().unwrap();
            if ix % 500 == 0 {
                info!("{}  {}  {:?}", ix, ch, addr);
            }
            if !channels_by_host.contains_key(&addr) {
                channels_by_host.insert(addr, vec![ch.to_string()]);
            } else {
                channels_by_host.get_mut(&addr).unwrap().push(ch.to_string());
            }
        }
    }
    if opts.abort_after_search == 1 {
        return Ok(());
    }
    info!("CONNECT TO HOSTS");
    let data_store = Arc::new(DataStore::new(scy.clone()).await?);
    let mut conn_jhs = vec![];
    for (host, channels) in channels_by_host {
        let data_store = data_store.clone();
        let conn_block = async move {
            info!("Create TCP connection to {:?}", (host.ip(), host.port()));
            let tcp = TcpStream::connect((host.ip().clone(), host.port())).await?;
            let mut conn = CaConn::new(tcp, data_store.clone());
            for c in channels {
                conn.channel_add(c);
            }
            while let Some(item) = conn.next().await {
                match item {
                    Ok(k) => {
                        trace!("CaConn gives item: {k:?}");
                    }
                    Err(e) => {
                        error!("CaConn gives error: {e:?}");
                        break;
                    }
                }
            }
            Ok::<_, Error>(())
        };
        let jh = tokio::spawn(conn_block);
        conn_jhs.push(jh);
    }
    for jh in conn_jhs {
        match jh.await {
            Ok(k) => match k {
                Ok(_) => {}
                Err(e) => {
                    error!("{e:?}");
                }
            },
            Err(e) => {
                error!("{e:?}");
            }
        }
    }
    Ok(())
}
