pub mod conn;
pub mod proto;

use conn::{CaConn, FindIoc};
use err::Error;
use futures_util::stream::FuturesUnordered;
use futures_util::{StreamExt, TryFutureExt};
use log::*;
use scylla::batch::Consistency;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session as ScySession;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::task::JoinError;
use tokio::time::error::Elapsed;

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

async fn unwrap_search_result(
    item: Result<Result<Result<(String, SocketAddrV4, Option<SocketAddrV4>), Error>, Elapsed>, JoinError>,
    scy: &ScySession,
    qu: &PreparedStatement,
) -> Result<(String, SocketAddrV4, Option<SocketAddrV4>), Error> {
    match item {
        Ok(k) => match k {
            Ok(k) => match k {
                Ok(h) => match h.2 {
                    Some(k) => {
                        scy.execute(qu, (&h.0, format!("{:?}", h.1), format!("{:?}", k)))
                            .await
                            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
                        Ok(h)
                    }
                    None => Ok(h),
                },
                Err(e) => {
                    error!("bad search {e:?}");
                    Err(e)
                }
            },
            Err(e) => {
                error!("Elapsed");
                Err(Error::with_msg_no_trace(format!("{e:?}")))
            }
        },
        Err(e) => {
            error!("JoinError");
            Err(Error::with_msg_no_trace(format!("{e:?}")))
        }
    }
}

pub async fn ca_search(opts: ListenFromFileOpts) -> Result<(), Error> {
    let opts = parse_config(opts.config).await?;
    let scy = scylla::SessionBuilder::new()
        .known_node("sf-nube-11:19042")
        .default_consistency(Consistency::Quorum)
        .use_keyspace("ks1", true)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu = scy
        .prepare("insert into ioc_by_channel (channel, searchaddr, addr) values (?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    const PORT_DEFAULT: u16 = 5064;
    info!("Look up {} channel hosts", opts.channels.len());
    let mut fut_queue = FuturesUnordered::new();
    let mut res2 = vec![];
    let mut chns = VecDeque::new();
    for ch in &opts.channels {
        for ac in &opts.search {
            chns.push_back((ch.clone(), ac.clone()));
        }
    }
    let max_simul = opts.max_simul;
    let timeout = opts.timeout;
    let mut ix1 = 0;
    'lo2: loop {
        while fut_queue.len() < max_simul && chns.len() > 0 {
            let (ch, ac) = chns.pop_front().unwrap();
            let ch2 = ch.clone();
            let ac = match ac.parse::<SocketAddrV4>() {
                Ok(k) => k,
                Err(_) => match ac.parse::<Ipv4Addr>() {
                    Ok(k) => SocketAddrV4::new(k, PORT_DEFAULT),
                    Err(e) => match tokio::net::lookup_host(&ac).await {
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
                                error!("Can not understand name for {:?}  {:?}", ac, vs);
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
            ix1 += 1;
            if ix1 >= 500 {
                info!("Start search for {} {}", ch, ac);
                ix1 = 0;
            }
            let fut = FindIoc::new(ch.clone(), Ipv4Addr::UNSPECIFIED, ac.clone(), timeout)
                .map_ok(move |x| (ch2, ac.clone(), x));
            let fut = tokio::time::timeout(Duration::from_millis(timeout + 1000), fut);
            let jh = tokio::spawn(fut);
            fut_queue.push(jh);
            if chns.is_empty() {
                break 'lo2;
            }
        }
        while fut_queue.len() >= max_simul {
            match fut_queue.next().await {
                Some(item) => {
                    let item = unwrap_search_result(item, &scy, &qu).await;
                    res2.push(item);
                }
                None => break,
            }
        }
    }
    while fut_queue.len() > 0 {
        match fut_queue.next().await {
            Some(item) => {
                let item = unwrap_search_result(item, &scy, &qu).await;
                res2.push(item);
            }
            None => break,
        }
    }
    info!("Collected {} results", res2.len());
    let mut channels_set = BTreeMap::new();
    let mut channels_by_host = BTreeMap::new();
    for item in res2 {
        // TODO should we continue even if some channel gives an error or can not be located?
        match item {
            Ok((ch, ac, Some(addr))) => {
                info!("Found address  {}  {:?}  {:?}", ch, ac, addr);
                channels_set.insert(ch.clone(), true);
                let key = addr;
                if !channels_by_host.contains_key(&key) {
                    channels_by_host.insert(key, vec![ch]);
                } else {
                    channels_by_host.get_mut(&key).unwrap().push(ch);
                }
            }
            Ok((_, _, None)) => {}
            Err(e) => {
                error!("Error in res2 list: {e:?}");
            }
        };
    }
    for (host, channels) in &channels_by_host {
        info!("Have: {:?}  {:?}", host, channels.len());
    }
    for ch in &opts.channels {
        if !channels_set.contains_key(ch) {
            error!("Could not locate {ch:?}");
        }
    }
    Ok(())
}

pub async fn ca_connect(opts: ListenFromFileOpts) -> Result<(), Error> {
    let opts = parse_config(opts.config).await?;
    let scy = scylla::SessionBuilder::new()
        .known_node("sf-nube-11:19042")
        .default_consistency(Consistency::Quorum)
        .use_keyspace("ks1", true)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_find_addr = scy
        .prepare("select addr from ioc_by_channel where channel = ?")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let mut channels_by_host = BTreeMap::new();
    for (ix, ch) in opts.channels.iter().enumerate() {
        let res = scy
            .execute(&qu_find_addr, (ch,))
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
    let mut conn_jhs = vec![];
    for (host, channels) in channels_by_host {
        let conn_block = async move {
            info!("Create TCP connection to {:?}", (host.ip(), host.port()));
            let tcp = TcpStream::connect((host.ip().clone(), host.port())).await?;
            let mut conn = CaConn::new(tcp);
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
