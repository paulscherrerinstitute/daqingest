pub mod conn;
pub mod proto;

use conn::{CaConn, FindIoc};
use err::Error;
use futures_util::stream::{FuturesOrdered, FuturesUnordered};
use futures_util::{StreamExt, TryFutureExt};
use log::*;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;

pub async fn ca_listen_from_file(conf: impl Into<PathBuf>) -> Result<(), Error> {
    let file = OpenOptions::new().read(true).open(conf.into()).await?;
    let mut lines = BufReader::new(file).lines();
    let re = regex::Regex::new(r"^([-:._A-Za-z0-9]+)")?;
    let mut channels = vec![];
    while let Some(line) = lines.next_line().await? {
        if let Some(cs) = re.captures(&line) {
            let m = cs.get(1).unwrap();
            let channel = m.as_str();
            channels.push(channel.to_string());
        }
    }
    let opts = CaConnectOpts { channels };
    ca_connect(opts).await
}

pub struct CaConnectOpts {
    pub channels: Vec<String>,
}

pub async fn ca_connect(opts: CaConnectOpts) -> Result<(), Error> {
    info!("Look up {} channel hosts", opts.channels.len());
    let mut fut_queue = FuturesUnordered::new();
    let mut res2 = vec![];
    let mut chns = VecDeque::from(opts.channels);
    'lo2: loop {
        const MAX_SIMUL: usize = 23;
        while fut_queue.len() < MAX_SIMUL && chns.len() > 0 {
            let ch = chns.pop_front().unwrap();
            let ch2 = ch.clone();
            info!("Start search for {}", ch);
            let fut = FindIoc::new(ch.clone()).map_ok(move |x| (ch2, x));
            let jh = tokio::spawn(fut);
            fut_queue.push(jh);
            if chns.is_empty() {
                break 'lo2;
            }
        }
        while fut_queue.len() >= MAX_SIMUL {
            match fut_queue.next().await {
                Some(item) => {
                    res2.push(item);
                }
                None => break,
            }
        }
    }
    while fut_queue.len() > 0 {
        match fut_queue.next().await {
            Some(item) => {
                res2.push(item);
            }
            None => break,
        }
    }
    info!("Collected {} results", res2.len());
    let mut channels_by_host = BTreeMap::new();
    for item in res2 {
        // TODO should we continue even if some channel gives an error?
        let item = item
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))
            .unwrap_or_else(|e| Err(e));
        match item {
            Ok(item) => {
                info!("Found address  {}  {:?}", item.0, item.1);
                let key = item.1;
                if !channels_by_host.contains_key(&key) {
                    channels_by_host.insert(key, vec![item.0]);
                } else {
                    channels_by_host.get_mut(&key).unwrap().push(item.0);
                }
            }
            Err(e) => {
                error!("Got error: {e:?}");
            }
        };
    }
    for (host, channels) in &channels_by_host {
        info!("Have: {:?}  {:?}", host, channels);
    }
    if false {
        return Ok(());
    }
    let mut conn_jhs = vec![];
    for (host, channels) in channels_by_host {
        let conn_block = async move {
            info!("Create TCP connection to {:?}", (host.addr, host.port));
            let tcp = TcpStream::connect((host.addr, host.port)).await?;
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
