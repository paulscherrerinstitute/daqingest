use crate::ca::findioc::FindIocStream;
use crate::conf::CaIngestOpts;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::conn::PgClient;
use dbpg::iocindex::IocItem;
use dbpg::iocindex::IocSearchIndexWorker;
use err::Error;
use futures_util::StreamExt;
use log::*;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

const DB_WORKER_COUNT: usize = 4;

async fn resolve_address(addr_str: &str) -> Result<SocketAddr, Error> {
    const PORT_DEFAULT: u16 = 5064;
    let ac = match addr_str.parse::<SocketAddr>() {
        Ok(k) => k,
        Err(_) => {
            trace!("can not parse {addr_str} as SocketAddr");
            match addr_str.parse::<IpAddr>() {
                Ok(k) => SocketAddr::new(k, PORT_DEFAULT),
                Err(_e) => {
                    trace!("can not parse {addr_str} as IpAddr");
                    let (hostname, port) = if addr_str.contains(":") {
                        let mut it = addr_str.split(":");
                        (
                            it.next().unwrap().to_string(),
                            it.next().unwrap().parse::<u16>().unwrap(),
                        )
                    } else {
                        (addr_str.to_string(), PORT_DEFAULT)
                    };
                    let host = format!("{}:{}", hostname.clone(), port);
                    match tokio::net::lookup_host(host.clone()).await {
                        Ok(mut k) => {
                            if let Some(k) = k.next() {
                                k
                            } else {
                                return Err(Error::with_msg_no_trace(format!("can not lookup host {host}")));
                            }
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    };
    Ok(ac)
}

struct DbUpdateWorker {
    jh: JoinHandle<()>,
}

impl DbUpdateWorker {
    async fn new(rx: Receiver<IocItem>, backend: String, pg: PgClient) -> Result<Self, Error> {
        let worker = IocSearchIndexWorker::prepare(rx, backend, pg)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let jh = tokio::spawn(async move { worker.worker().await });
        Ok(Self { jh })
    }
}

pub async fn ca_search(opts: CaIngestOpts, channels: &Vec<String>) -> Result<(), Error> {
    info!("ca_search begin");
    let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config())
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    dbpg::schema::schema_check(&pg)
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    let mut addrs = Vec::new();
    for s in opts.search() {
        match resolve_address(s).await {
            Ok(addr) => {
                trace!("resolved {s} as {addr}");
                addrs.push(addr);
            }
            Err(e) => {
                error!("can not resolve {s} {e}");
            }
        }
    }
    let gw_addrs = {
        let mut gw_addrs = Vec::new();
        for s in opts.search_blacklist() {
            match resolve_address(s).await {
                Ok(addr) => {
                    trace!("resolved {s} as {addr}");
                    gw_addrs.push(addr);
                }
                Err(e) => {
                    warn!("can not resolve {s} {e}");
                }
            }
        }
        gw_addrs
    };
    let addrs = addrs
        .into_iter()
        .filter_map(|x| match x {
            SocketAddr::V4(x) => Some(x),
            SocketAddr::V6(_) => {
                error!("TODO check ipv6 support for IOCs");
                None
            }
        })
        .collect();
    let mut finder = FindIocStream::new(addrs, Duration::from_millis(800), 20, 4);
    finder.set_stop_on_empty_queue();
    for ch in channels.iter() {
        finder.push(ch.into());
    }

    let (dbtx, dbrx) = async_channel::bounded(64);

    let mut dbworkers = Vec::new();
    for _ in 0..DB_WORKER_COUNT {
        let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config())
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let w = DbUpdateWorker::new(dbrx.clone(), opts.backend().into(), pg).await?;
        dbworkers.push(w);
    }
    drop(dbrx);
    let dbtx: Sender<_> = dbtx;

    let mut ts_last = Instant::now();
    'outer: loop {
        let ts_now = Instant::now();
        if ts_now.duration_since(ts_last) >= Duration::from_millis(2000) {
            ts_last = ts_now;
            info!("{}", finder.quick_state());
        }
        let k = tokio::time::timeout(Duration::from_millis(1500), finder.next()).await;
        let item = match k {
            Ok(Some(k)) => k,
            Ok(None) => {
                info!("Search stream exhausted");
                break;
            }
            Err(_) => {
                continue;
            }
        };
        let item = match item {
            Ok(k) => k,
            Err(e) => {
                error!("ca_search {e:?}");
                continue;
            }
        };
        for item in item {
            let mut do_block = false;
            for a2 in &gw_addrs {
                if let Some(response_addr) = &item.response_addr {
                    if &SocketAddr::V4(*response_addr) == a2 {
                        do_block = true;
                        warn!("gateways responded to search");
                    }
                }
            }
            if let Some(a1) = item.addr.as_ref() {
                for a2 in &gw_addrs {
                    if &SocketAddr::V4(*a1) == a2 {
                        do_block = true;
                        warn!("do not use gateways as ioc address");
                    }
                }
            }
            if do_block {
                info!("blacklisting {item:?}");
            } else {
                let item = IocItem::new(item.channel, item.response_addr, item.addr, item.dt);
                match dbtx.send(item).await {
                    Ok(_) => {}
                    Err(_) => {
                        error!("dbtx broken");
                        break 'outer;
                    }
                }
            }
        }
    }
    drop(dbtx);
    for w in dbworkers {
        match w.jh.await {
            Ok(_) => {}
            Err(e) => {
                error!("see error while join on db worker: {e}");
            }
        }
    }
    info!("all done");
    Ok(())
}
