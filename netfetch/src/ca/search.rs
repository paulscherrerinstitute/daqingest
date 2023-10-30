use super::findioc::FindIocRes;
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
use stats::IocFinderStats;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
use taskrun::tokio;
use tokio::task::JoinHandle;

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

#[cfg(DISABLED)]
pub async fn ca_search(opts: CaIngestOpts, channels: &Vec<String>) -> Result<(), Error> {
    info!("ca_search begin");
    let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config())
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    dbpg::schema::schema_check(&pg)
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;

    let (search_tgts, blacklist) = search_tgts_from_opts(&opts).await?;

    // let mut finder = FindIocStream::new(search_tgts, Duration::from_millis(800), 20, 16);
    // finder.set_stop_on_empty_queue();
    // for ch in channels.iter() {
    //     finder.push(ch.into());
    // }

    const DB_WORKER_COUNT: usize = 1;
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

pub async fn ca_search_workers_start(
    opts: &CaIngestOpts,
    stats: Arc<IocFinderStats>,
) -> Result<
    (
        Sender<String>,
        Receiver<Result<VecDeque<FindIocRes>, Error>>,
        JoinHandle<Result<(), Error>>,
        Vec<JoinHandle<Result<(), Error>>>,
    ),
    Error,
> {
    let (search_tgts, blacklist) = search_tgts_from_opts(&opts).await?;
    let batch_run_max = Duration::from_millis(800);
    let (inp_tx, inp_rx) = async_channel::bounded(256);
    let (out_tx, out_rx) = async_channel::bounded(256);
    let finder = FindIocStream::new(inp_rx, search_tgts, blacklist, batch_run_max, 20, 16, stats);
    let jh = taskrun::spawn(finder_run(finder, out_tx));
    let jhs = Vec::new();
    Ok((inp_tx, out_rx, jh, jhs))
}

async fn search_tgts_from_opts(opts: &CaIngestOpts) -> Result<(Vec<SocketAddrV4>, Vec<SocketAddrV4>), Error> {
    let mut addrs = Vec::new();
    for s in opts.search() {
        match resolve_address(s).await {
            Ok(addr) => {
                trace!("resolved {s} as {addr}");
                match addr {
                    SocketAddr::V4(addr) => {
                        addrs.push(addr);
                    }
                    SocketAddr::V6(_) => {
                        error!("no ipv6 for epics");
                    }
                }
            }
            Err(e) => {
                error!("can not resolve {s} {e}");
            }
        }
    }
    let blacklist = {
        let mut addrs = Vec::new();
        for s in opts.search_blacklist() {
            match resolve_address(s).await {
                Ok(addr) => {
                    trace!("resolved {s} as {addr}");
                    match addr {
                        SocketAddr::V4(addr) => {
                            addrs.push(addr);
                        }
                        SocketAddr::V6(_) => {
                            error!("no ipv6 for epics");
                        }
                    }
                }
                Err(e) => {
                    warn!("can not resolve {s} {e}");
                }
            }
        }
        addrs
    };
    Ok((addrs, blacklist))
}

async fn finder_run(finder: FindIocStream, tx: Sender<Result<VecDeque<FindIocRes>, Error>>) -> Result<(), Error> {
    let mut finder = Box::pin(finder);
    while let Some(item) = finder.next().await {
        if let Err(_) = tx.send(item).await {
            break;
        }
    }
    debug!("finder_run done");
    Ok(())
}
