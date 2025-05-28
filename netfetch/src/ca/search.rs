use crate::ca::findioc::FindIocStream;
use crate::conf::CaIngestOpts;
use async_channel::Receiver;
use async_channel::Sender;
use futures_util::StreamExt;
use log::*;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::time::Duration;
use taskrun::tokio;
use tokio::task::JoinHandle;

autoerr::create_error_v1!(
    name(Error, "IocSearch"),
    enum variants {
        LookupFailure(String),
        IO(#[from] std::io::Error),
    },
);

async fn resolve_address(addr_str: &str) -> Result<SocketAddr, Error> {
    const PORT_DEFAULT: u16 = 5064;
    let ac = match addr_str.parse::<SocketAddr>() {
        Ok(k) => k,
        Err(_) => {
            trace!("can not parse {} as SocketAddr", addr_str);
            match addr_str.parse::<IpAddr>() {
                Ok(k) => SocketAddr::new(k, PORT_DEFAULT),
                Err(_e) => {
                    trace!("can not parse {} as IpAddr", addr_str);
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
                        Ok(k) => k
                            .into_iter()
                            .filter(|addr| if let SocketAddr::V4(_) = addr { true } else { false })
                            .next()
                            .ok_or_else(|| Error::LookupFailure(host))?,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    };
    Ok(ac)
}

pub async fn ca_search_workers_start(
    opts: &CaIngestOpts,
) -> Result<
    (
        Sender<String>,
        Receiver<Result<VecDeque<crate::ca::findioc::FindIocRes>, crate::ca::findioc::Error>>,
        JoinHandle<Result<(), Error>>,
    ),
    Error,
> {
    let (search_tgts, blacklist) = search_tgts_from_opts(&opts).await?;
    let batch_run_max = Duration::from_millis(800);
    let (inp_tx, inp_rx) = async_channel::bounded(256);
    let (out_tx, out_rx) = async_channel::bounded(256);
    let finder = FindIocStream::new(inp_rx, search_tgts, blacklist, batch_run_max, 20, 16);
    let jh = taskrun::spawn(finder_run(finder, out_tx));
    Ok((inp_tx, out_rx, jh))
}

async fn search_tgts_from_opts(opts: &CaIngestOpts) -> Result<(Vec<SocketAddrV4>, Vec<SocketAddrV4>), Error> {
    let mut addrs = Vec::new();
    for s in opts.search() {
        match resolve_address(s).await {
            Ok(addr) => {
                trace!("resolved {} as {}", s, addr);
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
                error!("can not resolve {} {}", s, e);
            }
        }
    }
    let blacklist = {
        let mut addrs = Vec::new();
        for s in opts.search_blacklist() {
            match resolve_address(s).await {
                Ok(addr) => {
                    trace!("resolved {} as {}", s, addr);
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
                    warn!("can not resolve {} {}", s, e);
                }
            }
        }
        addrs
    };
    Ok((addrs, blacklist))
}

async fn finder_run(
    finder: FindIocStream,
    tx: Sender<Result<VecDeque<crate::ca::findioc::FindIocRes>, crate::ca::findioc::Error>>,
) -> Result<(), Error> {
    let mut finder = Box::pin(finder);
    while let Some(item) = finder.next().await {
        if let Err(_) = tx.send(item).await {
            break;
        }
    }
    trace!("finder_run done");
    Ok(())
}
