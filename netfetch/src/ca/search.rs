use crate::ca::findioc::FindIocStream;
use crate::ca::{parse_config, ListenFromFileOpts};
use err::Error;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn resolve_address(addr_str: &str) -> Result<SocketAddrV4, Error> {
    const PORT_DEFAULT: u16 = 5064;
    let ac = match addr_str.parse::<SocketAddrV4>() {
        Ok(k) => k,
        Err(_) => {
            trace!("can not parse {addr_str} as SocketAddrV4");
            match addr_str.parse::<Ipv4Addr>() {
                Ok(k) => SocketAddrV4::new(k, PORT_DEFAULT),
                Err(e) => {
                    trace!("can not parse {addr_str} as Ipv4Addr");
                    let (hostname, port) = if addr_str.contains(":") {
                        let mut it = addr_str.split(":");
                        (
                            it.next().unwrap().to_string(),
                            it.next().unwrap().parse::<u16>().unwrap(),
                        )
                    } else {
                        (addr_str.to_string(), PORT_DEFAULT)
                    };
                    match tokio::net::lookup_host(format!("{}:33", hostname.clone())).await {
                        Ok(k) => {
                            let vs: Vec<_> = k
                                .filter_map(|x| match x {
                                    SocketAddr::V4(k) => Some(k),
                                    SocketAddr::V6(_) => {
                                        error!("TODO ipv6 support");
                                        None
                                    }
                                })
                                .collect();
                            if let Some(k) = vs.first() {
                                let mut k = *k;
                                k.set_port(port);
                                k
                            } else {
                                return Err(e.into());
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

pub async fn ca_search(opts: ListenFromFileOpts) -> Result<(), Error> {
    let facility = "scylla";
    let opts = parse_config(opts.config).await?;
    let d = Database {
        name: opts.pgconf.name.clone(),
        host: opts.pgconf.host.clone(),
        port: opts.pgconf.port.clone(),
        user: opts.pgconf.user.clone(),
        pass: opts.pgconf.pass.clone(),
    };
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO join pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);
    let qu_select = pg_client
        .prepare("select addr from ioc_by_channel where facility = $1 and channel = $2 and searchaddr = $3")
        .await
        .unwrap();
    let qu_insert = {
        const TEXT: tokio_postgres::types::Type = tokio_postgres::types::Type::TEXT;
        pg_client
            .prepare_typed(
                "insert into ioc_by_channel (facility, channel, searchaddr, addr) values ($1, $2, $3, $4) on conflict do nothing",
                &[TEXT, TEXT, TEXT, TEXT],
            )
            .await
            .unwrap()
    };
    let qu_update = pg_client
        .prepare("update ioc_by_channel set addr = $4, tsmod = now(), modcount = modcount + 1 where facility = $1 and channel = $2 and searchaddr = $3")
        .await
        .unwrap();
    let mut addrs = vec![];
    for s in &opts.search {
        match resolve_address(s).await {
            Ok(addr) => {
                info!("resolved {s} as {addr}");
                addrs.push(addr);
            }
            Err(e) => {
                error!("can not resolve {s} {e}");
            }
        }
    }
    let gw_addrs = {
        // Try to blacklist..
        // TODO if it helps, add a config option for it.
        let gateways = [
            "sf-cagw",
            "saresa-cagw",
            "saresb-cagw",
            "saresc-cagw",
            "satesd-cagw",
            "satese-cagw",
            "satesf-cagw",
        ];
        let mut gw_addrs = vec![];
        for s in gateways {
            match resolve_address(s).await {
                Ok(addr) => {
                    info!("resolved {s} as {addr}");
                    gw_addrs.push(addr);
                }
                Err(e) => {
                    error!("can not resolve {s} {e}");
                }
            }
        }
        gw_addrs
    };
    info!("Blacklisting {} gateways", gw_addrs.len());
    let mut finder = FindIocStream::new(addrs);
    for ch in &opts.channels {
        finder.push(ch.into());
    }
    let mut ts_last = Instant::now();
    loop {
        let ts_now = Instant::now();
        if ts_now.duration_since(ts_last) >= Duration::from_millis(1000) {
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
                if &item.src == a2 {
                    do_block = true;
                    warn!("gateways responded to search");
                }
            }
            if let Some(a1) = item.addr.as_ref() {
                for a2 in &gw_addrs {
                    if a1 == a2 {
                        do_block = true;
                        warn!("do not use gateways as ioc address");
                    }
                }
            }
            if do_block {
                info!("blocking {item:?}");
            } else {
                info!("using {item:?}");
                let srcaddr = item.src.to_string();
                let addr = item.addr.map(|x| x.to_string()).unwrap_or(String::new());
                let rows = pg_client
                    .query(&qu_select, &[&facility, &item.channel, &srcaddr])
                    .await
                    .unwrap();
                if rows.is_empty() {
                    info!("insert {item:?}");
                    pg_client
                        .execute(&qu_insert, &[&facility, &item.channel, &srcaddr, &addr])
                        .await
                        .unwrap();
                } else {
                    info!("update {item:?}");
                    let addr2: &str = rows[0].get(0);
                    if addr2 != addr {
                        pg_client
                            .execute(&qu_update, &[&facility, &item.channel, &srcaddr, &addr])
                            .await
                            .unwrap();
                    }
                }
            }
        }
    }
    Ok(())
}
