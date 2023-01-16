use super::findioc::FindIocRes;
use crate::ca::findioc::FindIocStream;
use crate::conf::CaIngestOpts;
use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;

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
    fn new(rx: Receiver<FindIocRes>, backend: String, database: Database) -> Self {
        let jh = tokio::spawn(Self::worker(rx, backend, database));
        Self { jh }
    }

    async fn worker(rx: Receiver<FindIocRes>, backend: String, database: Database) {
        let d = &database;
        let (pg_client, pg_conn) = tokio_postgres::connect(
            &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
            tokio_postgres::tls::NoTls,
        )
        .await
        .unwrap();
        let (pgconn_out_tx, pgconn_out_rx) = async_channel::bounded(16);
        tokio::spawn(async move {
            if let Err(e) = pgconn_out_tx.send(pg_conn.await).await {
                error!("can not report status of pg conn {e}");
            }
        });
        let pg_client: PgClient = pg_client;
        let qu_select = {
            let sql = "select channel, addr from ioc_by_channel_log where facility = $1 and channel = $2 and addr is not distinct from $3 and archived = 0";
            pg_client.prepare(sql).await.unwrap()
        };
        let qu_update_tsmod = {
            let sql = "update ioc_by_channel_log set tsmod = now(), responseaddr = $4 where facility = $1 and channel = $2 and addr is not distinct from $3 and archived = 0";
            pg_client.prepare(sql).await.unwrap()
        };
        let qu_update_archived = {
            let sql =
                "update ioc_by_channel_log set archived = 1 where facility = $1 and channel = $2 and archived = 0";
            pg_client.prepare(sql).await.unwrap()
        };
        let qu_insert = {
            let sql = "insert into ioc_by_channel_log (facility, channel, addr, responseaddr) values ($1, $2, $3, $4)";
            const TEXT: tokio_postgres::types::Type = tokio_postgres::types::Type::TEXT;
            pg_client.prepare_typed(sql, &[TEXT, TEXT, TEXT, TEXT]).await.unwrap()
        };
        while let Ok(item) = rx.recv().await {
            let responseaddr = item.response_addr.map(|x| x.to_string());
            let addr = item.addr.map(|x| x.to_string());
            let res = pg_client
                .query(&qu_select, &[&backend, &item.channel, &addr])
                .await
                .unwrap();
            if res.len() == 0 {
                pg_client
                    .execute(&qu_update_archived, &[&backend, &item.channel])
                    .await
                    .unwrap();
                pg_client
                    .execute(&qu_insert, &[&backend, &item.channel, &addr, &responseaddr])
                    .await
                    .unwrap();
            } else if res.len() == 1 {
                pg_client
                    .execute(&qu_update_tsmod, &[&backend, &item.channel, &addr, &responseaddr])
                    .await
                    .unwrap();
            } else {
                warn!("Duplicate for {}", item.channel);
                let sql="with q1 as (select ctid from ioc_by_channel_log where facility = $1 and channel = $2 and addr is not distinct from $3 order by tsmod desc, ctid desc limit 1) update ioc_by_channel_log t set archived = 1 from q1 where t.facility = $1 and t.channel = $2 and t.addr is not distinct from $3 and t.ctid != q1.ctid";
                pg_client.execute(sql, &[&backend, &item.channel, &addr]).await.unwrap();
                pg_client
                    .execute(&qu_update_tsmod, &[&backend, &item.channel, &addr, &responseaddr])
                    .await
                    .unwrap();
            }
        }
        drop(pg_client);
        let x = pgconn_out_rx.recv().await;
        if let Err(e) = x {
            error!("db worker sees: {e}");
        }
    }
}

pub async fn ca_search(opts: CaIngestOpts, channels: &Vec<String>) -> Result<(), Error> {
    info!("ca_search begin");
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
                    error!("can not resolve {s} {e}");
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
        let w = DbUpdateWorker::new(dbrx.clone(), opts.backend().into(), opts.postgresql().clone());
        dbworkers.push(w);
    }
    drop(dbrx);
    let dbtx: Sender<_> = dbtx;

    let mut ts_last = Instant::now();
    loop {
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
                match dbtx.send(item).await {
                    Ok(_) => {}
                    Err(_) => {
                        error!("dbtx broken");
                        break;
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
