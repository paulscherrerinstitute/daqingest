use crate::conn::PgClient;
use crate::err::Error;
use async_channel::Receiver;
use log::*;
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio_postgres::Statement;

const TEXT: tokio_postgres::types::Type = tokio_postgres::types::Type::TEXT;

#[derive(Debug)]
pub struct IocItem {
    pub channel: String,
    pub response_addr: Option<SocketAddrV4>,
    pub addr: Option<SocketAddrV4>,
    pub dt: Duration,
}

impl IocItem {
    pub fn new(channel: String, response_addr: Option<SocketAddrV4>, addr: Option<SocketAddrV4>, dt: Duration) -> Self {
        Self {
            channel,
            response_addr,
            addr,
            dt,
        }
    }
}

// TODO
// Issue: the prepared statements are bound to a connection.
// A connection pool is therefore often not a good model.
// Better would probably be to hand a database-work-item to a queue.
// A worker can then deal with the query.
// Must choose between: some async fn waits for that item being finished.
// Or: enqueue-and-forget and let the result of that query be enqueued
// in yet another queue.
pub struct IocSearchIndexWorker {
    backend: String,
    pg: PgClient,
    qu_select: Statement,
    qu_update_tsmod: Statement,
    qu_update_archived: Statement,
    qu_insert: Statement,
    rx: Receiver<IocItem>,
}

impl IocSearchIndexWorker {
    pub async fn prepare(rx: Receiver<IocItem>, backend: String, pg: PgClient) -> Result<Self, Error> {
        let qu_select = {
            let sql = "select channel, addr from ioc_by_channel_log where facility = $1 and channel = $2 and addr is not distinct from $3 and archived = 0";
            pg.prepare(sql).await.unwrap()
        };
        let qu_update_tsmod = {
            let sql = "update ioc_by_channel_log set tsmod = now(), responseaddr = $4 where facility = $1 and channel = $2 and addr is not distinct from $3 and archived = 0";
            pg.prepare(sql).await.unwrap()
        };
        let qu_update_archived = {
            let sql =
                "update ioc_by_channel_log set archived = 1 where facility = $1 and channel = $2 and archived = 0";
            pg.prepare(sql).await.unwrap()
        };
        let qu_insert = {
            let sql = "insert into ioc_by_channel_log (facility, channel, addr, responseaddr) values ($1, $2, $3, $4)";
            pg.prepare_typed(sql, &[TEXT, TEXT, TEXT, TEXT]).await.unwrap()
        };
        let ret = Self {
            backend,
            pg,
            qu_select,
            qu_update_tsmod,
            qu_update_archived,
            qu_insert,
            rx,
        };
        Ok(ret)
    }

    pub async fn worker(&self) {
        let backend = &self.backend;
        let rx = &self.rx;
        let pg = &self.pg;
        while let Ok(item) = rx.recv().await {
            let responseaddr = item.response_addr.map(|x| x.to_string());
            let addr = item.addr.map(|x| x.to_string());
            let res = pg
                .query(&self.qu_select, &[backend, &item.channel, &addr])
                .await
                .unwrap();
            if res.len() == 0 {
                pg.execute(&self.qu_update_archived, &[backend, &item.channel])
                    .await
                    .unwrap();
                pg.execute(&self.qu_insert, &[backend, &item.channel, &addr, &responseaddr])
                    .await
                    .unwrap();
            } else if res.len() == 1 {
                pg.execute(&self.qu_update_tsmod, &[backend, &item.channel, &addr, &responseaddr])
                    .await
                    .unwrap();
            } else {
                warn!("Duplicate for {}", item.channel);
                let sql = concat!(
                    "with q1 as (select ctid from ioc_by_channel_log where facility = $1 and channel = $2 and addr is not distinct from $3 order by tsmod desc, ctid desc limit 1)",
                    " update ioc_by_channel_log t set archived = 1 from q1 where t.facility = $1 and t.channel = $2 and t.ctid != q1.ctid and archived != 1",
                );
                pg.execute(sql, &[backend, &item.channel, &addr]).await.unwrap();
                pg.execute(&self.qu_update_tsmod, &[&backend, &item.channel, &addr, &responseaddr])
                    .await
                    .unwrap();
            }
        }
    }
}
