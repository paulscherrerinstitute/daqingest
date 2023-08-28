use crate::conn::PgClient;
use err::thiserror;
use err::ThisError;
use log::*;
use std::net::SocketAddrV4;

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    IocAddrNotFound,
}

pub async fn find_channel_addr(backend: &str, name: String, pg: &PgClient) -> Result<Option<SocketAddrV4>, Error> {
    let qu_find_addr = pg
        .prepare(
            "select t1.facility, t1.channel, t1.addr from ioc_by_channel_log t1 where t1.facility = $1 and t1.channel = $2 and addr is not null order by tsmod desc limit 1",
        )
        .await?;
    let rows = pg.query(&qu_find_addr, &[&backend, &name]).await?;
    if rows.is_empty() {
        error!("can not find any addresses of channels {:?}", name);
        Err(Error::IocAddrNotFound)
    } else {
        for row in rows {
            match row.try_get::<_, &str>(2) {
                Ok(addr) => match addr.parse::<SocketAddrV4>() {
                    Ok(addr) => return Ok(Some(addr)),
                    Err(e) => {
                        error!("can not parse  {e:?}");
                        return Err(Error::IocAddrNotFound);
                    }
                },
                Err(e) => {
                    error!("can not find addr for {name}  {e:?}");
                }
            }
        }
        Ok(None)
    }
}

#[allow(unused)]
async fn query_addr_multiple(backend: &str, pg_client: &PgClient) -> Result<(), Error> {
    // TODO factor the find loop into a separate Stream.
    let sql = concat!(
        "with q1 as (select t1.facility, t1.channel, t1.addr from ioc_by_channel_log t1",
        " where t1.facility = $1",
        " and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9)",
        " and t1.addr is not null order by t1.tsmod desc)",
        " select distinct on (q1.facility, q1.channel) q1.facility, q1.channel, q1.addr from q1"
    );
    let qu_find_addr = pg_client.prepare(sql).await?;
    let mut chns_todo: &[String] = err::todoval();
    let mut chstmp = ["__NONE__"; 8];
    for (s1, s2) in chns_todo.iter().zip(chstmp.iter_mut()) {
        *s2 = s1;
    }
    chns_todo = &chns_todo[chstmp.len().min(chns_todo.len())..];
    let rows = pg_client
        .query(
            &qu_find_addr,
            &[
                &backend, &chstmp[0], &chstmp[1], &chstmp[2], &chstmp[3], &chstmp[4], &chstmp[5], &chstmp[6],
                &chstmp[7],
            ],
        )
        .await?;
    for row in rows {
        let ch: &str = row.get(1);
        let addr: &str = row.get(2);
        if addr == "" {
            // TODO the address was searched before but could not be found.
        } else {
            let addr: SocketAddrV4 = match addr.parse() {
                Ok(k) => k,
                Err(e) => {
                    error!("can not parse {addr:?} for channel {ch:?}  {e:?}");
                    continue;
                }
            };
            let _ = addr;
        }
    }
    Ok(())
}
