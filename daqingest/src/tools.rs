use crate::opts::RemoveOlder;
use chrono::DateTime;
use chrono::Utc;
use dbpg::conn::PgClient;
use err::thiserror;
use err::ThisError;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use scywr::config::ScyllaIngestConfig;
use scywr::scylla::transport::errors::QueryError;
use scywr::scylla::transport::iterator::NextRowError;
use scywr::session::ScySession;

#[derive(Debug, ThisError)]
pub enum Error {
    PgConn(#[from] dbpg::err::Error),
    Postgres(#[from] dbpg::postgres::Error),
    ScyllaSession(#[from] scywr::session::Error),
    ScyllaQuery(#[from] QueryError),
    ScyllaNextRowError(#[from] NextRowError),
    ParseError(String),
}

pub async fn remove_older(
    backend: String,
    params: RemoveOlder,
    pgconf: &Database,
    scyconf: &ScyllaIngestConfig,
) -> Result<(), Error> {
    let channel_regex = "^TEST:SLOW:SCALAR:.+:00000[0-9]$";
    let date_cut: DateTime<Utc> = params
        .date
        .parse()
        .map_err(|_| Error::ParseError(format!("can not parse {:?}", params.date)))?;
    let epoch: DateTime<Utc> = "1970-01-01T00:00:00Z".parse().unwrap();
    let dt_epoch = date_cut.signed_duration_since(epoch);
    let ts_cut = dt_epoch.num_seconds() as u64 * 1000000000;
    debug!("chosen date is  {:?}  {}", date_cut, ts_cut);
    let (pg, _) = dbpg::conn::make_pg_client(pgconf).await?;
    let scy = scywr::session::create_session(scyconf).await?;
    let sql = concat!(
        "select series, channel from series_by_channel",
        " where facility = $1 and kind = 2 and channel ~ $2"
    );
    let rows = pg.query(sql, &[&backend, &channel_regex]).await?;
    for row in rows {
        let series: i64 = row.get(0);
        let channel: String = row.get(1);
        let series = series as u64;
        debug!("care about  {}  {}", channel, series);
        remove_older_series(series, ts_cut, &pg, &scy).await?;
    }
    Ok(())
}

async fn remove_older_series(series: u64, ts_cut: u64, _pg: &PgClient, scy: &ScySession) -> Result<(), Error> {
    let it = scy
        .query_iter(
            "select ts_msp from ts_msp where series = ? and ts_msp < ?",
            &(series as i64, ts_cut as i64),
        )
        .await?;
    type RowType = (i64,);
    let mut it = it.into_typed::<RowType>().take(10);
    while let Some(e) = it.next().await {
        let row = e?;
        let ts_msp = row.0 as u64;
        debug!("remove ts_msp {}", ts_msp);
        // TODO must know scalar type and shape (at least scalar or wave) to select the correct scylla table
    }
    Ok(())
}
