pub mod catools;

use crate::opts::FindOlder;
use crate::opts::RemoveOlder;
use crate::opts::RemoveOlderAll;
use chrono::DateTime;
use chrono::Utc;
use dbpg::conn::PgClient;
use err::thiserror;
use err::ThisError;
use futures_util::future;
use futures_util::stream;
use futures_util::StreamExt;
use log::*;
use netpod::ttl::RetentionTime;
use netpod::Database;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use scywr::config::ScyllaIngestConfig;
use scywr::scylla::prepared_statement::PreparedStatement;
use scywr::scylla::transport::errors::QueryError;
use scywr::scylla::transport::iterator::NextRowError;
use scywr::session::ScySession;
use series::SeriesId;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, ThisError)]
#[cstm(name = "DaqingestTools")]
pub enum Error {
    PgConn(#[from] dbpg::err::Error),
    Postgres(#[from] dbpg::postgres::Error),
    ScyllaSession(#[from] scywr::session::Error),
    ScyllaQuery(#[from] QueryError),
    ScyllaNextRowError(#[from] NextRowError),
    ScyllaSchema(#[from] scywr::schema::Error),
    ParseError(String),
    InvalidValue,
}

pub async fn remove_older(
    backend: String,
    params: RemoveOlder,
    pgconf: &Database,
    scyconf: &ScyllaIngestConfig,
) -> Result<(), Error> {
    let date_cut = parse_date_str(&params.date)?;
    let ts_cut = TsMs::from_ns_u64(date_to_ts_ns(date_cut));
    debug!("chosen date is  {:?}  {:?}", date_cut, ts_cut);
    let (pg, _) = dbpg::conn::make_pg_client(pgconf).await?;
    let scy = scywr::session::create_session(scyconf).await?;
    let sql = concat!(
        "select series, channel, scalar_type, shape_dims",
        " from series_by_channel",
        " where facility = $1 and kind = 2 and channel ~ $2"
    );
    let rows = pg.query(sql, &[&backend, &params.channel_regex]).await?;
    for row in rows {
        let series: i64 = row.get(0);
        let channel: String = row.get(1);
        let scalar_type: i32 = row.get(2);
        let shape_dims: Vec<i32> = row.get(3);
        let series = series as u64;
        let scalar_type = ScalarType::from_scylla_i32(scalar_type).map_err(|_| Error::InvalidValue)?;
        let shape = Shape::from_scylla_shape_dims(&shape_dims).map_err(|_| Error::InvalidValue)?;
        debug!("care about  {}  {}", channel, series);
        remove_older_series(series, &scalar_type, &shape, ts_cut, &pg, &scy).await?;
    }
    Ok(())
}

async fn remove_older_series(
    series: u64,
    scalar_type: &ScalarType,
    shape: &Shape,
    ts_cut: TsMs,
    _pg: &PgClient,
    scy: &ScySession,
) -> Result<(), Error> {
    let table_name = table_name_from_type(scalar_type, shape);
    let cql = format!(concat!("delete from {} where series = ? and ts_msp = ?",), table_name);
    let qu_delete: PreparedStatement = scy.prepare(cql).await?;
    let it = scy
        .query_iter(
            "select ts_msp from ts_msp where series = ? and ts_msp < ?",
            (series as i64, ts_cut.to_i64()),
        )
        .await?;
    type RowType = (i64,);
    let mut it = it.into_typed::<RowType>();
    while let Some(e) = it.next().await {
        let row = e?;
        let ts_msp = row.0;
        debug!("remove ts_msp {}", ts_msp);
        let res = scy.execute(&qu_delete, (series as i64, ts_msp)).await?;
        {
            // informative
            if let Some(rows) = res.rows {
                debug!("rows returned {}", rows.len());
                for row in rows {
                    debug!("{:?}", row.columns);
                }
            } else {
                // debug!("delete no rows returned");
            }
        }
    }
    Ok(())
}

struct Stmts {
    qu_select_series: Arc<PreparedStatement>,
    qu_select_msp: PreparedStatement,
    qu_delete: Vec<PreparedStatement>,
}

impl Stmts {
    async fn new(ks: &str, rt: RetentionTime, scy: &ScySession) -> Result<Self, Error> {
        let cql = format!("select distinct series from {}.{}{}", ks, rt.table_prefix(), "ts_msp");
        let mut qu_select_series = scy.prepare(cql).await?;
        qu_select_series.set_page_size(10000);
        let qu_select_series = Arc::new(qu_select_series);
        let cql = format!(
            concat!("select ts_msp from {}.{}{} where series = ?"),
            ks,
            rt.table_prefix(),
            "ts_msp"
        );
        let mut qu_select_msp = scy.prepare(cql).await?;
        qu_select_msp.set_page_size(10000);
        let mut qu_delete = Vec::new();
        let tynames = [
            "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bool", "string",
        ];
        let shapenames = ["scalar", "array"];
        for shn in &shapenames {
            for tyn in &tynames {
                let qu = scy
                    .prepare(format!(
                        "delete from {}.{}events_{}_{} where series = ? and ts_msp = ?",
                        ks,
                        rt.table_prefix(),
                        shn,
                        tyn
                    ))
                    .await?;
                qu_delete.push(qu);
            }
        }
        for shn in &["scalar"] {
            for tyn in &["enum"] {
                let qu = scy
                    .prepare(format!(
                        "delete from {}.{}events_{}_{} where series = ? and ts_msp = ?",
                        ks,
                        rt.table_prefix(),
                        shn,
                        tyn
                    ))
                    .await?;
                qu_delete.push(qu);
            }
        }
        let ret = Self {
            qu_select_series,
            qu_select_msp,
            qu_delete,
        };
        Ok(ret)
    }
}

pub async fn remove_older_all(params: RemoveOlderAll, scyconf: &ScyllaIngestConfig) -> Result<(), Error> {
    let date_cut = parse_date_str(&params.date)?;
    let ts_cut = TsMs::from_ns_u64(date_to_ts_ns(date_cut));
    debug!("chosen date is  {:?}  {:?}", date_cut, ts_cut);
    let scy = scywr::session::create_session(scyconf).await?;
    let ks = scyconf.keyspace();
    for rt in [RetentionTime::Short] {
        remove_older_all_rt(ts_cut, ks, rt, &scy).await?;
    }
    Ok(())
}

pub async fn remove_older_all_rt(ts_cut: TsMs, ks: &str, rt: RetentionTime, scy: &ScySession) -> Result<(), Error> {
    let stmts = Stmts::new(ks, rt.clone(), &scy).await?;
    type RowType = (i64,);
    let it = scy.execute_iter(stmts.qu_select_series.as_ref().clone(), ()).await?;
    let mut it = it.into_typed::<RowType>();
    let mut series_ids = Vec::with_capacity(1000000);
    let print_dt = Duration::from_millis(2000);
    let mut print_next = Instant::now() + print_dt;
    while let Some(e) = it.next().await {
        let row = e?;
        let series = SeriesId::new(row.0 as u64);
        series_ids.push(series);
        let tsnow = Instant::now();
        if print_next <= tsnow {
            print_next = tsnow + print_dt;
            info!("found so far {}", series_ids.len());
        }
        if series_ids.len() > 50000000000 {
            break;
        }
    }
    info!("found {} series", series_ids.len());
    let mut print_next = Instant::now() + print_dt;
    for (i, series) in series_ids.iter().enumerate() {
        remove_older_all_series(ts_cut, series.clone(), &stmts, &scy).await?;
        let tsnow = Instant::now();
        if print_next <= tsnow {
            print_next = tsnow + print_dt;
            let frac = i as f32 / series_ids.len() as f32;
            info!("removed so far {:8} of {:8}  {:.4}", i, series_ids.len(), frac);
        }
    }
    Ok(())
}

async fn remove_older_all_series(ts_cut: TsMs, series: SeriesId, stmts: &Stmts, scy: &ScySession) -> Result<(), Error> {
    type RowType = (i64,);
    let ts1 = Instant::now();
    let mut it = scy
        .execute_iter(stmts.qu_select_msp.clone(), (series.to_i64(),))
        .await?
        .into_typed::<RowType>();
    let mut msp_last = 0;
    let mut to_remove = Vec::new();
    let mut n_keep = 0;
    let mut n_remove = 0;
    let ts2 = Instant::now();
    while let Some(e) = it.next().await {
        let row = e?;
        let msp = row.0 as u64;
        if msp < msp_last {
            panic!("msp ordering error  {:?}", series);
        }
        if msp <= ts_cut.0 && msp_last != 0 {
            // info!("remove  {:?}  {:?}", series, msp_last);
            n_remove += 1;
            to_remove.push(msp_last);
        } else {
            // info!("keep    {:?}  {:?}", series, msp_last);
            n_keep += 1;
        }
        msp_last = msp;
    }
    let ts3 = Instant::now();
    if n_remove != 0 {
        let frac = n_remove as f32 / (n_keep + n_remove) as f32;
        remove_older_all_series_msps(series, to_remove, stmts, scy).await?;
        let ts4 = Instant::now();
        let dt2 = ts2.saturating_duration_since(ts1);
        let dt3 = ts3.saturating_duration_since(ts2);
        let dt4 = ts4.saturating_duration_since(ts3);
        info!(
            "{:4.0}  {:4.0}  {:4.0}  n_keep {:7}     n_remove {:7}     {:.4}     {:?}",
            1e3 * dt2.as_secs_f32(),
            1e3 * dt3.as_secs_f32(),
            1e3 * dt4.as_secs_f32(),
            n_keep,
            n_remove,
            frac,
            series,
        );
    }
    Ok(())
}

async fn remove_older_all_series_msps(
    series: SeriesId,
    msps: Vec<u64>,
    stmts: &Stmts,
    scy: &ScySession,
) -> Result<(), Error> {
    for stmt in &stmts.qu_delete {
        stream::iter(msps.clone())
            .map(|msp| async move {
                let stmt = stmt.clone();
                scy.execute(&stmt, (series.to_i64(), msp as i64)).await
            })
            .buffer_unordered(32)
            .take_while(|x| {
                if let Err(e) = &x {
                    error!("{e}");
                }
                future::ready(x.is_ok())
            })
            .fold(0, |_, _| future::ready(0i32))
            .await;
    }
    Ok(())
}

pub async fn find_older_msp(
    _backend: String,
    params: FindOlder,
    pgconf: &Database,
    scyconf: &ScyllaIngestConfig,
) -> Result<(), Error> {
    let date_cut = parse_date_str(&params.date)?;
    let ts_cut = TsMs::from_ns_u64(date_to_ts_ns(date_cut));
    debug!("chosen date is  {:?}  {:?}", date_cut, ts_cut);
    let (_pg, _jh) = dbpg::conn::make_pg_client(pgconf).await?;
    let scy = scywr::session::create_session(scyconf).await?;
    let table_name = &params.table_name;
    let cql = format!(
        concat!(
            "select distinct series, ts_msp from {}",
            " where token(series, ts_msp) >= ? and token(series, ts_msp) <= ?",
        ),
        table_name
    );
    let min = i64::MIN;
    let max = i64::MAX;
    let mut trbeg = min;
    let d = (u64::MAX / params.slices as u64) as i64;
    let qu = scy.prepare(cql).await?;
    loop {
        let trend = if trbeg < max - d { trbeg + d } else { max };
        let mut it = scy
            .execute_iter(qu.clone(), (trbeg, trend))
            .await?
            .into_typed::<(i64, i64)>();
        let mut c = 0;
        while let Some(u) = it.next().await {
            let row = u?;
            let series = row.0 as u64;
            let ts_msp = row.1 as u64;
            if series == 9033627543553833740 {
                debug!("found  series {}  ts_msp {}", series, ts_msp);
            }
            c += 1;
        }
        let pct = trend - min;
        debug!("query  {:6}  {:016x}  {:016x}  had {:5} rows", pct / d, trbeg, trend, c);
        if trend == max {
            break;
        }
        trbeg += d;
    }
    Ok(())
}

fn table_name_from_type(scalar_type: &ScalarType, shape: &Shape) -> &'static str {
    match shape {
        Shape::Scalar => match scalar_type {
            ScalarType::F32 => "events_scalar_f32",
            _ => todo!(),
        },
        Shape::Wave(_) => match scalar_type {
            ScalarType::F32 => "events_array_f32",
            _ => todo!(),
        },
        Shape::Image(_, _) => todo!(),
    }
}

fn parse_date_str(inp: &str) -> Result<DateTime<Utc>, Error> {
    inp.parse()
        .map_err(|_| Error::ParseError(format!("can not parse {:?}", inp)))
}

fn date_to_ts_ns(date: DateTime<Utc>) -> u64 {
    let epoch: DateTime<Utc> = "1970-01-01T00:00:00Z".parse().unwrap();
    let dt_epoch = date.signed_duration_since(epoch);
    dt_epoch.num_seconds() as u64 * 1000000000
}
