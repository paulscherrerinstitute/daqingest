use crate::opts::FindOlder;
use crate::opts::RemoveOlder;
use chrono::DateTime;
use chrono::Utc;
use dbpg::conn::PgClient;
use err::thiserror;
use err::ThisError;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use netpod::ScalarType;
use netpod::Shape;
use scywr::config::ScyllaIngestConfig;
use scywr::scylla::prepared_statement::PreparedStatement;
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
    scywr::schema::migrate_scylla_data_schema(scyconf, netpod::ttl::RetentionTime::Short).await?;
    let date_cut = parse_date_str(&params.date)?;
    let ts_cut = date_to_ts_ns(date_cut);
    debug!("chosen date is  {:?}  {}", date_cut, ts_cut);
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
    ts_cut: u64,
    _pg: &PgClient,
    scy: &ScySession,
) -> Result<(), Error> {
    let table_name = table_name_from_type(scalar_type, shape);
    let cql = format!(concat!("delete from {} where series = ? and ts_msp = ?",), table_name);
    let qu_delete: PreparedStatement = scy.prepare(cql).await?;
    let it = scy
        .query_iter(
            "select ts_msp from ts_msp where series = ? and ts_msp < ?",
            (series as i64, ts_cut as i64),
        )
        .await?;
    type RowType = (i64,);
    let mut it = it.into_typed::<RowType>();
    while let Some(e) = it.next().await {
        let row = e?;
        let ts_msp = row.0 as u64;
        debug!("remove ts_msp {}", ts_msp);
        let res = scy.execute(&qu_delete, (series as i64, ts_msp as i64)).await?;
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

pub async fn find_older_msp(
    backend: String,
    params: FindOlder,
    pgconf: &Database,
    scyconf: &ScyllaIngestConfig,
) -> Result<(), Error> {
    let date_cut = parse_date_str(&params.date)?;
    let ts_cut = date_to_ts_ns(date_cut);
    debug!("chosen date is  {:?}  {}", date_cut, ts_cut);
    let (pg, _) = dbpg::conn::make_pg_client(pgconf).await?;
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
            ScalarType::U8 => todo!(),
            ScalarType::U16 => todo!(),
            ScalarType::U32 => todo!(),
            ScalarType::U64 => todo!(),
            ScalarType::I8 => todo!(),
            ScalarType::I16 => todo!(),
            ScalarType::I32 => todo!(),
            ScalarType::I64 => todo!(),
            ScalarType::F32 => "events_scalar_f32",
            ScalarType::F64 => todo!(),
            ScalarType::BOOL => todo!(),
            ScalarType::STRING => todo!(),
            ScalarType::ChannelStatus => todo!(),
        },
        Shape::Wave(_) => match scalar_type {
            ScalarType::U8 => todo!(),
            ScalarType::U16 => todo!(),
            ScalarType::U32 => todo!(),
            ScalarType::U64 => todo!(),
            ScalarType::I8 => todo!(),
            ScalarType::I16 => todo!(),
            ScalarType::I32 => todo!(),
            ScalarType::I64 => todo!(),
            ScalarType::F32 => "events_array_f32",
            ScalarType::F64 => todo!(),
            ScalarType::BOOL => todo!(),
            ScalarType::STRING => todo!(),
            ScalarType::ChannelStatus => todo!(),
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
