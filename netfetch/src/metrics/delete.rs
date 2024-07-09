use super::RoutesResources;
use axum::extract::FromRequest;
use axum::extract::Query;
use axum::handler::Handler;
use axum::http::HeaderMap;
use axum::Json;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use core::fmt;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::ScalarType;
use netpod::TsMs;
use netpod::TsNano;
use scylla::Session as ScySession;
use scywr::config::ScyllaIngestConfig;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::ArrayValue;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::ScalarValue;
use scywr::scylla;
use scywr::scylla::prepared_statement::PreparedStatement;
use serde::Deserialize;
use series::SeriesId;
use serieswriter::writer::SeriesWriter;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use streams::framed_bytes::FramedBytesStream;
use taskrun::tokio::time::timeout;

#[allow(unused)]
macro_rules! debug_cql {
    ($($arg:tt)*) => {
        if true {
            debug!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "HttpDelete")]
pub enum Error {
    Logic,
    MissingRetentionTime,
    MissingSeriesId,
    MissingScalarType,
    MissingBegDate,
    MissingEndDate,
    ScyllaTransport(#[from] scylla::transport::errors::NewSessionError),
    ScyllaQuery(#[from] scylla::transport::errors::QueryError),
    ScyllaRowType(#[from] scylla::transport::query_result::RowsExpectedError),
    ScyllaRowError(#[from] scylla::cql_to_rust::FromRowError),
    InvalidTimestamp,
}

pub async fn delete(
    (headers, Query(params), body): (HeaderMap, Query<HashMap<String, String>>, axum::body::Body),
    rres: Arc<RoutesResources>,
) -> Json<serde_json::Value> {
    match delete_try(headers, params, body, rres).await {
        Ok(k) => k,
        Err(e) => Json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

fn st_to_ns(v: DateTime<Utc>) -> Result<TsNano, Error> {
    let sec = v.timestamp();
    if sec < 0 {
        Err(Error::InvalidTimestamp)
    } else if sec > 18446744073 {
        Err(Error::InvalidTimestamp)
    } else {
        let w = 1000000000 * sec as u64 + v.timestamp_subsec_nanos() as u64;
        Ok(TsNano::from_ns(w))
    }
}

// select * from sf_lt.lt_account_00 where token(part, ts) > -100000000 and series = 8554946496751499549 allow filtering

async fn delete_try(
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    let rt: RetentionTime = params
        .get("retentionTime")
        .ok_or(Error::MissingRetentionTime)
        .and_then(|x| x.parse().map_err(|_| Error::MissingRetentionTime))?;
    let series = params
        .get("series")
        .ok_or(Error::MissingSeriesId)
        .and_then(|x| x.parse().map_err(|_| Error::MissingSeriesId))
        .map(SeriesId::new)?;
    let beg: DateTime<Utc> = params
        .get("begDate")
        .ok_or(Error::MissingBegDate)
        .and_then(|x| x.parse().map_err(|_| Error::MissingBegDate))?;
    let end: DateTime<Utc> = params
        .get("endDate")
        .ok_or(Error::MissingEndDate)
        .and_then(|x| x.parse().map_err(|_| Error::MissingEndDate))?;
    let scalar_type: ScalarType = params
        .get("scalarType")
        .ok_or(Error::MissingScalarType)
        .and_then(|x| ScalarType::from_variant_str(x).map_err(|_| Error::MissingScalarType))?;
    debug_cql!("delete  params  {rt:?}  {series:?}  {beg:?}  {end:?}");
    let beg = st_to_ns(beg)?;
    let end = st_to_ns(end)?;
    let scyconf = &rres.scyconf_st;
    let scy = scy_connect(scyconf).await?;
    let qu = {
        let cql = format!(
            concat!("select ts_msp from {}.{}ts_msp where series = ?"),
            scyconf.keyspace(),
            rt.table_prefix(),
        );
        scy.prepare(scylla::query::Query::new(cql).with_page_size(4)).await?
    };
    let qu_delete_val = {
        let _cql = format!(
            concat!(
                "select ts_lsp from {}.{}events_scalar_{}",
                " where series = ? and ts_msp = ?",
                " and ts_lsp >= ? and ts_lsp < ?",
            ),
            scyconf.keyspace(),
            rt.table_prefix(),
            scalar_type.to_scylla_table_name_id(),
        );
        let cql = format!(
            concat!(
                "delete from {}.{}events_scalar_{}",
                " where series = ? and ts_msp = ?",
                " and ts_lsp >= ? and ts_lsp < ?",
            ),
            scyconf.keyspace(),
            rt.table_prefix(),
            scalar_type.to_scylla_table_name_id(),
        );
        scy.prepare(scylla::query::Query::new(cql).with_page_size(100)).await?
    };
    let mut pst = None;
    let mut i = 0;
    loop {
        // debug_cql!("query iteration  {i}");
        let z = scy.execute_paged(&qu, (series.to_i64(),), pst).await?;
        pst = z.paging_state.clone();
        for x in z.rows_typed::<(i64,)>()? {
            let (msp,) = x?;
            let msp = TsMs::from_ms_u64(msp as _);
            let msp_ns = msp.ns_u64();
            delete_val(series.clone(), msp, beg, end, &qu_delete_val, &scy).await?;
        }
        if pst.is_none() {
            debug_cql!("last page");
            break;
        }
        i += 1;
        if false {
            if i > 20 {
                debug_cql!("loop limit");
                break;
            }
        }
    }
    Ok(Json(serde_json::Value::Null))
}

async fn delete_val(
    series: SeriesId,
    msp: TsMs,
    beg: TsNano,
    end: TsNano,
    qu_delete_val: &PreparedStatement,
    scy: &ScySession,
) -> Result<(), Error> {
    let msp_ns = msp.ns_u64();
    if msp_ns >= end.ns() {
        // debug_cql!("  return early  msp {msp}  after range");
        return Ok(());
    }
    let r1 = if msp_ns >= beg.ns() { 0 } else { beg.ns() - msp_ns };
    let r2 = end.ns() - msp_ns;
    let o0 = DateTime::from_timestamp_millis((msp.ms() + 0 / 1000000) as i64).unwrap();
    let o1 = DateTime::from_timestamp_millis((msp.ms() + r1 / 1000000) as i64).unwrap();
    let o2 = DateTime::from_timestamp_millis((msp.ms() + r2 / 1000000) as i64).unwrap();
    debug_cql!("  sub query  {o0:?}  {o1:?}  {o2:?}");
    let mut pst = None;
    let mut i = 0;
    loop {
        // debug_cql!("  sub query iteration  {i}");
        let params = (series.to_i64(), msp.ms() as i64, r1 as i64, r2 as i64);
        let z = scy.execute_paged(&qu_delete_val, params, pst).await?;
        pst = z.paging_state.clone();
        if z.rows_num().is_ok() {
            for (i, x) in z.rows_typed::<(i64,)>()?.enumerate() {
                let (lsp,) = x?;
                if false && i < 4 {
                    debug_cql!("  lsp {lsp}");
                }
            }
        }
        if pst.is_none() {
            // debug_cql!("  last page");
            break;
        }
        i += 1;
        if false {
            if i > 20 {
                debug_cql!("  loop limit");
                break;
            }
        }
    }
    Ok(())
}

async fn scy_connect(scyconf: &ScyllaIngestConfig) -> Result<Arc<ScySession>, Error> {
    use scylla::execution_profile::ExecutionProfileBuilder;
    use scylla::statement::Consistency;
    use scylla::transport::session::PoolSize;
    use scylla::transport::session_builder::GenericSessionBuilder;
    let profile = ExecutionProfileBuilder::default()
        .consistency(Consistency::Quorum)
        .build()
        .into_handle();
    let scy = GenericSessionBuilder::new()
        .pool_size(PoolSize::default())
        .known_nodes(scyconf.hosts())
        .default_execution_profile_handle(profile)
        .build()
        .await?;
    let scy = Arc::new(scy);
    Ok(scy)
}
