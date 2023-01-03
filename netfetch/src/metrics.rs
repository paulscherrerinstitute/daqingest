use crate::ca::conn::ConnCommand;
use crate::ca::IngestCommons;
use crate::ca::METRICS;
use axum::extract::Query;
use err::Error;
use http::Request;
use log::*;
use serde::{Deserialize, Serialize};
use stats::{CaConnStats, CaConnStatsAgg, CaConnStatsAggDiff};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtraInsertsConf {
    pub copies: Vec<(u64, u64)>,
}

impl ExtraInsertsConf {
    pub fn new() -> Self {
        Self { copies: Vec::new() }
    }
}

async fn find_channel(
    params: HashMap<String, String>,
    ingest_commons: Arc<IngestCommons>,
) -> axum::Json<Vec<(String, Vec<String>)>> {
    let pattern = params.get("pattern").map_or(String::new(), |x| x.clone()).to_string();
    // TODO allow usage of `?` in handler:
    let res = ingest_commons
        .ca_conn_set
        .send_command_to_all(|| ConnCommand::find_channel(pattern.clone()))
        .await
        .unwrap();
    let res = res.into_iter().map(|x| (x.0.to_string(), x.1)).collect();
    axum::Json(res)
}

async fn channel_add_inner(params: HashMap<String, String>, ingest_commons: Arc<IngestCommons>) -> Result<(), Error> {
    if let (Some(backend), Some(name)) = (params.get("backend"), params.get("name")) {
        match crate::ca::find_channel_addr(backend.into(), name.into(), &ingest_commons.pgconf).await {
            Ok(Some(addr)) => {
                ingest_commons
                    .ca_conn_set
                    .add_channel_to_addr(
                        ingest_commons.backend.clone(),
                        SocketAddr::V4(addr),
                        name.into(),
                        ingest_commons.clone(),
                    )
                    .await?;
                Ok(())
            }
            _ => {
                error!("can not find addr for channel");
                Err(Error::with_msg_no_trace(format!("can not find addr for channel")))
            }
        }
    } else {
        Err(Error::with_msg_no_trace(format!("wrong parameters given")))
    }
}

async fn channel_add(params: HashMap<String, String>, ingest_commons: Arc<IngestCommons>) -> axum::Json<bool> {
    let ret = match channel_add_inner(params, ingest_commons).await {
        Ok(_) => true,
        Err(_) => false,
    };
    axum::Json(ret)
}

async fn channel_remove(
    params: HashMap<String, String>,
    ingest_commons: Arc<IngestCommons>,
) -> axum::Json<serde_json::Value> {
    use axum::Json;
    use serde_json::Value;
    let addr = if let Some(x) = params.get("addr") {
        if let Ok(addr) = x.parse::<SocketAddrV4>() {
            addr
        } else {
            return Json(Value::Bool(false));
        }
    } else {
        return Json(Value::Bool(false));
    };
    let _backend = if let Some(x) = params.get("backend") {
        x
    } else {
        return Json(Value::Bool(false));
    };
    let name = if let Some(x) = params.get("name") {
        x
    } else {
        return Json(Value::Bool(false));
    };
    match ingest_commons
        .ca_conn_set
        .send_command_to_addr(&SocketAddr::V4(addr), || ConnCommand::channel_remove(name.into()))
        .await
    {
        Ok(k) => Json(Value::Bool(k)),
        Err(e) => {
            error!("{e:?}");
            Json(Value::Bool(false))
        }
    }
}

async fn channel_state(params: HashMap<String, String>, ingest_commons: Arc<IngestCommons>) -> String {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    match ingest_commons
        .ca_conn_set
        .send_command_to_all(|| ConnCommand::channel_state(name.clone()))
        .await
    {
        Ok(k) => {
            let a: Vec<_> = k.into_iter().map(|(a, b)| (a.to_string(), b)).collect();
            serde_json::to_string(&a).unwrap()
        }
        Err(e) => {
            error!("{e:?}");
            return format!("null");
        }
    }
}

async fn channel_states(
    params: HashMap<String, String>,
    ingest_commons: Arc<IngestCommons>,
) -> axum::Json<Vec<crate::ca::conn::ChannelStateInfo>> {
    let limit = params.get("limit").map(|x| x.parse()).unwrap_or(Ok(40)).unwrap_or(40);
    let vals = ingest_commons
        .ca_conn_set
        .send_command_to_all(|| ConnCommand::channel_states_all())
        .await
        .unwrap();
    let mut res = Vec::new();
    for h in vals {
        for j in h.1 {
            res.push(j);
        }
    }
    res.sort_unstable_by_key(|v| u32::MAX - v.interest_score as u32);
    res.truncate(limit);
    axum::Json(res)
}

async fn extra_inserts_conf_set(v: ExtraInsertsConf, ingest_commons: Arc<IngestCommons>) -> axum::Json<bool> {
    // TODO ingest_commons is the authorative value. Should have common function outside of this metrics which
    // can update everything to a given value.
    *ingest_commons.extra_inserts_conf.lock().await = v.clone();
    ingest_commons
        .ca_conn_set
        .send_command_to_all(|| ConnCommand::extra_inserts_conf_set(v.clone()))
        .await
        .unwrap();
    axum::Json(true)
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
struct DummyQuery {
    name: String,
    surname: Option<String>,
    age: usize,
}

pub async fn start_metrics_service(bind_to: String, ingest_commons: Arc<IngestCommons>) {
    use axum::extract;
    use axum::http::StatusCode;
    use axum::routing::{get, put};
    use axum::Router;
    let app = Router::new()
        .fallback(|req: Request<axum::body::Body>| async move {
            info!("Fallback for {} {}", req.method(), req.uri());
            StatusCode::NOT_FOUND
        })
        .nest(
            "/some",
            Router::new()
                .route("/path1", get(|| async { (StatusCode::OK, format!("Hello there!")) }))
                .route(
                    "/path2",
                    get(|qu: Query<DummyQuery>| async move { (StatusCode::OK, format!("{qu:?}")) }),
                ),
        )
        .route(
            "/metrics",
            get(|| async {
                let stats = crate::ca::METRICS.lock().unwrap();
                match stats.as_ref() {
                    Some(s) => {
                        trace!("Metrics");
                        s.prometheus()
                    }
                    None => {
                        trace!("Metrics empty");
                        String::new()
                    }
                }
            }),
        )
        .route(
            "/daqingest/find/channel",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| find_channel(params, ingest_commons)
            }),
        )
        .route(
            "/daqingest/channel/state",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| channel_state(params, ingest_commons)
            }),
        )
        .route(
            "/daqingest/channel/states",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| channel_states(params, ingest_commons)
            }),
        )
        .route(
            "/daqingest/channel/add",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| channel_add(params, ingest_commons)
            }),
        )
        .route(
            "/daqingest/channel/remove",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| channel_remove(params, ingest_commons)
            }),
        )
        .route(
            "/store_workers_rate",
            get({
                let c = ingest_commons.clone();
                || async move { axum::Json(c.store_workers_rate.load(Ordering::Acquire)) }
            })
            .put({
                let c = ingest_commons.clone();
                |v: extract::Json<u64>| async move {
                    c.store_workers_rate.store(v.0, Ordering::Release);
                }
            }),
        )
        .route(
            "/insert_frac",
            get({
                let c = ingest_commons.clone();
                || async move { axum::Json(c.insert_frac.load(Ordering::Acquire)) }
            })
            .put({
                let c = ingest_commons.clone();
                |v: extract::Json<u64>| async move {
                    c.insert_frac.store(v.0, Ordering::Release);
                }
            }),
        )
        .route(
            "/extra_inserts_conf",
            get({
                let c = ingest_commons.clone();
                || async move {
                    let res = c.extra_inserts_conf.lock().await;
                    axum::Json(serde_json::to_value(&*res).unwrap())
                }
            })
            .put({
                let ingest_commons = ingest_commons.clone();
                |v: extract::Json<ExtraInsertsConf>| extra_inserts_conf_set(v.0, ingest_commons)
            }),
        )
        .route(
            "/insert_ivl_min",
            put({
                let insert_ivl_min = ingest_commons.insert_ivl_min.clone();
                |v: extract::Json<u64>| async move {
                    insert_ivl_min.store(v.0, Ordering::Release);
                }
            }),
        );
    axum::Server::bind(&bind_to.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap()
}

pub async fn metrics_agg_task(
    ingest_commons: Arc<IngestCommons>,
    local_stats: Arc<CaConnStats>,
    store_stats: Arc<CaConnStats>,
) -> Result<(), Error> {
    let mut agg_last = CaConnStatsAgg::new();
    loop {
        tokio::time::sleep(Duration::from_millis(671)).await;
        let agg = CaConnStatsAgg::new();
        agg.push(&local_stats);
        agg.push(&store_stats);
        {
            let conn_stats_guard = ingest_commons.ca_conn_set.ca_conn_ress().lock().await;
            for (_, g) in conn_stats_guard.iter() {
                agg.push(g.stats());
            }
        }
        {
            let val = ingest_commons.insert_item_queue.receiver().len() as u64;
            agg.store_worker_recv_queue_len.store(val, Ordering::Release);
        }
        let mut m = METRICS.lock().unwrap();
        *m = Some(agg.clone());
        if false {
            let diff = CaConnStatsAggDiff::diff_from(&agg_last, &agg);
            info!("{}", diff.display());
        }
        agg_last = agg;
    }
}
