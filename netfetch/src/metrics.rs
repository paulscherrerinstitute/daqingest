use crate::ca::conn::ChannelStateInfo;
use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ChannelStatusesResponse;
use crate::ca::connset::ConnSetCmd;
use crate::ca::METRICS;
use crate::daemon_common::DaemonEvent;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakSender;
use axum::extract::Query;
use err::Error;
use http::Request;
use log::*;
use scywr::iteminsertqueue::QueryItem;
use serde::Deserialize;
use serde::Serialize;
use stats::CaConnSetStats;
use stats::CaConnStats;
use stats::CaConnStatsAgg;
use stats::CaConnStatsAggDiff;
use stats::DaemonStats;
use stats::InsertWorkerStats;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use taskrun::tokio;

pub struct StatsSet {
    daemon: Arc<DaemonStats>,
    ca_conn_set: Arc<CaConnSetStats>,
    insert_worker_stats: Arc<InsertWorkerStats>,
}

impl StatsSet {
    pub fn new(
        daemon: Arc<DaemonStats>,
        ca_conn_set: Arc<CaConnSetStats>,
        insert_worker_stats: Arc<InsertWorkerStats>,
    ) -> Self {
        Self {
            daemon,
            ca_conn_set,
            insert_worker_stats,
        }
    }
}

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
    dcom: Arc<DaemonComm>,
) -> axum::Json<Vec<(String, Vec<String>)>> {
    let pattern = params.get("pattern").map_or(String::new(), |x| x.clone()).to_string();
    // TODO ask Daemon for that information.
    error!("TODO find_channel");
    let res = Vec::new();
    axum::Json(res)
}

async fn channel_add_inner(params: HashMap<String, String>, dcom: Arc<DaemonComm>) -> Result<(), Error> {
    if let (Some(backend), Some(name)) = (params.get("backend"), params.get("name")) {
        error!("TODO channel_add_inner");
        Err(Error::with_msg_no_trace(format!("TODO channel_add_inner")))
    } else {
        Err(Error::with_msg_no_trace(format!("wrong parameters given")))
    }
}

async fn channel_add(params: HashMap<String, String>, dcom: Arc<DaemonComm>) -> axum::Json<bool> {
    let ret = match channel_add_inner(params, dcom).await {
        Ok(_) => true,
        Err(_) => false,
    };
    axum::Json(ret)
}

async fn channel_remove(params: HashMap<String, String>, dcom: Arc<DaemonComm>) -> axum::Json<serde_json::Value> {
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
    error!("TODO channel_remove");
    Json(Value::Bool(false))
}

async fn channel_state(params: HashMap<String, String>, dcom: Arc<DaemonComm>) -> axum::Json<bool> {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    error!("TODO channel_state");
    axum::Json(false)
}

// axum::Json<ChannelStatusesResponse>
async fn channel_states(params: HashMap<String, String>, tx: Sender<CaConnSetEvent>) -> String {
    let limit = params
        .get("limit")
        .map(|x| x.parse().ok())
        .unwrap_or(None)
        .unwrap_or(40);
    let (tx2, rx2) = async_channel::bounded(1);
    let req = ChannelStatusesRequest { tx: tx2 };
    let item = CaConnSetEvent::ConnSetCmd(ConnSetCmd::ChannelStatuses(req));
    // TODO handle error
    tx.send(item).await;
    let res = rx2.recv().await.unwrap();
    match serde_json::to_string(&res) {
        Ok(x) => x,
        Err(e) => {
            error!("Serialize error {e}");
            Err::<(), _>(e).unwrap();
            panic!();
        }
    }
    // axum::Json(res)
}

async fn extra_inserts_conf_set(v: ExtraInsertsConf, dcom: Arc<DaemonComm>) -> axum::Json<bool> {
    // TODO ingest_commons is the authorative value. Should have common function outside of this metrics which
    // can update everything to a given value.
    error!("TODO extra_inserts_conf_set");
    axum::Json(true)
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
struct DummyQuery {
    name: String,
    surname: Option<String>,
    age: usize,
}

pub struct DaemonComm {
    tx: Sender<DaemonEvent>,
}

impl DaemonComm {
    pub fn new(tx: Sender<DaemonEvent>) -> Self {
        Self { tx }
    }
}

fn make_routes(dcom: Arc<DaemonComm>, connset_cmd_tx: Sender<CaConnSetEvent>, stats_set: StatsSet) -> axum::Router {
    use axum::extract;
    use axum::routing::get;
    use axum::routing::put;
    use axum::Router;
    use http::StatusCode;

    Router::new()
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
            get({
                //
                || async move {
                    debug!("metrics");
                    let mut s1 = stats_set.daemon.prometheus();
                    let s2 = stats_set.ca_conn_set.prometheus();
                    let s3 = stats_set.insert_worker_stats.prometheus();
                    s1.push_str(&s2);
                    s1.push_str(&s3);
                    s1
                }
            }),
        )
        .route(
            "/daqingest/find/channel",
            get({
                let dcom = dcom.clone();
                |Query(params): Query<HashMap<String, String>>| find_channel(params, dcom)
            }),
        )
        .route(
            "/daqingest/channel/state",
            get({
                let dcom = dcom.clone();
                |Query(params): Query<HashMap<String, String>>| channel_state(params, dcom)
            }),
        )
        .route(
            "/daqingest/channel/states",
            get({
                let dcom = dcom.clone();
                let tx = connset_cmd_tx.clone();
                |Query(params): Query<HashMap<String, String>>| channel_states(params, tx)
            }),
        )
        .route(
            "/daqingest/channel/add",
            get({
                let dcom = dcom.clone();
                |Query(params): Query<HashMap<String, String>>| channel_add(params, dcom)
            }),
        )
        .route(
            "/daqingest/channel/remove",
            get({
                let dcom = dcom.clone();
                |Query(params): Query<HashMap<String, String>>| channel_remove(params, dcom)
            }),
        )
        .route(
            "/store_workers_rate",
            get({
                let dcom = dcom.clone();
                || async move { axum::Json(123) }
            })
            .put({
                let dcom = dcom.clone();
                |v: extract::Json<u64>| async move {}
            }),
        )
        .route(
            "/insert_frac",
            get({
                let dcom = dcom.clone();
                || async move { axum::Json(123) }
            })
            .put({
                let dcom = dcom.clone();
                |v: extract::Json<u64>| async move {}
            }),
        )
        .route(
            "/extra_inserts_conf",
            get({
                let dcom = dcom.clone();
                || async move { axum::Json(serde_json::to_value(&"TODO").unwrap()) }
            })
            .put({
                let dcom = dcom.clone();
                |v: extract::Json<ExtraInsertsConf>| extra_inserts_conf_set(v.0, dcom)
            }),
        )
        .route(
            "/insert_ivl_min",
            put({
                let dcom = dcom.clone();
                |v: extract::Json<u64>| async move {}
            }),
        )
}

pub async fn metrics_service(
    bind_to: String,
    dcom: Arc<DaemonComm>,
    connset_cmd_tx: Sender<CaConnSetEvent>,
    stats_set: StatsSet,
    shutdown_signal: Receiver<u32>,
) {
    let addr = bind_to.parse().unwrap();
    let router = make_routes(dcom, connset_cmd_tx, stats_set).into_make_service();
    axum::Server::bind(&addr)
        .serve(router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_signal.recv().await;
        })
        .await
        .unwrap()
}

pub async fn metrics_agg_task(
    query_item_chn: WeakSender<QueryItem>,
    local_stats: Arc<CaConnStats>,
    store_stats: Arc<CaConnStats>,
) -> Result<(), Error> {
    let mut agg_last = CaConnStatsAgg::new();
    loop {
        tokio::time::sleep(Duration::from_millis(671)).await;
        let agg = CaConnStatsAgg::new();
        agg.push(&local_stats);
        agg.push(&store_stats);
        trace!("TODO metrics_agg_task");
        // TODO when a CaConn is closed, I'll lose the so far collected counts, which creates a jump
        // in the metrics.
        // To make this sound:
        // Let CaConn keep a stats and just count.
        // At the tick, create a snapshot: all atomics are copied after each other.
        // Diff this new snapshot with an older snapshot and send that.
        // Note: some stats are counters, but some are current values.
        // e.g. the number of active channels should go down when a CaConn stops.
        #[cfg(DISABLED)]
        {
            let conn_stats_guard = ingest_commons.ca_conn_set.ca_conn_ress().lock().await;
            for (_, g) in conn_stats_guard.iter() {
                agg.push(g.stats());
            }
        }
        {
            warn!("TODO provide metrics with a weak ref to the query_item_channel");
            let nitems = query_item_chn.upgrade().map_or(0, |x| x.len());
            agg.store_worker_recv_queue_len.__set(nitems as u64);
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
