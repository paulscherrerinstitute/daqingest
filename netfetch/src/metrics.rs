#![allow(unused)]
pub mod delete;
pub mod ingest;
pub mod postingest;
pub mod status;

use crate::ca::conn::ChannelStateInfo;
use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ChannelStatusesResponse;
use crate::ca::connset::ConnSetCmd;
use crate::ca::statemap::ChannelState;
use crate::conf::ChannelConfig;
use crate::daemon_common::DaemonEvent;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakSender;
use axum::extract::Query;
use axum::http;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::Error;
use http::Request;
use http::StatusCode;
use http_body::Body;
use log::*;
use scywr::config::ScyllaIngestConfig;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use stats::CaConnSetStats;
use stats::CaConnStats;
use stats::CaConnStatsAgg;
use stats::CaConnStatsAggDiff;
use stats::CaProtoStats;
use stats::DaemonStats;
use stats::InsertWorkerStats;
use stats::IocFinderStats;
use stats::SeriesByChannelStats;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;
use taskrun::tokio::net::TcpListener;

struct PublicErrorMsg(String);

trait ToPublicErrorMsg {
    fn to_public_err_msg(&self) -> PublicErrorMsg;
}

impl ToPublicErrorMsg for err::Error {
    fn to_public_err_msg(&self) -> PublicErrorMsg {
        let msg = self
            .public_msg()
            .map_or("no error message provided".into(), |x| x.join(", "));
        PublicErrorMsg(msg)
    }
}

pub struct Res123 {
    content: Option<Bytes>,
}

impl http_body::Body for Res123 {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use Poll::*;
        match self.content.take() {
            Some(x) => Ready(Some(Ok(http_body::Frame::data(x)))),
            None => Ready(None),
        }
    }
}

impl IntoResponse for PublicErrorMsg {
    fn into_response(self) -> axum::response::Response {
        let msgbytes = self.0.as_bytes();
        // let body = axum::body::Bytes::from(msgbytes.to_vec());
        // let body = http_body::Frame::data(body);
        // let body = body.map_err(|_| axum::Error::new(Error::from_string("error while trying to create fixed body")));
        // let body = http_body::combinators::BoxBody::new(body);
        // let body = axum::body::Body::new(body);
        // let x = axum::response::Response::builder().status(500).body(body).unwrap();
        // return x;
        // x
        // let boddat = http_body::Empty::new();
        let res: Res123 = Res123 {
            content: Some(Bytes::from(self.0.as_bytes().to_vec())),
        };
        let bod = axum::body::Body::new(res);
        // let ret: http::Response<Bytes> = todo!();
        let ret = http::Response::builder().status(500).body(bod).unwrap();
        ret
    }
}

struct CustomErrorResponse(Response);

impl<T> From<T> for CustomErrorResponse
where
    T: ToPublicErrorMsg,
{
    fn from(value: T) -> Self {
        Self(value.to_public_err_msg().into_response())
    }
}

impl IntoResponse for CustomErrorResponse {
    fn into_response(self) -> Response {
        self.0
    }
}

#[derive(Clone)]
pub struct StatsSet {
    daemon: Arc<DaemonStats>,
    ca_conn_set: Arc<CaConnSetStats>,
    ca_conn: Arc<CaConnStats>,
    ca_proto: Arc<CaProtoStats>,
    insert_worker_stats: Arc<InsertWorkerStats>,
    series_by_channel_stats: Arc<SeriesByChannelStats>,
    ioc_finder_stats: Arc<IocFinderStats>,
    insert_frac: Arc<AtomicU64>,
}

impl StatsSet {
    pub fn new(
        daemon: Arc<DaemonStats>,
        ca_conn_set: Arc<CaConnSetStats>,
        ca_conn: Arc<CaConnStats>,
        ca_proto: Arc<CaProtoStats>,
        insert_worker_stats: Arc<InsertWorkerStats>,
        series_by_channel_stats: Arc<SeriesByChannelStats>,
        ioc_finder_stats: Arc<IocFinderStats>,
        insert_frac: Arc<AtomicU64>,
    ) -> Self {
        Self {
            daemon,
            ca_conn_set,
            ca_conn,
            ca_proto,
            insert_worker_stats,
            series_by_channel_stats,
            ioc_finder_stats,
            insert_frac,
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

async fn always_error(params: HashMap<String, String>) -> Result<axum::Json<bool>, Response> {
    Err(Error::with_public_msg_no_trace("The-public-message")
        .to_public_err_msg()
        .into_response())
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
    if let Some(name) = params.get("name") {
        // let ch = crate::daemon_common::Channel::new(name.into());
        let ch_cfg = ChannelConfig::st_monitor(name);
        let (tx, rx) = async_channel::bounded(1);
        let ev = DaemonEvent::ChannelAdd(ch_cfg, tx);
        dcom.tx.send(ev).await?;
        match rx.recv().await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(Error::with_msg_no_trace(format!("{e}"))),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e}"))),
        }
    } else {
        Err(Error::with_msg_no_trace(format!("wrong parameters given")))
    }
}

async fn channel_add(params: HashMap<String, String>, dcom: Arc<DaemonComm>) -> Result<axum::Json<bool>, Response> {
    match channel_add_inner(params, dcom).await {
        Ok(_) => Ok(axum::Json::from(true)),
        Err(e) => Err(e.to_public_err_msg().into_response()),
    }
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

// ChannelStatusesResponse
// BTreeMap<String, ChannelState>
async fn private_channel_states(
    params: HashMap<String, String>,
    tx: Sender<CaConnSetEvent>,
) -> axum::Json<BTreeMap<String, ChannelState>> {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    let limit = params
        .get("limit")
        .map(|x| x.parse().ok())
        .unwrap_or(None)
        .unwrap_or(40);
    let (tx2, rx2) = async_channel::bounded(1);
    let req = ChannelStatusesRequest { name, limit, tx: tx2 };
    let item = CaConnSetEvent::ConnSetCmd(ConnSetCmd::ChannelStatuses(req));
    // TODO handle error
    tx.send(item).await.unwrap();
    let res = rx2.recv().await.unwrap();
    axum::Json(res.channels_ca_conn_set)
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

fn metricbeat(stats_set: &StatsSet) -> axum::Json<serde_json::Value> {
    let mut map = serde_json::Map::new();
    map.insert("daemon".to_string(), stats_set.daemon.json());
    map.insert("insert_worker_stats".to_string(), stats_set.insert_worker_stats.json());
    let mut ret = serde_json::Map::new();
    ret.insert("daqingest".to_string(), serde_json::Value::Object(map));
    axum::Json(serde_json::Value::Object(ret))
}

fn metrics(stats_set: &StatsSet) -> String {
    let s1 = stats_set.daemon.prometheus();
    let s2 = stats_set.ca_conn_set.prometheus();
    let s3 = stats_set.insert_worker_stats.prometheus();
    let s4 = stats_set.ca_conn.prometheus();
    let s5 = stats_set.series_by_channel_stats.prometheus();
    let s6 = stats_set.ca_proto.prometheus();
    let s7 = stats_set.ioc_finder_stats.prometheus();
    [s1, s2, s3, s4, s5, s6, s7].join("")
}

pub struct RoutesResources {
    backend: String,
    worker_tx: Sender<ChannelInfoQuery>,
    iqtx: InsertQueuesTx,
    scyconf_st: ScyllaIngestConfig,
    scyconf_mt: ScyllaIngestConfig,
    scyconf_lt: ScyllaIngestConfig,
}

impl RoutesResources {
    pub fn new(
        backend: String,
        worker_tx: Sender<ChannelInfoQuery>,
        iqtx: InsertQueuesTx,
        scyconf_st: ScyllaIngestConfig,
        scyconf_mt: ScyllaIngestConfig,
        scyconf_lt: ScyllaIngestConfig,
    ) -> Self {
        Self {
            backend,
            worker_tx,
            iqtx,
            scyconf_st,
            scyconf_mt,
            scyconf_lt,
        }
    }
}

fn make_routes(
    rres: Arc<RoutesResources>,
    dcom: Arc<DaemonComm>,
    connset_cmd_tx: Sender<CaConnSetEvent>,
    stats_set: StatsSet,
) -> axum::Router {
    use axum::extract;
    use axum::routing::{get, post, put};
    use axum::Router;
    use http::StatusCode;

    Router::new()
        .fallback(|req: Request<axum::body::Body>| async move {
            info!("Fallback for {} {}", req.method(), req.uri());
            StatusCode::NOT_FOUND
        })
        .nest(
            "/daqingest/some",
            Router::new()
                .route("/path1", get(|| async { (StatusCode::OK, format!("Hello there!")) }))
                .route(
                    "/path2",
                    get(|qu: Query<DummyQuery>| async move { (StatusCode::OK, format!("{qu:?}")) }),
                )
                .route("/path3/", get(|| async { (StatusCode::OK, format!("Hello there!")) })),
        )
        .nest(
            "/daqingest",
            Router::new()
                .fallback(|| async { axum::Json(json!({"subcommands":["channel", "metrics"]})) })
                .nest(
                    "/metrics",
                    Router::new().fallback(|| async { StatusCode::NOT_FOUND }).route(
                        "/",
                        get({
                            let stats_set = stats_set.clone();
                            || async move { metrics(&stats_set) }
                        }),
                    ),
                )
                .nest(
                    "/channel",
                    Router::new()
                        .fallback(|| async { axum::Json(json!({"subcommands":["states"]})) })
                        .route(
                            "/states",
                            get({
                                let tx = connset_cmd_tx.clone();
                                |Query(params): Query<HashMap<String, String>>| status::channel_states(params, tx)
                            }),
                        )
                        .route(
                            "/add",
                            get({
                                let dcom = dcom.clone();
                                |Query(params): Query<HashMap<String, String>>| channel_add(params, dcom)
                            }),
                        )
                        .route(
                            "/remove",
                            get({
                                let dcom = dcom.clone();
                                |Query(params): Query<HashMap<String, String>>| channel_remove(params, dcom)
                            }),
                        ),
                )
                .nest(
                    "/ingest",
                    Router::new().route(
                        "/v1",
                        post({
                            let rres = rres.clone();
                            move |(headers, params, body): (
                                HeaderMap,
                                Query<HashMap<String, String>>,
                                axum::body::Body,
                            )| { ingest::post_v01((headers, params, body), rres) }
                        }),
                    ),
                )
                .nest(
                    "/private",
                    Router::new()
                        .nest(
                            "/channel",
                            Router::new().route(
                                "/delete",
                                post({
                                    let rres = rres.clone();
                                    move |(headers, params, body): (
                                        HeaderMap,
                                        Query<HashMap<String, String>>,
                                        axum::body::Body,
                                    )| {
                                        delete::delete((headers, params, body), rres)
                                    }
                                }),
                            ),
                        )
                        .route(
                            "/channel/states",
                            get({
                                let tx = connset_cmd_tx.clone();
                                |Query(params): Query<HashMap<String, String>>| private_channel_states(params, tx)
                            }),
                        ),
                ),
        )
        .route(
            "/daqingest/metricbeat",
            get({
                let stats_set = stats_set.clone();
                || async move { metricbeat(&stats_set) }
            }),
        )
        .route(
            "/daqingest/always-error/",
            get(|Query(params): Query<HashMap<String, String>>| always_error(params)),
        )
        .route(
            "/daqingest/find/channel",
            get({
                let dcom = dcom.clone();
                |Query(params): Query<HashMap<String, String>>| find_channel(params, dcom)
            }),
        )
        .route(
            "/daqingest/store_workers_rate",
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
            "/daqingest/insert_frac",
            get({
                let insert_frac = stats_set.insert_frac.clone();
                || async move { axum::Json(insert_frac.load(Ordering::Acquire)) }
            })
            .put({
                let insert_frac = stats_set.insert_frac.clone();
                |v: extract::Json<u64>| async move {
                    insert_frac.store(v.0, Ordering::Release);
                }
            }),
        )
        .route(
            "/daqingest/extra_inserts_conf",
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
            "/daqingest/insert_ivl_min",
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
    rres: Arc<RoutesResources>,
) -> Result<(), Error> {
    info!("metrics service start  {bind_to}");
    let addr: SocketAddr = bind_to.parse().map_err(Error::from_string)?;
    let router = make_routes(rres, dcom, connset_cmd_tx, stats_set).into_make_service();
    let listener = TcpListener::bind(addr).await?;
    // into_make_service_with_connect_info
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_signal.recv().await;
        })
        .await?;
    Ok(())
}

pub async fn metrics_agg_task(local_stats: Arc<CaConnStats>, store_stats: Arc<CaConnStats>) -> Result<(), Error> {
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
        #[cfg(DISABLED)]
        {
            let mut m = METRICS.lock().unwrap();
            *m = Some(agg.clone());
            if false {
                let diff = CaConnStatsAggDiff::diff_from(&agg_last, &agg);
                info!("{}", diff.display());
            }
        }
        agg_last = agg;
    }
}
