use crate::ca::conn::ConnCommand;
use crate::ca::{CommandQueueSet, IngestCommons};
use axum::extract::Query;
use http::request::Parts;
use log::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[allow(unused)]
#[derive(Debug, Deserialize)]
struct PromLabels {
    start: Option<String>,
    end: Option<String>,
    //#[serde(rename = "match[]")]
    //pattern: Option<Vec<String>>,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
struct PromLabelValues {
    start: Option<String>,
    end: Option<String>,
    //#[serde(rename = "match[]")]
    //pattern: Option<Vec<String>>,
}

async fn get_empty() -> String {
    format!("")
}

async fn channel_add(params: HashMap<String, String>, ingest_commons: Arc<IngestCommons>) -> String {
    if let (Some(backend), Some(name)) = (params.get("backend"), params.get("name")) {
        // TODO look up the address.
        match crate::ca::find_channel_addr(backend.into(), name.into(), &ingest_commons.pgconf).await {
            Ok(Some(addr)) => {
                if ingest_commons
                    .command_queue_set
                    .queues()
                    .lock()
                    .await
                    .contains_key(&addr)
                {
                } else {
                    match crate::ca::create_ca_conn(
                        addr,
                        ingest_commons.local_epics_hostname.clone(),
                        256,
                        32,
                        ingest_commons.insert_item_queue.clone(),
                        ingest_commons.data_store.clone(),
                        ingest_commons.insert_ivl_min.clone(),
                        ingest_commons.conn_stats.clone(),
                        ingest_commons.command_queue_set.clone(),
                    )
                    .await
                    {
                        Ok(_) => {
                            // TODO keep the join handle.
                        }
                        Err(_) => {
                            error!("can not create CaConn");
                        }
                    }
                }
                if let Some(tx) = ingest_commons.command_queue_set.queues().lock().await.get(&addr) {
                    let (cmd, rx) = ConnCommand::channel_add(name.into());
                    if let Err(_) = tx.send(cmd).await {
                        error!("can not send command");
                        "false".into()
                    } else {
                        match rx.recv().await {
                            Ok(x) => {
                                if x {
                                    "true".into()
                                } else {
                                    "false".into()
                                }
                            }
                            Err(_) => "false".into(),
                        }
                    }
                } else {
                    error!("Even after create, can not locate the connection.");
                    "false".into()
                }
            }
            _ => {
                error!("can not find addr for channel");
                "false".into()
            }
        }
    } else {
        "false".into()
    }
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
    let backend = if let Some(x) = params.get("backend") {
        x
    } else {
        return Json(Value::Bool(false));
    };
    let name = if let Some(x) = params.get("name") {
        x
    } else {
        return Json(Value::Bool(false));
    };
    if let Some(tx) = ingest_commons.command_queue_set.queues().lock().await.get(&addr) {
        // TODO any need to check the backend here?
        let _ = backend;
        let (cmd, rx) = ConnCommand::channel_remove(name.into());
        if let Err(_) = tx.send(cmd).await {
            error!("can not send command");
            Json(Value::Bool(false))
        } else {
            match rx.recv().await {
                Ok(x) => Json(Value::Bool(x)),
                Err(_) => Json(Value::Bool(false)),
            }
        }
    } else {
        Json(Value::Bool(false))
    }
}

async fn prom_query(
    Query(params): Query<HashMap<String, String>>,
    parts: Parts,
    body: bytes::Bytes,
) -> axum::Json<serde_json::Value> {
    use axum::Json;
    info!("/api/v1/query  params {:?}  {:?}", params, parts);
    let url = url::Url::parse(&format!("dummy://{}", &parts.uri));
    info!("/api/v1/query  parsed url: {:?}", url);
    let body_str = String::from_utf8_lossy(&body);
    info!("/api/v1/query  body_str: {:?}", body_str);
    let formurl = url::Url::parse(&format!("dummy:///?{}", body_str));
    info!("/api/v1/query  formurl: {:?}", formurl);
    let res = serde_json::json!({
        "status": "success",
        "data": {
            "resultType": "scalar",
            "result": [40, "2"]
        }
    });
    Json(res)
}

async fn prom_query_range(
    Query(params): Query<HashMap<String, String>>,
    parts: Parts,
    body: bytes::Bytes,
) -> axum::Json<serde_json::Value> {
    use axum::Json;
    info!("/api/v1/query_range  {:?}   Query(params) {:?}", parts, params);
    let url = url::Url::parse(&format!("dummy://{}", &parts.uri));
    info!("/api/v1/query_range  parsed url: {:?}", url);
    let body_str = String::from_utf8_lossy(&body);
    info!("/api/v1/query_range  body_str: {:?}", body_str);
    let formurl = url::Url::parse(&format!("dummy:///?{}", body_str));
    info!("/api/v1/query_range  formurl: {:?}", formurl);
    let res = serde_json::json!({
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "__name__": "series1",
                    },
                    "values": [
                    ]
                }
            ]
        }
    });
    Json(res)
}

pub async fn start_metrics_service(
    bind_to: String,
    insert_frac: Arc<AtomicU64>,
    insert_ivl_min: Arc<AtomicU64>,
    command_queue_set: Arc<CommandQueueSet>,
    ingest_commons: Arc<IngestCommons>,
) {
    use axum::routing::{get, post, put};
    use axum::Form;
    use axum::{extract, Router};
    let app = Router::new()
        .route(
            "/metrics",
            get(|| async {
                let stats = crate::ca::get_metrics();
                match stats {
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
                let command_queue_set = command_queue_set.clone();
                |Query(params): Query<HashMap<String, String>>| async move {
                    let pattern = params.get("pattern").map_or(String::new(), |x| x.clone()).to_string();
                    let g = command_queue_set.queues().lock().await;
                    let mut rxs = Vec::new();
                    for (_, tx) in g.iter() {
                        let (cmd, rx) = ConnCommand::find_channel(pattern.clone());
                        rxs.push(rx);
                        if let Err(_) = tx.send(cmd).await {
                            error!("can not send command");
                        }
                    }
                    let mut res = Vec::new();
                    for rx in rxs {
                        let item = rx.recv().await.unwrap();
                        if item.1.len() > 0 {
                            let item = (item.0.to_string(), item.1);
                            res.push(item);
                        }
                    }
                    serde_json::to_string(&res).unwrap()
                }
            }),
        )
        .route(
            "/daqingest/channel/state",
            get({
                let command_queue_set = command_queue_set.clone();
                |Query(params): Query<HashMap<String, String>>| async move {
                    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
                    let g = command_queue_set.queues().lock().await;
                    let mut rxs = Vec::new();
                    for (_, tx) in g.iter() {
                        let (cmd, rx) = ConnCommand::channel_state(name.clone());
                        rxs.push(rx);
                        if let Err(_) = tx.send(cmd).await {
                            error!("can not send command");
                        }
                    }
                    let mut res = Vec::new();
                    for rx in rxs {
                        let item = rx.recv().await.unwrap();
                        if let Some(st) = item.1 {
                            let item = (item.0.to_string(), st);
                            res.push(item);
                        }
                    }
                    serde_json::to_string(&res).unwrap()
                }
            }),
        )
        .route(
            "/daqingest/channel/states",
            get({
                let command_queue_set = command_queue_set.clone();
                |Query(_params): Query<HashMap<String, String>>| async move {
                    let g = command_queue_set.queues().lock().await;
                    let mut rxs = Vec::new();
                    for (_, tx) in g.iter() {
                        let (cmd, rx) = ConnCommand::channel_states_all();
                        rxs.push(rx);
                        if let Err(_) = tx.send(cmd).await {
                            error!("can not send command");
                        }
                    }
                    let mut res = Vec::new();
                    for rx in rxs {
                        let item = rx.recv().await.unwrap();
                        for h in item.1 {
                            res.push((item.0.clone(), h));
                        }
                    }
                    res.sort_unstable_by_key(|(_, v)| v.interest_score as u32);
                    let res: Vec<_> = res.into_iter().rev().take(10).collect();
                    serde_json::to_string(&res).unwrap()
                }
            }),
        )
        .route(
            "/daqingest/channel/add",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| async move { channel_add(params, ingest_commons).await }
            }),
        )
        .route(
            "/daqingest/channel/remove",
            get({
                let ingest_commons = ingest_commons.clone();
                |Query(params): Query<HashMap<String, String>>| async move { channel_remove(params, ingest_commons).await }
            }),
        )
        .route(
            "/insert_frac",
            get(get_empty).put(|v: extract::Json<u64>| async move {
                insert_frac.store(v.0, Ordering::Release);
            }),
        )
        .route(
            "/insert_ivl_min",
            put(|v: extract::Json<u64>| async move {
                insert_ivl_min.store(v.0, Ordering::Release);
            }),
        )
        .route(
            "/api/v1/status/buildinfo",
            get(|| async {
                let res = serde_json::json!({
                  "status": "success",
                  "data": {
                    "version": "2.37",
                    "revision": "daqingest",
                    "branch": "dev",
                    "buildUser": "dominik.werder",
                    "buildDate": "2022-07-21",
                    "goVersion": "nogo"
                  }
                });
                serde_json::to_string(&res).unwrap()
            }),
        )
        .route("/api/v1/query", post(prom_query))
        .route("/api/v1/query_range", post(prom_query_range))
        .route(
            "/api/v1/labels",
            post(|Form(_form): Form<PromLabels>| async move {
                let res = {
                    serde_json::json!({
                        "status": "success",
                        "data": ["__name__", "instance"]
                    })
                };
                serde_json::to_string(&res).unwrap()
            }),
        )
        .route(
            "/api/v1/label/__name__/values",
            get(|| async move {
                let res = {
                    serde_json::json!({
                        "status": "success",
                        "data": ["series1", "series2"]
                    })
                };
                serde_json::to_string(&res).unwrap()
            }),
        )
        .route(
            "/api/v1/label/instance/values",
            get(|| async move {
                let res = {
                    serde_json::json!({
                        "status": "success",
                        "data": ["node1", "node2"]
                    })
                };
                serde_json::to_string(&res).unwrap()
            }),
        )
        .route(
            "/api/v1/metadata",
            get(|| async move {
                let res = {
                    serde_json::json!({
                        "status": "success",
                        "data": {}
                    })
                };
                serde_json::to_string(&res).unwrap()
            }),
        )
        .route(
            "/api/v1/series",
            post(|parts: Parts, body: bytes::Bytes| async move {
                info!("Asked for series, form: {parts:?}");
                let url = url::Url::parse(&format!("http://dummy{}", parts.uri))
                    .unwrap_or_else(|_| url::Url::parse("http://a/").unwrap());
                info!("PARSED SERIES URL {:?}", url);
                let bodyparams = url::Url::parse(&String::from_utf8_lossy(&body));
                info!("BODY PARAMS: {:?}", bodyparams);
                let res = {
                    serde_json::json!({
                        "status": "success",
                        "data": [
                            {
                                "__name__": "series1",
                                "job": "daqingest",
                                "instance": "node1"
                            }
                        ]
                    })
                };
                serde_json::to_string(&res).unwrap()
            }),
        )
        .fallback(
            get(|parts: Parts, body: extract::RawBody<hyper::Body>| async move {
                let bytes = hyper::body::to_bytes(body.0).await.unwrap();
                let s = String::from_utf8_lossy(&bytes);
                info!("GET  {parts:?}  body: {s:?}");
            })
            .post(|parts: Parts, body: extract::RawBody<hyper::Body>| async move {
                let bytes = hyper::body::to_bytes(body.0).await.unwrap();
                let s = String::from_utf8_lossy(&bytes);
                info!("POST  {parts:?}  body: {s:?}");
            })
            .put(|parts: Parts, body: extract::RawBody<hyper::Body>| async move {
                let bytes = hyper::body::to_bytes(body.0).await.unwrap();
                let s = String::from_utf8_lossy(&bytes);
                info!("PUT  {parts:?}  body: {s:?}");
            })
            .delete(|parts: Parts, body: extract::RawBody<hyper::Body>| async move {
                let bytes = hyper::body::to_bytes(body.0).await.unwrap();
                let s = String::from_utf8_lossy(&bytes);
                info!("DELETE  {parts:?}  body: {s:?}");
            }),
        );
    axum::Server::bind(&bind_to.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap()
}
