use crate::ca::conn::ConnCommand;
use crate::ca::IngestCommons;
use axum::extract::Query;
use http::request::Parts;
use log::*;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

async fn get_empty() -> String {
    String::new()
}

async fn send_command<'a, IT, F, R>(it: &mut IT, cmdgen: F) -> Vec<async_channel::Receiver<R>>
where
    IT: Iterator<Item = (&'a SocketAddrV4, &'a async_channel::Sender<ConnCommand>)>,
    F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
{
    let mut rxs = Vec::new();
    for (_, tx) in it {
        let (cmd, rx) = cmdgen();
        match tx.send(cmd).await {
            Ok(()) => {
                rxs.push(rx);
            }
            Err(e) => {
                error!("can not send command {e:?}");
            }
        }
    }
    rxs
}

async fn find_channel(
    params: HashMap<String, String>,
    ingest_commons: Arc<IngestCommons>,
) -> axum::Json<Vec<(String, Vec<String>)>> {
    let pattern = params.get("pattern").map_or(String::new(), |x| x.clone()).to_string();
    let g = ingest_commons.command_queue_set.queues_locked().await;
    let mut it = g.iter();
    let rxs = send_command(&mut it, || ConnCommand::find_channel(pattern.clone())).await;
    let mut res = Vec::new();
    for rx in rxs {
        let item = rx.recv().await.unwrap();
        if item.1.len() > 0 {
            let item = (item.0.to_string(), item.1);
            res.push(item);
        }
    }
    axum::Json(res)
}

async fn channel_add(params: HashMap<String, String>, ingest_commons: Arc<IngestCommons>) -> String {
    if let (Some(backend), Some(name)) = (params.get("backend"), params.get("name")) {
        // TODO look up the address.
        match crate::ca::find_channel_addr(backend.into(), name.into(), &ingest_commons.pgconf).await {
            Ok(Some(addr)) => {
                if ingest_commons
                    .command_queue_set
                    .queues_locked()
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
                if let Some(tx) = ingest_commons.command_queue_set.queues_locked().await.get(&addr) {
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
    if let Some(tx) = ingest_commons.command_queue_set.queues_locked().await.get(&addr) {
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

async fn channel_state(params: HashMap<String, String>, ingest_commons: Arc<IngestCommons>) -> String {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    let g = ingest_commons.command_queue_set.queues_locked().await;
    let mut rxs = Vec::new();
    for (_, tx) in g.iter() {
        let (cmd, rx) = ConnCommand::channel_state(name.clone());
        match tx.send(cmd).await {
            Ok(()) => {
                rxs.push(rx);
            }
            Err(e) => {
                error!("can not send command {e:?}");
            }
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

async fn channel_states(
    _params: HashMap<String, String>,
    ingest_commons: Arc<IngestCommons>,
) -> axum::Json<Vec<crate::ca::conn::ChannelStateInfo>> {
    let g = ingest_commons.command_queue_set.queues_locked().await;
    let mut rxs = Vec::new();
    for (_, tx) in g.iter() {
        let (cmd, rx) = ConnCommand::channel_states_all();
        match tx.send(cmd).await {
            Ok(()) => {
                rxs.push(rx);
            }
            Err(e) => {
                error!("can not send command {e:?}");
            }
        }
    }
    let mut res = Vec::new();
    for rx in rxs {
        let item = rx.recv().await.unwrap();
        for h in item.1 {
            res.push(h);
        }
    }
    res.sort_unstable_by_key(|v| u32::MAX - v.interest_score as u32);
    //let res: Vec<_> = res.into_iter().rev().take(10).collect();
    axum::Json(res)
}

pub async fn start_metrics_service(
    bind_to: String,
    insert_frac: Arc<AtomicU64>,
    insert_ivl_min: Arc<AtomicU64>,
    ingest_commons: Arc<IngestCommons>,
) {
    use axum::routing::{get, put};
    use axum::{extract, Router};
    let app = Router::new()
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
