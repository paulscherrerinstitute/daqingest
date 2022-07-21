use crate::ca::conn::ConnCommand;
use crate::ca::CommandQueueSet;
use log::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

async fn get_empty() -> String {
    format!("")
}

pub async fn start_metrics_service(
    bind_to: String,
    insert_frac: Arc<AtomicU64>,
    insert_ivl_min: Arc<AtomicU64>,
    command_queue_set: Arc<CommandQueueSet>,
) {
    use axum::extract::Query;
    use axum::routing::{get, post, put};
    use axum::{extract, Router};
    use http::request::Parts;
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
            get(|Query(params): Query<HashMap<String, String>>| async move {
                let pattern = params.get("pattern").map_or(String::new(), |x| x.clone()).to_string();
                let g = command_queue_set.queues().lock().await;
                let mut rxs = Vec::new();
                for tx in g.iter() {
                    let (cmd, rx) = ConnCommand::find_channel(pattern.clone());
                    rxs.push(rx);
                    if let Err(_) = tx.send(cmd).await {
                        error!("can not send command");
                    }
                }
                let mut res = Vec::new();
                for rx in rxs {
                    let item = rx.recv().await.unwrap();
                    let item = (item.0.to_string(), item.1);
                    if item.1.len() > 0 {
                        res.push(item);
                    }
                }
                serde_json::to_string(&res).unwrap()
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
                    "revision": "aaaaaaaaaaaaaaaaaaaaaaaaa",
                    "branch": "dev",
                    "buildUser": "empty",
                    "buildDate": "2022-07-14",
                    "goVersion": "go1"
                  }
                });
                serde_json::to_string(&res).unwrap()
            }),
        )
        .route(
            "/api/v1/query",
            post(
                |Query(params): Query<HashMap<String, String>>, parts: Parts| async move {
                    info!("/api/v1/query  params {params:?}  {parts:?}");
                },
            ),
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
