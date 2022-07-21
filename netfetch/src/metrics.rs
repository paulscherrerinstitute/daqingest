use log::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub async fn start_metrics_service(bind_to: String, insert_frac: Arc<AtomicU64>, insert_ivl_min: Arc<AtomicU64>) {
    let app = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(|| async {
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
            "/insert_frac",
            axum::routing::put(|v: axum::extract::Json<u64>| async move {
                insert_frac.store(v.0, Ordering::Release);
            }),
        )
        .route(
            "/insert_ivl_min",
            axum::routing::put(|v: axum::extract::Json<u64>| async move {
                insert_ivl_min.store(v.0, Ordering::Release);
            }),
        );
    axum::Server::bind(&bind_to.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap()
}
