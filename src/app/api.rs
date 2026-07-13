use axum::routing::{get, post};
use tracing::{error, info, warn};

use crate::{
    app::jobs::{batch_flusher, create_job, get_job, marker_flusher},
    db::redis::init_stream_group,
    metrics::registry::metrics_handler,
    state::app_state::init_state,
};

pub async fn run_api() {
    let (state, job_rx, marker_rx) = init_state().await;
    info!("API AppState Initialized");

    let mut redis = state.redis.clone();
    if let Err(e) = init_stream_group(&mut redis).await {
        warn!("init_stream_group: {e}");
    }

    let flusher_handle = tokio::spawn(batch_flusher(job_rx, state.db.clone()));
    let marker_handle = tokio::spawn(marker_flusher(marker_rx, state.db.clone()));

    let app = axum::Router::new()
        .route("/createjob", post(create_job))
        .route("/getjob/{id}", get(get_job))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to start API server");

    info!("API listeneing on localhost:8000");
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!("server error: {e}");
    }

    info!("server drained; flushing remaining batches");
    drop(state);

    match tokio::time::timeout(std::time::Duration::from_secs(10), flusher_handle).await {
        Ok(_) => {}
        Err(_) => error!("batch flusher didn't drain in 10s - a sender leaked"),
    }

    match tokio::time::timeout(std::time::Duration::from_secs(10), marker_handle).await {
        Ok(_) => {}
        Err(_) => error!("marker flusher didn't drain in 10s - a sender leaked"),
    }

    info!("shudown complete");
}

pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {}
    }
}
