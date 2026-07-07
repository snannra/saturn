use axum::routing::{get, post};
use tracing::info;

use crate::{app::jobs, metrics::registry::metrics_handler, state::app_state::init_state};

pub async fn run_api() {
    let state = init_state().await.clone();
    info!("API AppState Initialized");

    let app = axum::Router::new()
        .route("/createjob", post(jobs::create_job))
        .route("/getjob/{id}", get(jobs::get_job))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to start API server");

    info!("API listeneing on localhost:8000");
    axum::serve(listener, app).await.unwrap();
}
