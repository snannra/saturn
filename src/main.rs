use axum::routing::{get, post};
use crossbeam::channel::unbounded;
use tracing::info;

use crate::{
    app_state::{AppState, init_state},
    jobs::JobToExecute,
    metrics::metrics_handler,
};

mod app_state;
mod config;
mod fault_tolerance;
mod jobs;
mod metrics;
mod scheduler;
mod users;
mod worker;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let state = init_state().await;
    info!("AppState Initialized");

    let (tx, rx) = unbounded::<JobToExecute>();

    let sched_count: usize = std::env::var("SCHEDULER_COUNT")
        .unwrap_or("11".to_string())
        .parse()
        .unwrap();
    let worker_count: usize = std::env::var("WORKER_COUNT")
        .unwrap_or("11".to_string())
        .parse()
        .unwrap();
    for _ in 0..sched_count {
        let tx = tx.clone();

        tokio::spawn(async move {
            let _ = scheduler::poll(tx).await;
        });
    }
    info!("Scheduler's spawned");

    for _ in 0..worker_count {
        let rx = rx.clone();

        tokio::spawn(async move {
            let _ = worker::worker(rx).await;
        });

        tokio::spawn(async move {
            let _ = fault_tolerance::recover_stuck_jobs().await;
        });
    }
    info!("Workers Spawned");
    info!("Fault tolerance online");

    let app = axum::Router::new()
        .route("/createjob", post(jobs::create_job))
        .route("/getjob/{id}", get(jobs::get_job))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to Start Server!");

    info!("Listening on localhost:8000");
    axum::serve(listener, app).await.unwrap()
}
