use axum::routing::{get, post};
use crossbeam::channel::unbounded;
use dotenvy::dotenv;
use redis;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;
use tracing::info;

use crate::{config::Config, jobs::JobToExecute};

mod config;
mod fault_tolerance;
mod jobs;
mod scheduler;
mod users;
mod worker;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: redis::aio::MultiplexedConnection,
    pub config: Config,
}

static STATE: OnceCell<AppState> = OnceCell::const_new();

pub async fn init_state() -> &'static AppState {
    STATE
        .get_or_init(|| async {
            dotenv().ok();

            let config = Config::from_env();

            let db = PgPoolOptions::new()
                .max_connections(50)
                .connect(&config.postgres_url)
                .await
                .unwrap();

            let redis_client =
                redis::Client::open(&*config.redis_url).expect("failed to create redis client");

            let redis_conn = redis_client
                .get_multiplexed_async_connection()
                .await
                .unwrap();

            AppState {
                db,
                redis: redis_conn,
                config,
            }
        })
        .await
}

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
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to Start Server!");

    info!("Listening on localhost:8000");
    axum::serve(listener, app).await.unwrap()
}
