use axum::routing::{get, post};
use crossbeam::channel::unbounded;
use dotenvy::dotenv;
use redis;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;

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
    pub redis: redis::Client,
    pub config: Config,
}

static STATE: OnceCell<AppState> = OnceCell::const_new();

pub async fn init_state() -> &'static AppState {
    STATE
        .get_or_init(|| async {
            dotenv().ok();

            let config = Config::from_env();

            let db = PgPoolOptions::new()
                .max_connections(10)
                .connect(&config.postgres_url)
                .await
                .unwrap();

            let redis =
                redis::Client::open(&*config.redis_url).expect("failed to create redis client");

            AppState { db, redis, config }
        })
        .await
}

#[tokio::main]
async fn main() {
    let state = init_state().await;

    let (tx, rx) = unbounded::<JobToExecute>();

    for _ in 0..11 {
        let tx = tx.clone();

        tokio::spawn(async move {
            let _ = scheduler::poll(tx).await;
        });
    }

    for _ in 0..11 {
        let rx = rx.clone();

        tokio::spawn(async move {
            let _ = worker::worker(rx).await;
        });

        tokio::spawn(async move {
            let _ = fault_tolerance::recover_stuck_jobs().await;
        });
    }

    let app = axum::Router::new()
        .route("/createjob", post(jobs::create_job))
        .route("/getjob/{id}", get(jobs::get_job))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to Start Server!");

    axum::serve(listener, app).await.unwrap()
}
