use axum::routing::{get, post};
use dotenvy::dotenv;
use redis;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio;

use crate::config::Config;

mod config;
mod jobs;
mod users;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: redis::Client,
    pub config: Config,
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let config = Config::from_env();

    let postgres_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.postgres_url)
        .await
        .unwrap();

    let redis_client =
        redis::Client::open(&*config.redis_url).expect("failed to create redis client");

    let state = AppState {
        db: postgres_pool,
        redis: redis_client,
        config,
    };

    let app = axum::Router::new()
        .route("/createjob", post(jobs::create_job))
        .route("/getjob/{id}", get(jobs::get_job))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to Start Server!");

    axum::serve(listener, app).await.unwrap()
}
