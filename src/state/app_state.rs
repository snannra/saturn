use crate::app::jobs::JobBatchItem;
use crate::db::{postgres::create_pg_pool, redis::create_redis_conn};
use dotenvy::dotenv;
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::{metrics::registry::setup_metrics, state::config::Config};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: redis::aio::MultiplexedConnection,
    pub config: Config,
    pub prom_handle: PrometheusHandle,
    pub job_tx: tokio::sync::mpsc::Sender<JobBatchItem>,
    pub marker_tx: tokio::sync::mpsc::Sender<i32>,
}

pub async fn init_state() -> (AppState, mpsc::Receiver<JobBatchItem>, mpsc::Receiver<i32>) {
    dotenv().ok();
    let config = Config::from_env();
    let db = create_pg_pool(&config).await;
    let redis = create_redis_conn(&config).await;
    let prom_handle = setup_metrics();
    let (job_tx, job_rx) = mpsc::channel(10_000);
    let (marker_tx, marker_rx) = mpsc::channel(10_000);
    (
        AppState {
            db,
            redis,
            config,
            prom_handle,
            job_tx,
            marker_tx,
        },
        job_rx,
        marker_rx,
    )
}
