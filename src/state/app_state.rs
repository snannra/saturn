use crate::app::jobs::{JobBatchItem, batch_flusher, marker_flusher};
use crate::db::{postgres::create_pg_pool, redis::create_redis_conn};
use dotenvy::dotenv;
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use tokio::sync::{OnceCell, mpsc};

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

pub static STATE: OnceCell<AppState> = OnceCell::const_new();

pub async fn init_state() -> &'static AppState {
    STATE
        .get_or_init(|| async {
            dotenv().ok();

            let config = Config::from_env();

            let db = create_pg_pool(&config).await;

            let redis_conn = create_redis_conn(&config).await;

            let prom_handle = setup_metrics();

            let (job_tx, job_rx) = mpsc::channel::<JobBatchItem>(10000);
            let (marker_tx, marker_rx) = mpsc::channel::<i32>(10000);

            tokio::spawn(batch_flusher(job_rx, db.clone()));
            tokio::spawn(marker_flusher(marker_rx, db.clone()));

            AppState {
                db,
                redis: redis_conn,
                config,
                prom_handle,
                job_tx,
                marker_tx,
            }
        })
        .await
}
