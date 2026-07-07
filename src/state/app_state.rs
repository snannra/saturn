use crate::db::{postgres::create_pg_pool, redis::create_redis_conn};
use dotenvy::dotenv;
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::{metrics::registry::setup_metrics, state::config::Config};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: redis::aio::MultiplexedConnection,
    pub config: Config,
    pub prom_handle: PrometheusHandle,
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

            AppState {
                db,
                redis: redis_conn,
                config,
                prom_handle,
            }
        })
        .await
}
