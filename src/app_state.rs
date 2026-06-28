use dotenvy::dotenv;
use metrics_exporter_prometheus::PrometheusHandle;
use redis::{self, aio::MultiplexedConnection};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;

use crate::{config::Config, metrics::setup_metrics};

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

pub async fn create_pg_pool(config: &Config) -> PgPool {
    PgPoolOptions::new()
        .max_connections(50)
        .connect(&config.postgres_url)
        .await
        .expect("failed to connect to postgres")
}

pub async fn create_redis_conn(config: &Config) -> MultiplexedConnection {
    let redis_client =
        redis::Client::open(&*config.redis_url).expect("failed to create redis client");

    redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
