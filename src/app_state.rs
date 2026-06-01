use dotenvy::dotenv;
use metrics_exporter_prometheus::PrometheusHandle;
use redis;
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

            let db = PgPoolOptions::new()
                .max_connections(50)
                .connect(&config.postgres_url)
                .await
                .unwrap();

            sqlx::migrate!("./migrations")
                .run(&db)
                .await
                .expect("Migration failed");

            let redis_client =
                redis::Client::open(&*config.redis_url).expect("failed to create redis client");

            let redis_conn = redis_client
                .get_multiplexed_async_connection()
                .await
                .unwrap();

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
