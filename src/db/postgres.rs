use crate::state::config::Config;
use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pg_pool(config: &Config) -> PgPool {
    PgPoolOptions::new()
        .max_connections(100)
        .connect(&config.postgres_url)
        .await
        .expect("failed to connect to postgres")
}
