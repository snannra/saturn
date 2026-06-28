use crate::app_state::create_pg_pool;
use crate::config::Config;
use sqlx;

pub async fn run_migrations() {
    let config = Config::from_env();

    let db = create_pg_pool(&config).await;

    sqlx::migrate!("./migrations")
        .run(&db)
        .await
        .expect("Migration failed");
}
