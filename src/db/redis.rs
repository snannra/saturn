use crate::state::config::Config;
use redis::{self, aio::MultiplexedConnection};

pub async fn create_redis_conn(config: &Config) -> MultiplexedConnection {
    let redis_client =
        redis::Client::open(&*config.redis_url).expect("failed to create redis client");

    redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
