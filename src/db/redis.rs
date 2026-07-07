use crate::state::config::Config;
use redis::{self, AsyncCommands, RedisResult, aio::MultiplexedConnection};

pub async fn redis_stream_enqueue(job_id: i32) -> Result<(), String> {
    Ok(())
}

pub async fn create_redis_conn(config: &Config) -> MultiplexedConnection {
    let redis_client =
        redis::Client::open(&*config.redis_url).expect("failed to create redis client");

    redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

pub async fn init_stream_group(redis: &mut MultiplexedConnection) -> RedisResult<()> {
    let result: RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("ready_jobs")
        .arg("workers")
        .arg("0")
        .arg("MKSTREAM")
        .query_async(redis)
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
        Err(e) => Err(e),
    }
}
