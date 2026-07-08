use crate::state::config::Config;
use redis::{
    self, AsyncCommands, RedisResult,
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
};

pub async fn redis_stream_enqueue(
    redis: &mut MultiplexedConnection,
    job_id: i32,
) -> RedisResult<String> {
    redis::cmd("XADD")
        .arg("ready_jobs")
        .arg("*")
        .arg("job_id")
        .arg(job_id)
        .query_async(redis)
        .await
}

pub async fn redis_ack(redis: &mut MultiplexedConnection, stream_id: &str) -> RedisResult<usize> {
    redis.xack("ready_jobs", "workers", &[stream_id]).await
}

pub async fn read_next_job(
    redis: &mut MultiplexedConnection,
    worker_id: &str,
) -> RedisResult<Option<(String, i32)>> {
    let reply: StreamReadReply = redis
        .xread_options(
            &["ready_jobs"],
            &[">"],
            &StreamReadOptions::default()
                .group("workers", worker_id)
                .count(1)
                .block(5000),
        )
        .await?;

    let Some(stream_key) = reply.keys.first() else {
        return Ok(None);
    };

    let Some(message) = stream_key.ids.first() else {
        return Ok(None);
    };

    let stream_id = message.id.clone();

    let Some(job_id_value) = message.map.get("job_id") else {
        return Ok(None);
    };

    let job_id: i32 = redis::from_redis_value(job_id_value.clone())?;

    Ok(Some((stream_id, job_id)))
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
