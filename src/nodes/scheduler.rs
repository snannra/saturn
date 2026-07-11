use crate::db::redis::redis_stream_enqueue;
use crate::nodes::node::{heartbeat, register_node};
use crate::state::app_state::init_state;
use chrono::Utc;
use redis::{self, Script};
use sqlx;
use tracing::error;

pub async fn run_scheduler() {
    let state = init_state().await;
    let node_id = register_node(state, "scheduler".to_string()).await.unwrap();

    tokio::spawn({
        let node_id = node_id.clone();
        async move {
            let _ = heartbeat(&node_id).await;
        }
    });

    poll().await;
}

pub async fn poll() {
    let state = init_state().await.clone();

    let mut redis_conn = state.redis.clone();

    loop {
        let db_now = Utc::now();
        let redis_now = db_now.timestamp();

        let script = Script::new(
            r#"
        local jobs = redis.call(
            "ZRANGEBYSCORE",
            KEYS[1],
            0,
            ARGV[1],
            "LIMIT",
            0,
            ARGV[2]
        )
        
        if #jobs > 0 then
            redis.call("ZREM", KEYS[1], unpack(jobs))
        end
        
        return jobs
        "#,
        );

        let job_ids: Vec<i32> = match script
            .key("pending_jobs")
            .arg(redis_now)
            .arg(100)
            .invoke_async(&mut redis_conn)
            .await
            .unwrap()
        {
            Ok(v) => v,
            Err(_) => vec![],
        };

        if !job_ids.is_empty() {
            let jobs: Vec<i32> = match sqlx::query_scalar::<_, i32>(
                r#"
                    UPDATE pendingjobs
                    SET status = 'queued',
                        updated_at = $2
                    WHERE id = ANY($1) AND status = 'pending'
                    RETURNING id
                    "#,
            )
            .bind(&job_ids)
            .bind(db_now)
            .fetch_all(&state.db)
            .await
            {
                Ok(ids) => ids,
                Err(e) => {
                    error!("SQL job search failed: {e}");
                    continue;
                }
            };

            for job_id in jobs {
                if let Err(e) = redis_stream_enqueue(&mut redis_conn, job_id).await {
                    tracing::error!("Failde to enqueue job {job_id}: {e}");
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
