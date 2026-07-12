use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use redis::RedisResult;
use sqlx;
use tracing::{debug, error, info, warn};

use crate::app::jobs::ForgottenJob;
use crate::db::redis::redis_stream_enqueue;
use crate::state::app_state::{AppState, init_state};

pub async fn run_fault_tolerance() {
    let state = init_state().await;

    tokio::spawn(recover_jobs_redis_write_fail(state));
    tokio::spawn(recover_stuck_jobs(state));
    tokio::spawn(check_node_heartbeat(state));

    std::future::pending::<()>().await;
}

pub async fn check_node_heartbeat(state: &AppState) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let now = Utc::now();

        let expired_heartbeat = now - Duration::seconds(15);

        let down_nodes: Vec<String> = match sqlx::query_scalar::<_, String>(
            r#"
            SELECT node_id
            FROM saturn_nodes
            WHERE last_heartbeat_at <= $1
            "#,
        )
        .bind(expired_heartbeat)
        .fetch_all(&state.db)
        .await
        {
            Ok(nodes) => nodes,
            Err(e) => {
                error!("Failed to check node heartbeats: {}", e);
                continue;
            }
        };

        if !down_nodes.is_empty() {
            warn!("Marked nodes as dead: {:?}", down_nodes);
        }
    }
}

pub async fn recover_stuck_jobs(state: &AppState) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let now: DateTime<Utc> = Utc::now();

        let expired_ids: Vec<i32> = match sqlx::query_scalar(
            r#"
            UPDATE pendingjobs
            SET status = 'queued',
                updated_at = $1,
                claimed_by = NULL,
                attempt_id = NULL,
                lease_expires_at = NULL
            WHERE status = 'executing' 
              AND lease_expires_at < now()
            RETURNING id
        "#,
        )
        .bind(now)
        .fetch_all(&state.db)
        .await
        {
            Ok(rows) => {
                if rows.is_empty() {
                    debug!("No jobs to recover");
                } else {
                    info!("Recovered {} jobs", rows.len());
                }
                rows
            }
            Err(e) => {
                error!("Failed to recover jobs: {}", e);
                continue;
            }
        };

        if !expired_ids.is_empty() {
            let mut redis_conn = state.redis.clone();

            for id in expired_ids {
                if let Err(e) = redis_stream_enqueue(&mut redis_conn, id).await {
                    tracing::error!("Failde to enqueue job {id}: {e}");
                }
            }
            info!("Finished replacing failed jobs");
        }
    }
}

pub async fn recover_jobs_redis_write_fail(state: &AppState) {
    loop {
        let forgotten_jobs = match sqlx::query_as::<_, ForgottenJob>(
            r#"
            SELECT id, scheduled_for
            FROM pendingjobs
            WHERE status = 'pending' AND redis_indexed_at IS NULL AND created_at < NOW() - interval '5 seconds'
            LIMIT 100;
            "#,
        )
        .fetch_all(&state.db)
        .await
        {
            Ok(jobs) => jobs,
            Err(e) => {
                error!("Unable to get forgotten jobs: {:?}", e);
                vec![]
            }
        };

        if !forgotten_jobs.is_empty() {
            let mut redis = state.redis.clone();

            for job in forgotten_jobs {
                let score = job.scheduled_for.timestamp();

                match redis
                    .zadd::<_, _, _, ()>("pending_jobs", job.id, score)
                    .await
                {
                    Ok(_) => {
                        if let Err(e) = sqlx::query(
                            r#"
                            UPDATE pendingjobs
                            SET redis_indexed_at = NOW()
                            WHERE id = $1
                            "#,
                        )
                        .bind(job.id)
                        .execute(&state.db)
                        .await
                        {
                            error!("failed to makr job {} as indexed: {e}", job.id);
                        }
                    }

                    Err(e) => {
                        error!("Failed to index job {}: {}", job.id, e);
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn recover_pending_stream_jobs(
    state: AppState,
    worker_id: &str,
) -> RedisResult<Vec<(String, i32)>> {
    let mut redis = state.redis;
    let reply: redis::streams::StreamAutoClaimReply = redis
        .xautoclaim_options(
            "ready_jobs",
            "workers",
            worker_id,
            30_000,
            "0-0",
            redis::streams::StreamAutoClaimOptions::default().count(10),
        )
        .await?;

    let mut jobs = Vec::new();

    for message in reply.claimed {
        let stream_id = message.id;

        let Some(job_id_value) = message.map.get("job_id") else {
            continue;
        };

        let job_id: i32 = redis::from_redis_value(job_id_value.clone())?;

        jobs.push((stream_id, job_id))
    }

    Ok(jobs)
}
