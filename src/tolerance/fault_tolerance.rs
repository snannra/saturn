use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use sqlx;
use tracing::{debug, error, info, warn};

use crate::app::jobs::ForgottenJob;
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

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn recover_stuck_jobs(state: &AppState) {
    loop {
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
              AND updated_at < now()
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
                let _: () = redis_conn
                    .lpush("ready_jobs", id)
                    .await
                    .map_err(|e| {
                        error!("Redis insert into redis queue failed: {e}");
                    })
                    .unwrap();
            }
            info!("Finished replacing failed jobs");
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn recover_jobs_redis_write_fail(state: &AppState) {
    loop {
        let forgotten_jobs = match sqlx::query_as::<_, ForgottenJob>(
            r#"
            SELECT id, scheduled_for
            FROM pendingjobs
            WHERE status = 'pending' AND redis_indexed_at IS NULL
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
                        sqlx::query(
                            r#"
                            UPDATE pendingjobs
                            SET redis_indexed_at = NOW()
                            WHERE id = $1
                            "#,
                        )
                        .bind(job.id)
                        .execute(&state.db)
                        .await
                        .unwrap();
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
