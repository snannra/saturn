use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use sqlx;
use tracing::{debug, error, info, warn};

use crate::app_state::STATE;

pub async fn run_fault_tolerance() {
    tokio::spawn(async move {
        let _ = recover_stuck_jobs().await;
    });
}

pub async fn check_node_heartbeat() {
    let state = STATE.get().unwrap();

    loop {
        let now = Utc::now();

        let expired_heartbeat = now - Duration::seconds(15);

        let down_nodes: Vec<String> = match sqlx::query_scalar::<_, String>(
            r#"
            UPDATE saturn_nodes
            WHERE last_heartbeat_at <= $1
            RETURNING node_id
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

pub async fn recover_stuck_jobs() {
    let state = STATE.get().unwrap();

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
