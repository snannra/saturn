use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use sqlx;
use tracing::{debug, error, info};

use crate::STATE;

pub async fn recover_stuck_jobs() {
    let state = STATE.get().unwrap();

    loop {
        let now = Utc::now();

        let expired = now - Duration::seconds(30);

        let expired_ids: Vec<(i32, DateTime<Utc>)> = match sqlx::query_as(
            r#"
            UPDATE pendingjobs
            SET status = 'pending',
                updated_at = $1
            WHERE status = 'executing' 
              AND updated_at < $2
            RETURNING id, scheduled_for
        "#,
        )
        .bind(now)
        .bind(expired)
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

            for (id, scheduled) in expired_ids {
                let _: () = redis_conn
                    .zadd("pending_jobs", id, scheduled.timestamp())
                    .await
                    .map_err(|e| {
                        error!("redis zadd failed: {e}");
                    })
                    .unwrap();
            }
            info!("Finished replacing failed jobs");
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
