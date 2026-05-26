use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use sqlx;

use crate::STATE;

pub async fn recover_stuck_jobs() {
    let state = STATE.get().unwrap();

    loop {
        let now = Utc::now();

        let expired = now - Duration::seconds(30);

        let expired_ids: Vec<(i32, DateTime<Utc>)> = sqlx::query_as(
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
        .unwrap();

        if !expired_ids.is_empty() {
            let mut redis_conn = state
                .redis
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| {
                    eprintln!("redis connection failed: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })
                .unwrap();

            for (id, scheduled) in expired_ids {
                let _: () = redis_conn
                    .zadd("pending_jobs", id, scheduled.timestamp())
                    .await
                    .map_err(|e| {
                        eprintln!("redis zadd failed: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })
                    .unwrap();
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
