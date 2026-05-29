use super::{STATE, jobs::JobToExecute};
use axum::http::StatusCode;
use chrono::Utc;
use crossbeam::channel::Sender;
use redis::{self, AsyncCommands};
use sqlx;
use tracing::{error, info};

pub async fn poll(tx: Sender<JobToExecute>) {
    let state = STATE.get().unwrap().clone();

    let mut redis_conn = state
        .redis
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| {
            error!("Redis Connection Failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .unwrap();

    loop {
        let db_now = Utc::now();
        let redis_now = db_now.timestamp();

        let job_ids: Vec<i32> = match redis_conn.zrangebyscore("pending_jobs", 0, redis_now).await {
            Ok(ids) => {
                info!("Found job Ids: {:?}", ids);
                ids
            }
            Err(_) => vec![],
        };

        if !job_ids.is_empty() {
            let _: () = redis_conn.zrem("pending_jobs", &job_ids).await.unwrap();

            let jobs: Vec<JobToExecute> = match sqlx::query_as::<_, JobToExecute>(
                r#"
                    UPDATE pendingjobs
                    SET status = 'executing',
                        updated_at = $2
                    WHERE id = ANY($1) AND status = 'pending'
                    RETURNING id, job_data
                    "#,
            )
            .bind(&job_ids)
            .bind(db_now)
            .fetch_all(&state.db)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    error!("SQL job search failed: {e}");
                    continue;
                }
            };

            jobs.into_iter().for_each(|job| {
                let _ = tx.send(job);
            });
        }
    }
}
