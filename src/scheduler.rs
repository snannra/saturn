use super::{STATE, jobs::JobToExecute};
use axum::http::StatusCode;
use chrono::Utc;
use crossbeam::channel::Sender;
use redis::{self, AsyncCommands};
use sqlx;

pub async fn poll(tx: Sender<JobToExecute>) {
    let state = STATE.get().unwrap().clone();

    let mut redis_conn = state
        .redis
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| {
            eprintln!("redis connection failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .unwrap();

    loop {
        let now = Utc::now().timestamp();

        let job_ids: Vec<i32> = match redis_conn.zrangebyscore("pending_jobs", 0, now).await {
            Ok(ids) => {
                println!("found job ids: {:?}", ids);
                ids
            }
            Err(_) => vec![],
        };

        if !job_ids.is_empty() {
            let _: () = redis_conn.zrem("pending_jobs", &job_ids).await.unwrap();

            let jobs: Vec<JobToExecute> = sqlx::query_as::<_, JobToExecute>(
                r#"
                    UPDATE pendingjobs
                    SET status = 'executing'
                    WHERE id = ANY($1) AND status = 'pending'
                    RETURNING id, job_data
                    "#,
            )
            .bind(&job_ids)
            .fetch_all(&state.db)
            .await
            .unwrap();

            jobs.into_iter().for_each(|job| {
                let _ = tx.send(job);
            });
        }
    }
}
