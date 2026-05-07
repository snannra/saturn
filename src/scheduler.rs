use super::{STATE, jobs::JobToExecute};
use axum::http::StatusCode;
use chrono::Utc;
use redis::{self, AsyncCommands};
use sqlx;
use std::thread::sleep;
use tokio::{sync::mpsc, time::Duration};

pub async fn poll(tx: mpsc::UnboundedSender<JobToExecute>) {
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

    let now = Utc::now().timestamp();

    loop {
        let job_ids: Vec<i32> = redis_conn
            .zrangebyscore("pending_jobs", 0, "inf")
            .await
            .unwrap();

        println!("{:?}", job_ids);

        if !job_ids.is_empty() {
            let _: () = redis_conn.zrem("pending_jobs", &job_ids).await.unwrap();

            let jobs: Vec<JobToExecute> = sqlx::query_as::<_, JobToExecute>(
                r#"
                    UPDATE pendingjobs
                    SET status = 'executing'
                    WHERE id = ANY($1)
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

        sleep(Duration::from_secs(1));
    }
}
