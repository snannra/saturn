use crate::STATE;
use crate::jobs::JobToExecute;
use axum::http::StatusCode;
use crossbeam::channel::Receiver;
use sqlx;
use std::thread::sleep;
use tokio::time::Duration;

pub async fn worker(rx: Receiver<JobToExecute>) {
    let state = STATE.get().unwrap();

    while let Ok(job) = rx.recv() {
        sqlx::query(
            r#"
            UPDATE pendingjobs
            SET status = 'executing'
            WHERE id = $1
            "#,
        )
        .bind(job.id)
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("execution of job {} failed: {}", job.id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .unwrap();

        println!("Executing job {}: {}", job.id, job.job_data);
        sleep(Duration::from_secs(1));

        sqlx::query(
            r#"
            UPDATE pendingjobs
            SET status = 'complete'
            WHERE id = $1
            "#,
        )
        .bind(job.id)
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("execution of job {} failed: {}", job.id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .unwrap();
    }
}
