use crate::STATE;
use crate::jobs::JobToExecute;
use axum::http::StatusCode;
use sqlx;
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn worker(mut rx: UnboundedReceiver<JobToExecute>) {
    let state = STATE.get().unwrap();

    while let Some(job) = rx.recv().await {
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

        println!("Executing Job {}: {}", job.id, job.job_data);

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
