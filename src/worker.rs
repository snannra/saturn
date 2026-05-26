use crate::STATE;
use crate::jobs::JobToExecute;
use axum::http::StatusCode;
use chrono::Utc;
use crossbeam::channel::Receiver;
use sqlx;

pub async fn worker(rx: Receiver<JobToExecute>) {
    let state = STATE.get().unwrap();

    while let Ok(job) = rx.recv() {
        println!("Executing job {}: {}", job.id, job.job_data);

        let now = Utc::now().timestamp();

        sqlx::query(
            r#"
            UPDATE pendingjobs
            SET status = 'complete',
                updated_at = $2
            WHERE id = $1 AND STATUS = 'executing'
            "#,
        )
        .bind(job.id)
        .bind(now)
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("execution of job {} failed: {}", job.id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .unwrap();
    }
}
