use crate::STATE;
use crate::jobs::JobToExecute;
use chrono::Utc;
use crossbeam::channel::Receiver;
use sqlx;
use tracing::{error, info};

pub async fn worker(rx: Receiver<JobToExecute>) {
    let state = STATE.get().unwrap();

    while let Ok(job) = rx.recv() {
        info!("Executing job.{}{}", job.id, job.job_data);

        let now = Utc::now().timestamp();

        match sqlx::query(
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
        {
            Ok(_) => {
                info!("Execution of job {} complete", job.id);
            }
            Err(e) => {
                error!("Execution of job {} failed: {}", job.id, e);
            }
        }
    }
}
