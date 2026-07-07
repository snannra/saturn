use crate::{
    app::jobs::JobToExecute,
    nodes::node::{heartbeat, register_node},
    state::app_state::{AppState, init_state},
};
use chrono::{Duration, Utc};
use redis::AsyncCommands;
use sqlx;
use tracing::{error, info};
use uuid::Uuid;

pub async fn run_worker() {
    let state = init_state().await;
    let node_id = register_node(state, "worker".to_string()).await.unwrap();

    tokio::spawn({
        let node_id = node_id.clone();
        async move {
            let _ = heartbeat(&node_id).await;
        }
    });

    worker(&node_id).await;
}

pub async fn worker(node_id: &str) {
    let state = init_state().await;

    let mut redis_conn = state.redis.clone();

    loop {
        match redis_conn.brpop("ready_jobs", 5.0).await {
            Ok(Some((_queue, job_id))) => handle_worker(_queue, job_id, node_id, state).await,
            Ok(None) => continue,
            Err(err) => {
                tracing::warn!("redis brpop error: {:?}", err);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        }
    }
}

pub async fn handle_worker(_queue: String, job_id: i32, node_id: &str, state: &AppState) {
    let mut now = Utc::now();
    let attempt_lease = now + Duration::seconds(10);

    let attempt_uid = Uuid::new_v4().to_string();

    let claimed = sqlx::query_as::<_, JobToExecute>(
            r#"
            UPDATE pendingjobs
            SET status = 'executing', updated_at = $2, claimed_by = $3, attempt_id = $4, lease_expires_at = $5
            WHERE ID = $1 AND status = 'queued'
            RETURNING id, job_data
            "#,
        )
        .bind(job_id)
        .bind(now)
        .bind(&node_id)
        .bind(&attempt_uid)
        .bind(attempt_lease)
        .fetch_optional(&state.db)
        .await;

    let Some(JobToExecute { id, job_data }) = claimed.unwrap() else {
        info!("Job {} was not claimable, skipping", job_id);
        return;
    };

    info!("Executing job {}.", id);

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let renew_state = state.clone();
    let renew_job_id = job_id;
    let renew_node_id = node_id.to_owned().clone();
    let renew_attempt_id = attempt_uid.clone();

    let renew_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let new_lease = Utc::now() + chrono::Duration::seconds(10);

                    let result = sqlx::query(
                        r#"
                            UPDATE pendingjobs
                            SET lease_expires_at = $4, updated_at = now()
                            WHERE id = $1
                                AND claimed_by = $2
                                AND attempt_id = $3
                                AND status = 'executing'
                            "#,
                    )
                    .bind(renew_job_id)
                    .bind(&renew_node_id)
                    .bind(&renew_attempt_id)
                    .bind(new_lease)
                    .execute(&renew_state.db)
                    .await;

                    match result {
                        Ok(done) if done.rows_affected() == 1 => {
                            info!("Lease renewed for job {} by node {}", renew_job_id, renew_node_id)
                        }
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            error!("Failed to renew lease for job {}: {}", renew_job_id, e);
                            break;
                        }
                    }
                }
                _ = &mut stop_rx => {
                    break;
                }
            }
        }
    });

    // Job Completed
    let _ = stop_tx.send(());
    let _ = renew_handle.await;

    now = Utc::now();

    let result = sqlx::query(
        r#"UPDATE pendingjobs
            SET status = 'complete', updated_at = $2, lease_expires_at = NULL
            WHERE id = $1 AND status = 'executing' AND attempt_id = $3 AND claimed_by = $4"#,
    )
    .bind(job_id)
    .bind(now)
    .bind(attempt_uid)
    .bind(&node_id)
    .execute(&state.db)
    .await
    .unwrap();

    if result.rows_affected() == 0 {
        error!("Worker lost ownership of job {}", job_id);
    }
}
