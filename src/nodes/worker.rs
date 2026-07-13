use crate::{
    app::{
        api::shutdown_signal,
        error::{JobError, JobOutcome},
        jobs::JobToExecute,
    },
    db::redis::{read_next_job, redis_ack},
    state::app_state::{AppState, init_state},
    tolerance::fault_tolerance::recover_pending_stream_jobs,
};
use chrono::{Duration, Utc};
use metrics::counter;
use redis::aio::MultiplexedConnection;
use sqlx;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub async fn run_worker() {
    let (state, _job_rx, _marker_rx) = init_state().await;

    let shutdown = CancellationToken::new();
    let node_id = Uuid::new_v4().to_string();

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            info!("shutdown signal received");
            shutdown.cancel();
        }
    });

    let recovery_handle = tokio::spawn({
        let mut state = state.clone();
        let node_id = node_id.clone();
        let shutdown = shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {}
                }
                match recover_pending_stream_jobs(&mut state, &node_id).await {
                    Ok(jobs) => {
                        for (sid, jid) in jobs {
                            if shutdown.is_cancelled() {
                                break;
                            }
                            handle_worker(sid, jid, &node_id, &state).await;
                        }
                    }
                    Err(e) => error!("stream recovery failed: {e}"),
                }
            }
        }
    });

    worker(&node_id, &state, shutdown).await;

    let _ = recovery_handle.await;
    info!("worker shutdown complete");
}

pub async fn worker(node_id: &str, state: &AppState, shutdown: CancellationToken) {
    let mut redis_conn = state.redis.clone();

    loop {
        let next = tokio::select! {
            _ = shutdown.cancelled() => break,
            r = read_next_job(&mut redis_conn, node_id) => r,
        };

        match next {
            Ok(Some((stream_id, job_id))) => {
                handle_worker(stream_id, job_id, node_id, state).await;
            }
            Ok(None) => {
                continue;
            }
            Err(err) => {
                tracing::warn!("redis xreadgroup error: {:?}", err);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

pub async fn handle_worker(stream_id: String, job_id: i32, node_id: &str, state: &AppState) {
    let mut redis_conn = state.redis.clone();
    let mut now = Utc::now();
    let attempt_lease = now + Duration::seconds(10);

    let attempt_uid = Uuid::new_v4().to_string();

    let claimed = sqlx::query_as::<_, JobToExecute>(
            r#"
            UPDATE pendingjobs
            SET status = 'executing', updated_at = $2, claimed_by = $3, attempt_id = $4, lease_expires_at = $5, attempts = attempts + 1
            WHERE ID = $1 AND status = 'queued'
            RETURNING id, job_data, attempts, max_attempts
            "#,
        )
        .bind(job_id)
        .bind(now)
        .bind(&node_id)
        .bind(&attempt_uid)
        .bind(attempt_lease)
        .fetch_optional(&state.db)
        .await;

    let claimed = match claimed {
        Ok(c) => c,
        Err(e) => {
            error!("claim query failed for job {job_id}: {e}");
            return;
        }
    };

    let Some(JobToExecute {
        id,
        job_data,
        attempts,
        max_attempts,
    }) = claimed
    else {
        info!("Job {job_id} was not claimable, acking stale message");
        if let Err(e) = redis_ack(&mut redis_conn, &stream_id).await {
            error!("stale-message ack failed for {stream_id}: {e}");
        }
        return;
    };

    let cancel = CancellationToken::new();

    let renew_handle = tokio::spawn({
        let cancel = cancel.clone();
        let db = state.db.clone();
        let node_id = node_id.to_owned();
        let attempt_id = attempt_uid.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
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
                        .bind(job_id)
                        .bind(&node_id)
                        .bind(&attempt_id)
                        .bind(new_lease)
                        .execute(&db)
                        .await;

                        match result {
                            Ok(done) if done.rows_affected() == 1 => {
                                info!("Lease renewed for job {} by node {}", job_id, node_id)
                            }
                            Ok(_) => {
                                warn!("lost lease on job {job_id}; cancelling execution");
                                cancel.cancel();
                                break;
                            }
                            Err(e) => {
                                error!("Failed to renew lease for job {}: {}", job_id, e);
                            }
                        }
                    }
                }
            }
        }
    });

    let job_max_duration = std::time::Duration::from_secs(30);

    let exec_handle = tokio::spawn({
        let job_data = job_data.clone();
        async move { execute_job(&job_data).await }
    });
    let exec_abort = exec_handle.abort_handle();

    let outcome = tokio::select! {
        res = tokio::time::timeout(job_max_duration, exec_handle) => {
            match res {
                Err(_elapsed) => {
                    exec_abort.abort();
                    JobOutcome::Failed(JobError::Retryable("timed out".into()))
                },
                Ok(Err(join_err)) => JobOutcome::Failed(JobError::Retryable(format!("panicked: {join_err}"))),
                Ok(Ok(Ok(()))) => JobOutcome::Success,
                Ok(Ok(Err(e))) => JobOutcome::Failed(e),
            }
        }
        _ = cancel.cancelled() => {
            exec_abort.abort();
            JobOutcome::LeaseLost
        },
    };

    cancel.cancel();
    let _ = renew_handle.await;

    now = Utc::now();

    match outcome {
        JobOutcome::LeaseLost => {
            warn!("job {id} aborted: lease lost");
            return;
        }
        JobOutcome::Success => {
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
            .await;

            finish(result, &mut redis_conn, &stream_id, job_id, "complete").await;
        }
        JobOutcome::Failed(err) => {
            let (msg, permanent) = match err {
                JobError::Permanent(m) => (m, true),
                JobError::Retryable(m) => (m, false),
            };

            let exhausted = attempts >= max_attempts;

            let result = if permanent || exhausted {
                counter!("saturn_jobs_dead_lettered_total").increment(1);
                sqlx::query(
                    r#"UPDATE pendingjobs
                       SET status = 'failed', last_error = $5, updated_at = $2,
                           claimed_by = NULL, attempt_id = NULL, lease_expires_at = NULL
                       WHERE id = $1 AND status = 'executing' AND attempt_id = $3 AND claimed_by = $4"#,
                )
                .bind(job_id).bind(now).bind(&attempt_uid).bind(node_id).bind(&msg)
                .execute(&state.db).await
            } else {
                counter!("saturn_jobs_retried_total").increment(1);
                sqlx::query(
                    r#"
                    UPDATE pendingjobs
                    SET status = 'pending', last_error = $5, updated_at = $2,
                        scheduled_for = now() + make_interval(secs =>
                        LEAST(300.0, 5.0 * pow(2, attempts)) * (0.5 + random())),
                        claimed_by = NULL, attempt_id = NULL, lease_expires_at = NULL,
                        redis_indexed_at = NULL,
                    WHERE id = $1 AND status = 'executing' AND attempt_id = $3 AND claimed_by = $4
                    "#,
                )
                .bind(job_id)
                .bind(now)
                .bind(&attempt_uid)
                .bind(node_id)
                .bind(&msg)
                .execute(&state.db)
                .await
            };

            finish(
                result,
                &mut redis_conn,
                &stream_id,
                job_id,
                "failure-handled",
            )
            .await;
        }
    }
}

async fn finish(
    result: Result<sqlx::postgres::PgQueryResult, sqlx::Error>,
    redis_conn: &mut MultiplexedConnection,
    stream_id: &str,
    job_id: i32,
    what: &str,
) {
    match result {
        Ok(r) if r.rows_affected() == 1 => {
            if let Err(e) = redis_ack(redis_conn, stream_id).await {
                error!("ack failed for {stream_id}: {e}");
            }
        }
        Ok(_) => warn!("{what} write fenced for job {job_id}: ownership lost at finish"),
        Err(e) => error!("{what} write failed for job {job_id}: {e}"),
    }
}

async fn execute_job(job_data: &serde_json::Value) -> Result<(), JobError> {
    debug!("Executing job with job data: {job_data}");
    Ok(())
}
