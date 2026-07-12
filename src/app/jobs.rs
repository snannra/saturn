use crate::{app::users::User, state::app_state::AppState};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use metrics::{counter, histogram};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::error;

#[derive(Debug, sqlx::FromRow)]
pub struct JobToExecute {
    pub id: i32,
    pub job_data: serde_json::Value,
    pub attempts: i32,
    pub max_attempts: i32,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ForgottenJob {
    pub id: i32,
    pub scheduled_for: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct JobCreateResponse {
    job_id: i32,
    message: String,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct JobStatusResponse {
    pub id: i32,
    pub status: String,
    pub job_data: serde_json::Value,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct JobRequest {
    user: User,
    job: serde_json::Value,
    scheduled_for: Option<String>,
}

pub struct JobBatchItem {
    username: String,
    job: serde_json::Value,
    scheduled_for: DateTime<Utc>,
    respond_to: oneshot::Sender<Result<i32, String>>,
}

pub async fn create_job(
    State(state): State<AppState>,
    Json(job_request): Json<JobRequest>,
) -> Result<(StatusCode, Json<JobCreateResponse>), StatusCode> {
    let username = job_request.user.username;
    let job = job_request.job;

    // validate first: bad input 400s before anything is durable
    let scheduled = job_request
        .scheduled_for
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    let scheduled_for = DateTime::parse_from_rfc3339(&scheduled)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .with_timezone(&Utc);

    let insert_start = Instant::now();
    let (resp_tx, resp_rx) = oneshot::channel();
    state
        .job_tx
        .send(JobBatchItem {
            username,
            job,
            scheduled_for,
            respond_to: resp_tx,
        })
        .await
        .map_err(|_| {
            error!("job batch channel closed: flusher is gone");
            counter!("saturn_jobs_create_failed_total").increment(1);
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    // backpressure gauge: ~0 when healthy, climbs when the insert channel is full
    histogram!("saturn_batch_send_ms").record(insert_start.elapsed().as_secs_f64() * 1000.0);

    let job_id: i32 = resp_rx
        .await
        // outer: flusher dropped our sender without responding
        .map_err(|_| {
            error!("flusher dropped response channel without answering");
            counter!("saturn_jobs_create_failed_total").increment(1);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        // inner: flusher responded, but the batch insert failed
        .map_err(|e| {
            error!("batch insert failed: {e:?}");
            counter!("saturn_jobs_create_failed_total").increment(1);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // full "submitted -> durable" duration the client pays for the insert
    histogram!("saturn_insert_wait_ms").record(insert_start.elapsed().as_secs_f64() * 1000.0);

    let redis_start = Instant::now();

    let mut redis_conn = state.redis.clone();
    let redis_score = scheduled_for.timestamp();
    let _: () = redis_conn
        .zadd("pending_jobs", job_id, redis_score)
        .await
        .map_err(|e| {
            error!("redis write failed: {e}");
            counter!("saturn_redis_write_failed_total").increment(1);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    histogram!("saturn_redis_insert_ms").record(redis_start.elapsed().as_secs_f64() * 1000.0);

    // best-effort: sweeper reconciles anything dropped here
    if state.marker_tx.try_send(job_id).is_err() {
        counter!("saturn_marker_dropped_total").increment(1);
    }

    histogram!("saturn_job_create_total_ms").record(insert_start.elapsed().as_secs_f64() * 1000.0);

    Ok((
        StatusCode::OK,
        Json(JobCreateResponse {
            job_id,
            message: "Job Created Successfully".to_string(),
        }),
    ))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Json<JobStatusResponse>, StatusCode> {
    let job = sqlx::query_as::<_, JobStatusResponse>(
        r#"
            SELECT 
                id, 
                status, 
                job_data,
                updated_at,
                created_at
            FROM pendingjobs 
            WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(job))
}

pub async fn batch_flusher(
    mut job_rx: tokio::sync::mpsc::Receiver<JobBatchItem>,
    db: sqlx::PgPool,
) {
    loop {
        let Some(first) = job_rx.recv().await else {
            return;
        };

        let mut batch = vec![first];

        let deadline = Instant::now() + Duration::from_millis(5);

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => break,
                item = job_rx.recv() => match item {
                    Some(it) => {
                        batch.push(it);
                        if batch.len() >= 200 {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        flush(&db, batch).await;
    }
}

async fn flush(db: &PgPool, batch: Vec<JobBatchItem>) {
    // packing factor: throughput = batch_size x flushes/sec
    histogram!("saturn_batch_size").record(batch.len() as f64);

    let mut user_ids = Vec::with_capacity(batch.len());
    let mut scheduled = Vec::with_capacity(batch.len());
    let mut job_data = Vec::with_capacity(batch.len());

    let now = Utc::now();

    for item in &batch {
        user_ids.push(item.username.clone());
        scheduled.push(item.scheduled_for);
        job_data.push(item.job.clone());
    }

    let flush_start = Instant::now();

    let result: Result<Vec<i32>, sqlx::Error> = sqlx::query_scalar(
        r#"
        INSERT INTO pendingjobs (user_id, scheduled_for, job_data, status, updated_at)
        SELECT u, s, j, 'pending', $4
        FROM UNNEST($1::text[], $2::timestamptz[], $3::jsonb[]) AS t(u,s,j)
        RETURNING id 
        "#,
    )
    .bind(&user_ids)
    .bind(&scheduled)
    .bind(&job_data)
    .bind(now)
    .fetch_all(db)
    .await;

    // one round trip + one commit per batch; expect ~flat regardless of batch size
    histogram!("saturn_batch_flush_ms").record(flush_start.elapsed().as_secs_f64() * 1000.0);

    match result {
        Ok(ids) => {
            // rows durably inserted: incremented by batch size, here, so the
            // counter means "committed rows" rather than "handled requests"
            counter!("saturn_jobs_created_total").increment(ids.len() as u64);
            for (item, id) in batch.into_iter().zip(ids) {
                let _ = item.respond_to.send(Ok(id));
            }
        }
        Err(e) => {
            error!("batch insert failed ({} jobs): {e}", batch.len());
            counter!("saturn_batch_flush_errors_total").increment(1);
            for item in batch {
                let _ = item.respond_to.send(Err("insert failed".to_string()));
            }
        }
    }
}

pub async fn marker_flusher(mut rx: tokio::sync::mpsc::Receiver<i32>, db: PgPool) {
    let mut ids = Vec::with_capacity(1000);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            item = rx.recv() => match item {
                Some(id) => {
                    ids.push(id);
                    if ids.len() < 1000 { continue;}
                }
                None => {
                    if !ids.is_empty() {flush_markers(&db, &ids).await;}
                    return;
                }
            }
        }
        if !ids.is_empty() {
            flush_markers(&db, std::mem::take(&mut ids).as_slice()).await;
            ids = Vec::with_capacity(1000);
        }
    }
}

async fn flush_markers(db: &PgPool, ids: &[i32]) {
    histogram!("saturn_marker_batch_size").record(ids.len() as f64);

    let flush_start = Instant::now();

    let result = sqlx::query(
        r#"
        UPDATE pendingjobs
        SET redis_indexed_at = NOW()
        WHERE id = ANY($1);
        "#,
    )
    .bind(ids)
    .execute(db)
    .await;

    histogram!("saturn_marker_flush_ms").record(flush_start.elapsed().as_secs_f64() * 1000.0);

    if let Err(e) = result {
        error!("marker batch update failed ({} ids): {e}", ids.len());
        counter!("saturn_marker_flush_errors_total").increment(1);
        // ids are dropped; the sweeper reconciles unmarked jobs
    }
}
