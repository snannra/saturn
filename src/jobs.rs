use crate::{AppState, users::User};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use metrics::{counter, histogram};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{error, info};

#[derive(Debug, sqlx::FromRow)]
pub struct JobToExecute {
    pub id: i32,
    pub job_data: serde_json::Value,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ForgottenJob {
    pub id: i32,
    pub scheduled_for: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct JobRequest {
    user: User,
    job: serde_json::Value,
    scheduled_for: Option<String>,
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

pub async fn create_job(
    State(state): State<AppState>,
    Json(job_request): Json<JobRequest>,
) -> Result<(StatusCode, Json<JobCreateResponse>), StatusCode> {
    let start = Instant::now();
    let job_data = job_request.job;
    let job_time = job_request.scheduled_for.unwrap_or(Utc::now().to_rfc3339());
    let scheduled = DateTime::parse_from_rfc3339(&job_time)
        .unwrap()
        .with_timezone(&Utc);

    let now = Utc::now();

    let job_id: i32 = match sqlx::query_scalar(
        r#"
            INSERT INTO pendingjobs (
                user_id,
                scheduled_for,
                job_data,
                status,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        "#,
    )
    .bind(job_request.user.username)
    .bind(scheduled)
    .bind(&job_data)
    .bind("pending")
    .bind(now)
    .fetch_one(&state.db)
    .await
    {
        Ok(id) => {
            info!("Created job: {}", id);
            id
        }
        Err(e) => {
            error!("postgres insert failed: {e}");
            counter!("saturn_jobs_create_failed_total").increment(1);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let pg_elapsed = start.elapsed();
    histogram!("saturn_postgres_insert_ms").record(pg_elapsed.as_secs_f64() * 1000.0);
    counter!("saturn_jobs_created_total").increment(1);

    let redis_start = Instant::now();

    let mut redis_conn = state.redis.clone();

    let redis_score = scheduled.timestamp();

    let _: () = redis_conn
        .zadd("pending_jobs", job_id, redis_score)
        .await
        .map_err(|e| {
            error!("redis write failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let redis_elapsed = redis_start.elapsed();
    histogram!("saturn_redis_insert_ms").record(redis_elapsed.as_secs_f64() * 1000.0);
    let total_elapsed = start.elapsed();
    histogram!("saturn_job_create_total_ms").record(total_elapsed.as_secs_f64() * 1000.0);

    info!(
        job_id = job_id,
        postgres_ms = pg_elapsed.as_millis(),
        redis_ms = redis_elapsed.as_millis(),
        total_ms = total_elapsed.as_millis(),
        "Job Created"
    );

    match sqlx::query(
        r#"
        UPDATE pendingjobs
        SET redis_indexed_at = NOW()
        WHERE id = $1;
        "#,
    )
    .bind(job_id)
    .execute(&state.db)
    .await
    {
        Ok(_) => {
            info!("Inserted into redis sorted set");
            Ok((
                StatusCode::OK,
                Json(JobCreateResponse {
                    job_id,
                    message: "Job Created Successfully".to_string(),
                }),
            ))
        }
        Err(e) => {
            error!("Write to redis failed");
            Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(JobCreateResponse {
                    job_id,
                    message: "Failed to write to redis sorted set".to_string(),
                }),
            ))
        }
    }
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
