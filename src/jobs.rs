use crate::{AppState, users::User};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

#[derive(Debug, sqlx::FromRow)]
pub struct JobToExecute {
    pub id: i32,
    pub job_data: serde_json::Value,
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
        Ok(id) => id,
        Err(e) => {
            eprintln!("postgres insert failed: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let mut redis_conn = state
        .redis
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| {
            eprintln!("redis connection failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let redis_score = scheduled.timestamp();

    let _: () = redis_conn
        .zadd("pending_jobs", job_id, redis_score)
        .await
        .map_err(|e| {
            eprintln!("redis write failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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
