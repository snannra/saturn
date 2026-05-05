use crate::users::User;
use axum::{extract::Json, http::StatusCode, response::IntoResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct JobRequest {
    user: User,
    job: serde_json::Value,
    scheduled_for: DateTime<Utc>,
}

struct JobState {
    current_job: u32,
}

#[derive(Serialize)]
pub struct JobResponse {
    job_id: u32,
    message: String,
}

pub async fn create_job(Json(job_request): Json<JobRequest>) -> (StatusCode, Json<JobResponse>) {
    (
        StatusCode::OK,
        Json(JobResponse {
            job_id: 0,
            message: "Job Created Successfully".to_string(),
        }),
    )
}
