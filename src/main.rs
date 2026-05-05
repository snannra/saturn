use axum::routing::post;
use tokio;

mod jobs;
mod users;

#[tokio::main]
async fn main() {
    let app = axum::Router::new().route("/createjob", post(jobs::create_job));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Failed to Start Server!");

    axum::serve(listener, app).await.unwrap()
}
