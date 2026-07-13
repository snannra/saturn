use crate::state::app_state::{AppState, init_state};
use crate::{app::api::shutdown_signal, db::redis::redis_stream_enqueue};
use chrono::Utc;
use redis::{self, AsyncTypedCommands};
use sqlx;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub async fn run_scheduler() {
    let (state, _job_rx, _marker_rx) = init_state().await;
    info!("scheduler starting");

    let shutdown = CancellationToken::new();

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            info!("shutdown signal received; finishing current pass");
            shutdown.cancel();
        }
    });

    poll(&state, shutdown).await;
    info!("scheduler shutdown complete");
}

pub async fn poll(state: &AppState, shutdown: CancellationToken) {
    let mut redis_conn = state.redis.clone();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }

        let db_now = Utc::now();
        let redis_now = db_now.timestamp();

        let due_ids: Vec<i32> = match redis_conn
            .zrangebyscore_limit("pending_jobs", 0, redis_now, 0, 100)
            .await
        {
            Ok(ids) => ids
                .into_iter()
                .filter_map(|id| id.parse::<i32>().ok())
                .collect(),
            Err(e) => {
                error!("zrangebyscore failed: {e}");
                continue;
            }
        };

        if due_ids.is_empty() {
            continue;
        }

        let transitioned: Vec<i32> = match sqlx::query_scalar::<_, i32>(
            r#"
            UPDATE pendingjobs
            SET status = 'queued', updated_at = $2
            WHERE id = ANY($1) AND status = 'pending'
            RETURNING id
            "#,
        )
        .bind(&due_ids)
        .bind(db_now)
        .fetch_all(&state.db)
        .await
        {
            Ok(ids) => ids,
            Err(e) => {
                error!("transition update failed: {e}");
                continue;
            }
        };

        let mut announced = Vec::with_capacity(transitioned.len());
        for job_id in &transitioned {
            match redis_stream_enqueue(&mut redis_conn, *job_id).await {
                Ok(_) => announced.push(*job_id),
                Err(e) => error!("failed to enqueue job {job_id}: {e}"),
            }
        }

        let stale: Vec<i32> = due_ids
            .iter()
            .copied()
            .filter(|id| !transitioned.contains(id))
            .collect();

        let mut to_remove = announced;
        to_remove.extend(stale);

        if !to_remove.is_empty() {
            if let Err(e) = redis_conn
                .zrem::<_, &Vec<i32>>("pending_jobs", &to_remove)
                .await
            {
                error!("zrem failed: {e}");
            }
        }

        if !to_remove.is_empty() {
            info!(
                transitioned = transitioned.len(),
                cleaned = to_remove.len(),
                "scheduling pass complete"
            )
        }
    }
}
