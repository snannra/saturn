use crate::state::app_state::{AppState, init_state};
use sqlx;
use tracing::error;
use uuid::Uuid;

pub async fn register_node(state: &AppState, role: String) -> Result<String, sqlx::Error> {
    let node_id = Uuid::new_v4().to_string();

    let node_id = sqlx::query_scalar::<_, String>(
        r#"
            INSERT INTO saturn_nodes(
                node_id,
                role,
                started_at,
                last_heartbeat_at
            )
            VALUES (
                $1,
                $2,
                now(),
                now()
            )
            ON CONFLICT (node_id)
            DO UPDATE SET
                role = EXCLUDED.role,
                last_heartbeat_at = now(),
                deleted_at = NULL
            RETURNING node_id
            "#,
    )
    .bind(node_id)
    .bind(role)
    .fetch_one(&state.db)
    .await?;

    Ok(node_id)
}

pub async fn heartbeat(node_id: &str, state: &AppState) -> Result<String, sqlx::Error> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

    loop {
        interval.tick().await;

        if let Err(e) = sqlx::query(
            r#"
                UPDATE saturn_nodes
                SET last_heartbeat_at = now()
                WHERE node_id = $1
                "#,
        )
        .bind(node_id)
        .execute(&state.db)
        .await
        {
            error!("Heartbeat failed for node {}: {}", node_id, e);
        }
    }
}
