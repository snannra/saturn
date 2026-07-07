use crate::state::app_state::AppState;
use axum::extract::State;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub fn setup_metrics() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets(&[
            0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0,
        ])
        .expect("Invalid Histogram Buckets")
        .install_recorder()
        .expect("Failed to install recorder")
}

pub async fn metrics_handler(State(state): State<AppState>) -> String {
    state.prom_handle.render()
}
