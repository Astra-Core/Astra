pub mod health;
pub mod pipelines;

use crate::state::AppState;
use axum::{
    routing::{get, post},
    Router,
};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(health::index))
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route("/version", get(health::version))
        .route("/api/v1/pipelines", get(pipelines::list_pipelines))
        .route(
            "/api/v1/pipelines/:pipeline_name",
            get(pipelines::get_pipeline_yaml),
        )
        .route(
            "/api/v1/pipelines/:pipeline_name/runs",
            get(pipelines::list_pipeline_runs),
        )
        .route(
            "/api/v1/pipelines/:pipeline_name/latest-run",
            get(pipelines::get_latest_run),
        )
        .route(
            "/api/v1/pipelines/:pipeline_name/run-history",
            get(pipelines::get_run_history),
        )
        .route(
            "/api/v1/pipeline-runs",
            post(pipelines::create_pipeline_run),
        )
        .route(
            "/api/v1/pipeline-runs/:run_id/status",
            post(pipelines::update_pipeline_run_status),
        )
        .route(
            "/api/v1/pipeline-runs/:pipeline_run_id/artifacts",
            post(pipelines::record_staged_artifact).get(pipelines::list_staged_artifacts),
        )
        .route("/api/v1/specs/apply", post(pipelines::apply_spec))
        .route(
            "/api/v1/examples/postgres-to-warehouse",
            get(health::example_postgres_to_warehouse),
        )
}
