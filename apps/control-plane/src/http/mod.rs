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
        .route("/api/v1/specs/apply", post(pipelines::apply_spec))
        .route(
            "/api/v1/examples/postgres-to-warehouse",
            get(health::example_postgres_to_warehouse),
        )
}
