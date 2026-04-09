pub mod auth;
pub mod connections;
pub mod health;
pub mod ldap_mappings;
pub mod pipelines;
pub mod users;

use crate::{middleware::auth::auth_middleware, state::AppState};
use axum::{
    middleware::from_fn_with_state,
    routing::{delete, get, post},
    Router,
};

pub fn routes(state: AppState) -> Router<AppState> {
    let public = public_routes();
    let protected = protected_routes().layer(from_fn_with_state(state, auth_middleware));

    Router::new().merge(public).merge(protected)
}

/// Routes that do not require authentication.
fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(health::index))
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route("/version", get(health::version))
        .route(
            "/api/v1/examples/postgres-to-warehouse",
            get(health::example_postgres_to_warehouse),
        )
        // Auth — login, refresh, and logout are unauthenticated by design.
        .route("/api/v1/auth/login", post(auth::login))
        .route("/api/v1/auth/refresh", post(auth::refresh))
        .route("/api/v1/auth/logout", post(auth::logout))
}

/// Routes that require a valid JWT and casbin-enforced permissions.
fn protected_routes() -> Router<AppState> {
    Router::new()
        // Auth — me requires a valid token but no specific casbin permission.
        .route("/api/v1/auth/me", get(auth::me))
        // Pipelines
        .route("/api/v1/pipelines", get(pipelines::list_pipelines))
        .route(
            "/api/v1/pipelines/:pipeline_name",
            get(pipelines::get_pipeline_yaml).delete(pipelines::delete_pipeline),
        )
        .route(
            "/api/v1/pipelines/:pipeline_name/disable",
            post(pipelines::disable_pipeline),
        )
        .route(
            "/api/v1/pipelines/:pipeline_name/enable",
            post(pipelines::enable_pipeline),
        )
        .route(
            "/api/v1/pipelines/:pipeline_name/trigger",
            post(pipelines::trigger_pipeline),
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
        .route(
            "/api/v1/pipeline-runs/:pipeline_run_id/table-executions",
            post(pipelines::upsert_table_execution).get(pipelines::list_table_executions),
        )
        .route("/api/v1/specs/apply", post(pipelines::apply_spec))
        // Connections
        .route(
            "/api/v1/connections",
            get(connections::list_connections).post(connections::create_connection),
        )
        .route(
            "/api/v1/connections/test",
            post(connections::test_connection),
        )
        .route(
            "/api/v1/connections/:name",
            get(connections::get_connection)
                .put(connections::update_connection)
                .delete(connections::delete_connection),
        )
        // Users (admin)
        .route(
            "/api/v1/users",
            get(users::list_users).post(users::create_user),
        )
        .route("/api/v1/users/:id", delete(users::delete_user))
        .route(
            "/api/v1/users/:id/roles",
            get(users::get_roles).put(users::set_roles),
        )
        // LDAP group mappings (admin)
        .route(
            "/api/v1/ldap/group-mappings",
            get(ldap_mappings::list_mappings).post(ldap_mappings::add_mapping),
        )
        .route(
            "/api/v1/ldap/group-mappings/:id",
            delete(ldap_mappings::delete_mapping),
        )
}
