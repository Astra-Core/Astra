use crate::authz::AstraEnforcer;
use crate::services::{AuthService, ConnectionService, PipelineService};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pipeline_service: Arc<PipelineService>,
    pub connection_service: Arc<ConnectionService>,
    /// Present when both `ASTRA_DATABASE_URL` and `ASTRA_JWT_SECRET` are set.
    /// Consumed by auth HTTP handlers (issue #149) and Axum middleware (issue #148).
    pub auth_service: Option<Arc<AuthService>>,
    /// Casbin RBAC enforcer. Always present — Postgres-backed when a database URL
    /// is configured, otherwise backed by an in-memory adapter.
    /// Consumed by authorization middleware (issue #148).
    pub enforcer: Arc<AstraEnforcer>,
}

impl AppState {
    pub fn new(
        pipeline_service: Arc<PipelineService>,
        connection_service: Arc<ConnectionService>,
        auth_service: Option<Arc<AuthService>>,
        enforcer: Arc<AstraEnforcer>,
    ) -> Self {
        Self {
            pipeline_service,
            connection_service,
            auth_service,
            enforcer,
        }
    }
}
