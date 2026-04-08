use crate::services::{AuthService, ConnectionService, PipelineService};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pipeline_service: Arc<PipelineService>,
    pub connection_service: Arc<ConnectionService>,
    /// Present when both `ASTRA_DATABASE_URL` and `ASTRA_JWT_SECRET` are set.
    /// Consumed by auth HTTP handlers (issue #149) and Axum middleware (issue #148).
    #[allow(dead_code)]
    pub auth_service: Option<Arc<AuthService>>,
}

impl AppState {
    pub fn new(
        pipeline_service: Arc<PipelineService>,
        connection_service: Arc<ConnectionService>,
        auth_service: Option<Arc<AuthService>>,
    ) -> Self {
        Self {
            pipeline_service,
            connection_service,
            auth_service,
        }
    }
}
