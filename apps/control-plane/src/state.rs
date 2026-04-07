use crate::services::{ConnectionService, PipelineService};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pipeline_service: Arc<PipelineService>,
    pub connection_service: Arc<ConnectionService>,
}

impl AppState {
    pub fn new(
        pipeline_service: Arc<PipelineService>,
        connection_service: Arc<ConnectionService>,
    ) -> Self {
        Self {
            pipeline_service,
            connection_service,
        }
    }
}
