use crate::services::PipelineService;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pipeline_service: Arc<PipelineService>,
}

impl AppState {
    pub fn new(pipeline_service: Arc<PipelineService>) -> Self {
        Self { pipeline_service }
    }
}
