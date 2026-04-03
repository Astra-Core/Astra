mod api;
mod app;
mod config;
mod error;
mod executor;
mod http;
mod metadata;
mod models;
pub mod repositories;
mod scheduler;
mod services;
mod state;

use api::ApiModule;
use config::ConfigModule;
use metadata::MetadataModule;
use repositories::{
    memory::InMemoryPipelineRepository, postgres::PostgresPipelineRepository, PipelineRepository,
};
use scheduler::SchedulerModule;
use services::PipelineService;
use state::AppState;
use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let config = ConfigModule::from_env();
    let api = ApiModule::new();
    let scheduler = SchedulerModule::new();
    let metadata = MetadataModule::new();
    let repository = build_repository(&config).await;
    let pipeline_service = Arc::new(PipelineService::new(repository));
    let state = AppState::new(pipeline_service);
    let app = app::build_router(state);

    tracing::info!(
        config = config.status(),
        api = api.status(),
        scheduler = scheduler.status(),
        metadata = metadata.status(),
        database_backend = config.database_backend_label(),
        "astra control-plane modules initialized"
    );

    let addr: SocketAddr = config.bind_addr.parse()?;
    tracing::info!(%addr, "astra control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn build_repository(config: &ConfigModule) -> Arc<dyn PipelineRepository> {
    if let Some(database_url) = config.database_url.as_deref() {
        match PostgresPipelineRepository::connect(database_url).await {
            Ok(repository) => {
                tracing::info!("using Postgres pipeline repository");
                return Arc::new(repository);
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "failed to initialize Postgres repository, falling back to in-memory storage"
                );
            }
        }
    } else {
        tracing::info!("ASTRA_DATABASE_URL not set; using in-memory pipeline repository");
    }

    Arc::new(InMemoryPipelineRepository::default())
}
