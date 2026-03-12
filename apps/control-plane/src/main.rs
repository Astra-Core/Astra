mod app;
mod config;
mod error;
mod http;
mod models;
mod repositories;
mod services;
mod state;

use config::Config;
use repositories::memory::InMemoryPipelineRepository;
use services::PipelineService;
use state::AppState;
use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let config = Config::from_env();
    let repository = Arc::new(InMemoryPipelineRepository::default());
    let pipeline_service = Arc::new(PipelineService::new(repository));
    let state = AppState::new(pipeline_service);
    let app = app::build_router(state);

    let addr: SocketAddr = config.bind_addr.parse()?;
    tracing::info!(%addr, "astra control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
