mod api;
mod app;
mod auth;
mod authz;
mod config;
mod error;
mod executor;
mod http;
mod ldap;
mod metadata;
mod models;
pub mod repositories;
mod scheduler;
mod services;
mod state;

use api::ApiModule;
use authz::AstraEnforcer;
use config::ConfigModule;
use ldap::LdapClient;
use metadata::MetadataModule;
use repositories::{
    memory::{InMemoryConnectionRepository, InMemoryPipelineRepository, InMemoryUserRepository},
    postgres::PostgresPipelineRepository,
    ConnectionRepository, PipelineRepository, UserRepository,
};
use scheduler::SchedulerModule;
use services::{AuthService, ConnectionService, PipelineService};
use state::AppState;
use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_env("ASTRA_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let config = ConfigModule::from_env();
    let api = ApiModule::new();
    let scheduler = SchedulerModule::new();
    let metadata = MetadataModule::new();

    let (pipeline_repo, connection_repo, user_repo) = build_repositories(&config).await;

    let enforcer = build_enforcer(&config).await;

    let ldap_client = config.ldap.as_ref().map(|ldap_cfg| {
        let client = Arc::new(LdapClient::new(ldap_cfg.clone()));
        tracing::info!(url = %ldap_cfg.url, "LDAP authentication enabled");
        client
    });

    let auth_service = if config.auth_enabled {
        let secret = config
            .jwt_secret
            .as_deref()
            .unwrap_or_default()
            .as_bytes()
            .to_vec();
        let svc = Arc::new(AuthService::new(
            user_repo,
            secret,
            ldap_client,
            enforcer.clone(),
        ));
        tracing::info!("auth enabled (JWT + local users)");
        Some(svc)
    } else {
        tracing::info!("auth disabled (ASTRA_JWT_SECRET not set)");
        None
    };

    let connection_service = Arc::new(ConnectionService::new(connection_repo));
    let pipeline_service = Arc::new(PipelineService::new(
        pipeline_repo,
        connection_service.clone(),
    ));
    let state = AppState::new(pipeline_service, connection_service, auth_service, enforcer);
    let app = app::build_router(state);

    tracing::info!(
        config = config.status(),
        api = api.status(),
        scheduler = scheduler.status(),
        metadata = metadata.status(),
        database_backend = config.database_backend_label(),
        auth_enabled = config.auth_enabled,
        "astra control-plane modules initialized"
    );

    let addr: SocketAddr = config.bind_addr.parse()?;
    tracing::info!(%addr, "astra control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn build_enforcer(config: &ConfigModule) -> Arc<AstraEnforcer> {
    if let Some(db_url) = config.database_url.as_deref() {
        match AstraEnforcer::new(db_url).await {
            Ok(enforcer) => {
                tracing::info!("casbin enforcer initialized (postgres-backed)");
                return Arc::new(enforcer);
            }
            Err(e) => {
                tracing::warn!(
                    ?e,
                    "failed to initialize postgres casbin enforcer, falling back to in-memory"
                );
            }
        }
    }

    let enforcer = AstraEnforcer::new_memory()
        .await
        .expect("failed to create in-memory casbin enforcer");
    tracing::info!("casbin enforcer initialized (memory-backed)");
    Arc::new(enforcer)
}

async fn build_repositories(
    config: &ConfigModule,
) -> (
    Arc<dyn PipelineRepository>,
    Arc<dyn ConnectionRepository>,
    Arc<dyn UserRepository>,
) {
    if let Some(database_url) = config.database_url.as_deref() {
        match PostgresPipelineRepository::connect(database_url).await {
            Ok(repository) => {
                tracing::info!("using Postgres pipeline repository");
                let repo = Arc::new(repository);

                // Seed the initial admin user if both env vars are set and users
                // table is empty.
                if let (Some(email), Some(password)) = (&config.admin_email, &config.admin_password)
                {
                    match auth::hash_password(password) {
                        Ok(hash) => {
                            if let Err(e) = repo.maybe_seed_admin(email, &hash).await {
                                tracing::warn!(?e, "admin seed failed");
                            }
                        }
                        Err(e) => tracing::warn!(?e, "failed to hash admin password for seed"),
                    }
                }

                return (
                    repo.clone() as Arc<dyn PipelineRepository>,
                    repo.clone() as Arc<dyn ConnectionRepository>,
                    repo as Arc<dyn UserRepository>,
                );
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

    (
        Arc::new(InMemoryPipelineRepository::default()),
        Arc::new(InMemoryConnectionRepository::default()),
        Arc::new(InMemoryUserRepository::default()),
    )
}
