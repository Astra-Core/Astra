use axum::{routing::get, Json, Router};
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/version", get(version));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!(%addr, "astra control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "astra-control-plane",
    })
}

async fn ready() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ready",
        service: "astra-control-plane",
    })
}

async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "astra-control-plane",
        "version": env!("CARGO_PKG_VERSION"),
        "note": "skeleton only"
    }))
}
