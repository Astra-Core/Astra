use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::net::SocketAddr;

const INDEX_HTML: &str = include_str!("../../web/index.html");
const APP_JS: &str = include_str!("../../web/app.js");
const STYLES_CSS: &str = include_str!("../../web/styles.css");
const EXAMPLE_YAML: &str = include_str!("../../../examples/postgres-to-warehouse.astra.yaml");

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[derive(Serialize)]
struct PipelineSummary {
    name: &'static str,
    source_kind: &'static str,
    destination_kind: &'static str,
    status: &'static str,
}

#[derive(Serialize)]
struct PipelinesResponse {
    pipelines: Vec<PipelineSummary>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/version", get(version))
        .route("/api/v1/pipelines", get(pipelines))
        .route(
            "/api/v1/examples/postgres-to-warehouse",
            get(example_postgres_to_warehouse),
        );

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!(%addr, "astra control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn styles_css() -> impl IntoResponse {
    ([("content-type", "text/css; charset=utf-8")], STYLES_CSS)
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
        "note": "control-plane + UI shell"
    }))
}

async fn pipelines() -> Json<PipelinesResponse> {
    Json(PipelinesResponse {
        pipelines: vec![
            PipelineSummary {
                name: "postgres-analytics",
                source_kind: "postgres",
                destination_kind: "snowflake",
                status: "draft",
            },
            PipelineSummary {
                name: "billing-replication",
                source_kind: "mysql",
                destination_kind: "bigquery",
                status: "planned",
            },
        ],
    })
}

async fn example_postgres_to_warehouse() -> impl IntoResponse {
    (
        [("content-type", "text/plain; charset=utf-8")],
        EXAMPLE_YAML,
    )
}
