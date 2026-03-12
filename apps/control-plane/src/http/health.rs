use axum::{
    response::{Html, IntoResponse},
    Json,
};
use serde::Serialize;

const INDEX_HTML: &str = include_str!("../../../web/index.html");
const APP_JS: &str = include_str!("../../../web/app.js");
const STYLES_CSS: &str = include_str!("../../../web/styles.css");
const EXAMPLE_YAML: &str = include_str!("../../../../examples/postgres-to-warehouse.astra.yaml");

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

pub async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

pub async fn app_js() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        APP_JS,
    )
}

pub async fn styles_css() -> impl IntoResponse {
    ([("content-type", "text/css; charset=utf-8")], STYLES_CSS)
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "astra-control-plane",
    })
}

pub async fn ready() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ready",
        service: "astra-control-plane",
    })
}

pub async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "astra-control-plane",
        "version": env!("CARGO_PKG_VERSION"),
        "note": "modular control-plane"
    }))
}

pub async fn example_postgres_to_warehouse() -> impl IntoResponse {
    (
        [("content-type", "text/plain; charset=utf-8")],
        EXAMPLE_YAML,
    )
}
