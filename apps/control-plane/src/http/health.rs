use axum::{
    response::{Html, IntoResponse},
    Json,
};
use serde::Serialize;
use std::path::PathBuf;

const WEB_APP_MISSING_HTML: &str = r#"<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Astra UI build missing</title>
    <style>
      :root { color-scheme: dark; font-family: Inter, system-ui, sans-serif; }
      body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #09101e; color: #eef2ff; }
      main { max-width: 44rem; padding: 2rem; border-radius: 20px; background: #111935; border: 1px solid #26335d; }
      code, pre { font-family: ui-monospace, monospace; background: #0a1021; border-radius: 10px; padding: 0.2rem 0.4rem; }
      pre { padding: 1rem; overflow: auto; }
      h1 { margin-top: 0; }
      p { color: #c4cffd; }
    </style>
  </head>
  <body>
    <main>
      <h1>Astra UI build not found</h1>
      <p>The control plane can serve the web app once <code>apps/web/dist</code> exists.</p>
      <p>For local UI development, run the Vite dev server in <code>apps/web</code>:</p>
      <pre>npm install
npm run dev</pre>
      <p>For a self-hosted static build, run:</p>
      <pre>npm install
npm run build
cargo run -p astra-control-plane</pre>
      <p>The API is still available under <code>/api/v1/*</code>. The frontend build is the missing piece, not the backend.</p>
    </main>
  </body>
</html>
"#;
const EXAMPLE_YAML: &str = include_str!("../../../../examples/postgres-to-warehouse.astra.yaml");

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

pub async fn index() -> Html<String> {
    let index_file = web_dist_dir().join("index.html");
    match tokio::fs::read_to_string(index_file).await {
        Ok(contents) => Html(contents),
        Err(_) => Html(WEB_APP_MISSING_HTML.to_string()),
    }
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

fn web_dist_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../web/dist")
}
