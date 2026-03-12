use crate::{http, state::AppState};
use axum::Router;
use std::path::PathBuf;
use tower_http::services::{ServeDir, ServeFile};

pub fn build_router(state: AppState) -> Router {
    let web_dist_dir = web_dist_dir();
    let index_file = web_dist_dir.join("index.html");

    Router::new()
        .merge(http::routes())
        .fallback_service(ServeDir::new(web_dist_dir).not_found_service(ServeFile::new(index_file)))
        .with_state(state)
}

fn web_dist_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../web/dist")
}
