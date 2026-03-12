use crate::{http, state::AppState};
use axum::Router;

pub fn build_router(state: AppState) -> Router {
    Router::new().merge(http::routes()).with_state(state)
}
