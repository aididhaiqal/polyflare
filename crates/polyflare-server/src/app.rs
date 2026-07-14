//! Application state and router construction.

use std::sync::Arc;

use axum::routing::post;
use axum::Router;

use polyflare_core::{Account, Executor};

use crate::ingress::responses_handler;

pub struct AppState {
    pub executor: Arc<dyn Executor>,
    pub account: Account,
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .with_state(state)
}
