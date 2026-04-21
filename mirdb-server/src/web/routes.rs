//! Route definitions for the web server.
//! Owner: Scenario 1 - HTTP Server Startup
//!
//! Routes:
//! - GET  /                -> Homepage HTML
//! - GET  /static/*        -> Static assets (CSS, JS)
//! - GET  /api/metrics     -> Server metrics JSON (Scenario 3)
//! - GET  /api/config      -> Configuration JSON (Scenario 4)
//! - GET  /api/health      -> Health check JSON (Scenario 8)
//! - POST /api/kv/set      -> Set key-value (Scenario 5)
//! - GET  /api/kv/get      -> Get value by key (Scenario 6)
//! - DELETE /api/kv/delete -> Delete key (Scenario 7)

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, MethodRouter},
    Json, Router,
};
use std::sync::Arc;

use crate::config::Config;
use crate::store::Store;
use crate::web::handlers::config::{ConfigState, SharedConfigState};
use crate::web::handlers::health::get_health;
use crate::web::handlers::kv::delete_kv;
use crate::web::handlers::static_files::{serve_homepage, serve_static};
use crate::web::types::{ApiError, ApiResponse, ConfigResponse, HealthResponse};

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub config: Config,
    pub config_state: SharedConfigState,
}

/// Create the web server router with all routes
pub fn create_router() -> Router {
    Router::new()
        // Static content routes (Scenario 2)
        .route("/", get(serve_homepage))
        .route("/static/*path", get(serve_static))
    // API routes will be added by other scenarios:
    // .route("/api/metrics", get(handlers::metrics::get_metrics))
    // .route("/api/config", get(handlers::config::get_config))
    // .route("/api/health", get(handlers::health::get_health))
    // .route("/api/kv/set", post(handlers::kv::set_kv))
    // .route("/api/kv/get", get(handlers::kv::get_kv))
    // .route("/api/kv/delete", delete(handlers::kv::delete_kv))
}

/// Create the router with application state (for health and config endpoints)
pub fn create_router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/", get(serve_homepage))
        .route("/static/*path", get(serve_static))
        .route("/api/health", get(get_health))
        .route("/api/config", config_route())
        .route("/api/kv/delete", delete(delete_kv))
        .with_state(state)
}

/// Configuration endpoint route handler
/// GET /api/config - Returns current configuration
/// All other methods - Returns 405 Method Not Allowed
fn config_route() -> MethodRouter<AppState> {
    get(get_config_handler)
        .post(config_method_not_allowed)
        .put(config_method_not_allowed)
        .delete(config_method_not_allowed)
        .patch(config_method_not_allowed)
}

/// Handler for GET /api/config
async fn get_config_handler(
    State(state): State<AppState>,
) -> Json<ApiResponse<ConfigResponse>> {
    let config_response = state.config_state.get_config();
    Json(ApiResponse::success(config_response))
}

/// Handler for non-GET methods on /api/config - returns 405 Method Not Allowed
async fn config_method_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(ApiError::new(
            "Method Not Allowed. Configuration is read-only.",
            405,
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_router() {
        let _router = create_router();
        // Router created successfully
    }
}
