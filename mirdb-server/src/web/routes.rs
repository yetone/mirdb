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

use axum::{routing::get, Router};

use crate::web::handlers::static_files::{serve_homepage, serve_static};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_router() {
        let _router = create_router();
        // Router created successfully
    }
}
