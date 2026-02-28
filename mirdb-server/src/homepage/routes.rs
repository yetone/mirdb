//! Route Definitions
//!
//! Defines all HTTP routes for the homepage:
//! - GET / - Homepage HTML
//! - GET /api/metrics - JSON metrics
//! - GET /api/status - Server status
//! - GET /api/config - Configuration
//! - GET /static/* - Static assets
//!
//! Owner: Scenario 2 - Homepage Static Content Rendering
//! Co-owner: Scenario 9 - Static Asset Handling
//!
//! Expected exports:
//! - pub fn routes(state: Arc<AppState>) -> impl Filter
//! - pub fn static_routes() -> impl Filter
//! - pub fn api_routes(state: Arc<AppState>) -> impl Filter

use std::sync::Arc;
use warp::Filter;

use super::handlers;
use super::metrics::MetricsCache;
use super::state::AppState;

/// Creates all routes for the homepage
pub fn routes(
    state: Arc<AppState>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let metrics_cache = Arc::new(MetricsCache::default());
    routes_with_metrics(state, metrics_cache)
}

/// Creates all routes with custom metrics cache
pub fn routes_with_metrics(
    state: Arc<AppState>,
    metrics_cache: Arc<MetricsCache>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    index_route()
        .or(static_routes())
        .or(api_routes(state, metrics_cache))
}

/// Route for serving the homepage HTML at /
pub fn index_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::end()
        .and(warp::get())
        .and_then(handlers::handle_index)
}

/// Routes for serving static assets at /static/*
///
/// Scenario 9 - Static Asset Handling:
/// Serves CSS, JavaScript, and image assets with correct MIME types.
/// Supports gzip compression when Accept-Encoding: gzip header is present.
pub fn static_routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("static")
        .and(warp::get())
        .and(warp::path::tail())
        .and(warp::header::optional::<String>("accept-encoding"))
        .and_then(handlers::handle_static_with_compression)
}

/// Routes for serving static assets without compression (for backwards compatibility)
pub fn static_routes_no_compression() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("static")
        .and(warp::get())
        .and(warp::path::tail())
        .and_then(handlers::handle_static)
}

/// API routes for JSON endpoints
pub fn api_routes(
    state: Arc<AppState>,
    metrics_cache: Arc<MetricsCache>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let state_filter = warp::any().map(move || state.clone());
    let metrics_filter = warp::any().map(move || metrics_cache.clone());

    let status_route = warp::path!("api" / "status")
        .and(warp::get())
        .and(state_filter.clone())
        .and_then(handlers::handle_status);

    let config_route = warp::path!("api" / "config")
        .and(warp::get())
        .and(state_filter)
        .and_then(handlers::handle_config);

    let metrics_route = warp::path!("api" / "metrics")
        .and(warp::get())
        .and(metrics_filter)
        .and_then(handlers::handle_metrics);

    status_route.or(config_route).or(metrics_route)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routes_compile() {
        // Basic compilation test - routes structure is valid
        // Full integration tests are in tests/homepage/routes_tests.rs
    }
}
