//! Request Handlers
//!
//! Handler functions for each route endpoint.
//!
//! Owner: Scenario 3 - Server Status and Version Display
//! Co-owner: Scenario 4 - Configuration Display
//! Co-owner: Scenario 5 - Metrics Display and API
//!
//! Expected exports:
//! - pub async fn handle_index() -> impl Reply
//! - pub async fn handle_status(state: Arc<AppState>) -> impl Reply
//! - pub async fn handle_config(state: Arc<AppState>) -> impl Reply
//! - pub async fn handle_metrics(metrics: Arc<MetricsCache>) -> impl Reply

use std::sync::Arc;
use warp::http::Response;
use warp::path::Tail;
use warp::Reply;

use super::state::AppState;

/// Embedded HTML content for the homepage
const INDEX_HTML: &str = include_str!("../../assets/index.html");

/// Embedded CSS content
const STYLE_CSS: &str = include_str!("../../assets/css/style.css");

/// Handle requests to the homepage root /
pub async fn handle_index() -> Result<impl Reply, warp::Rejection> {
    Ok(Response::builder()
        .header("Content-Type", "text/html; charset=utf-8")
        .body(INDEX_HTML.to_string())
        .unwrap())
}

/// Handle requests for static assets at /static/*
pub async fn handle_static(path: Tail) -> Result<impl Reply, warp::Rejection> {
    let path_str = path.as_str();

    match path_str {
        "css/style.css" => Ok(Response::builder()
            .header("Content-Type", "text/css; charset=utf-8")
            .body(STYLE_CSS.to_string())
            .unwrap()),
        "js/main.js" => Ok(Response::builder()
            .header("Content-Type", "application/javascript; charset=utf-8")
            .body(get_main_js().to_string())
            .unwrap()),
        _ => Err(warp::reject::not_found()),
    }
}

/// Get the main.js content (placeholder for Scenario 6)
fn get_main_js() -> &'static str {
    r#"// MirDB Homepage JavaScript - Scenario 6 will implement theme toggle
document.addEventListener('DOMContentLoaded', function() {
    console.log('MirDB Homepage loaded');
});
"#
}

/// Handle requests to /api/status (Scenario 3 will implement)
pub async fn handle_status(state: Arc<AppState>) -> Result<impl Reply, warp::Rejection> {
    let status = serde_json::json!({
        "running": state.is_running(),
        "version": state.get_version()
    });
    Ok(warp::reply::json(&status))
}

/// Handle requests to /api/config (Scenario 4 will implement)
pub async fn handle_config(state: Arc<AppState>) -> Result<impl Reply, warp::Rejection> {
    let config = serde_json::json!({
        "work_dir": state.work_dir,
        "addr": state.addr
    });
    Ok(warp::reply::json(&config))
}

/// Handle requests to /api/metrics (Scenario 5 will implement)
pub async fn handle_metrics(_state: Arc<AppState>) -> Result<impl Reply, warp::Rejection> {
    let metrics = serde_json::json!({
        "active_connections": 0,
        "total_keys": 0,
        "memtable_size": 0
    });
    Ok(warp::reply::json(&metrics))
}
