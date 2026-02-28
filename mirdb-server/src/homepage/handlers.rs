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

use super::metrics::MetricsCache;
use super::state::AppState;

/// Embedded HTML content for the homepage
const INDEX_HTML: &str = include_str!("../../assets/index.html");

/// Embedded CSS content
const STYLE_CSS: &str = include_str!("../../assets/css/style.css");

/// Embedded JavaScript content for theme toggle (Scenario 6)
const MAIN_JS: &str = include_str!("../../assets/js/main.js");

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

/// Get the main.js content (Scenario 6 - Theme Toggle)
fn get_main_js() -> &'static str {
    MAIN_JS
}

/// Handle requests to /api/status (Scenario 3 will implement)
pub async fn handle_status(state: Arc<AppState>) -> Result<impl Reply, warp::Rejection> {
    let status = serde_json::json!({
        "running": state.is_running(),
        "version": state.get_version()
    });
    Ok(warp::reply::json(&status))
}

/// Handle requests to /api/config
/// Returns configuration values including work_dir, port, and limits (REQ-3)
pub async fn handle_config(state: Arc<AppState>) -> Result<impl Reply, warp::Rejection> {
    // Extract port from addr (format: "host:port")
    let port = state.addr.split(':').last().unwrap_or("12333");

    let config = serde_json::json!({
        "work_dir": state.work_dir,
        "addr": state.addr,
        "port": port,
        "memtable_size_limit": state.memtable_size_limit,
        "memtable_size_limit_formatted": super::state::format_bytes(state.memtable_size_limit),
        "sst_max_size": state.sst_max_size,
        "sst_max_size_formatted": super::state::format_bytes(state.sst_max_size),
        "block_size": state.block_size,
        "block_size_formatted": super::state::format_bytes(state.block_size),
        "homepage_port": state.config.port,
        "homepage_enabled": state.config.enabled
    });
    Ok(warp::reply::json(&config))
}

/// Handle requests to /api/metrics
///
/// REQ-4: Must display basic metrics: number of active connections,
/// total keys stored, memtable size
///
/// Returns JSON response with current server metrics including:
/// - active_connections: Number of active memcached client connections
/// - total_keys: Total number of keys stored in the database
/// - memtable_size: Current memtable size in bytes
/// - sstable_levels: SSTable level information (as per appendix specification)
pub async fn handle_metrics(metrics_cache: Arc<MetricsCache>) -> Result<impl Reply, warp::Rejection> {
    let metrics = metrics_cache.get();
    Ok(warp::reply::json(&metrics))
}
