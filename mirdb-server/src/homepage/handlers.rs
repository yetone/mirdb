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

/// Embedded SVG logo (Scenario 9 - Static Asset Handling)
const LOGO_SVG: &str = include_str!("../../assets/images/logo.svg");

/// Handle requests to the homepage root /
pub async fn handle_index() -> Result<impl Reply, warp::Rejection> {
    Ok(Response::builder()
        .header("Content-Type", "text/html; charset=utf-8")
        .body(INDEX_HTML.to_string())
        .unwrap())
}

/// Handle requests for static assets at /static/*
///
/// Scenario 9 - Static Asset Handling:
/// Serves CSS, JavaScript, and image assets with correct MIME types.
/// Supports gzip compression via the Accept-Encoding header.
pub async fn handle_static(path: Tail) -> Result<impl Reply, warp::Rejection> {
    let path_str = path.as_str();

    match path_str {
        "css/style.css" | "style.css" => Ok(Response::builder()
            .header("Content-Type", "text/css; charset=utf-8")
            .header("Cache-Control", "public, max-age=3600")
            .body(STYLE_CSS.to_string())
            .unwrap()),
        "js/main.js" | "main.js" => Ok(Response::builder()
            .header("Content-Type", "application/javascript; charset=utf-8")
            .header("Cache-Control", "public, max-age=3600")
            .body(get_main_js().to_string())
            .unwrap()),
        "images/logo.svg" | "logo.svg" => Ok(Response::builder()
            .header("Content-Type", "image/svg+xml")
            .header("Cache-Control", "public, max-age=86400")
            .body(LOGO_SVG.to_string())
            .unwrap()),
        _ => Err(warp::reject::not_found()),
    }
}

/// Handle requests for static assets with gzip compression support
///
/// Scenario 9 - Static Asset Handling:
/// This handler checks the Accept-Encoding header and compresses text-based
/// assets (CSS, JS, HTML) with gzip when supported by the client.
pub async fn handle_static_with_compression(
    path: Tail,
    accept_encoding: Option<String>,
) -> Result<impl Reply, warp::Rejection> {
    let path_str = path.as_str();
    let supports_gzip = accept_encoding
        .as_ref()
        .map(|enc| enc.contains("gzip"))
        .unwrap_or(false);

    // Get the content and content type based on path
    let (content, content_type, is_compressible) = match path_str {
        "css/style.css" | "style.css" => (STYLE_CSS, "text/css; charset=utf-8", true),
        "js/main.js" | "main.js" => (get_main_js(), "application/javascript; charset=utf-8", true),
        "images/logo.svg" | "logo.svg" => (LOGO_SVG, "image/svg+xml", true),
        _ => return Err(warp::reject::not_found()),
    };

    // Build response with or without compression
    if supports_gzip && is_compressible {
        // Compress content using flate2 (gzip)
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(content.as_bytes()).map_err(|_| warp::reject::not_found())?;
        let compressed = encoder.finish().map_err(|_| warp::reject::not_found())?;

        Ok(Response::builder()
            .header("Content-Type", content_type)
            .header("Content-Encoding", "gzip")
            .header("Cache-Control", "public, max-age=3600")
            .header("Vary", "Accept-Encoding")
            .body(compressed)
            .unwrap())
    } else {
        Ok(Response::builder()
            .header("Content-Type", content_type)
            .header("Cache-Control", "public, max-age=3600")
            .header("Vary", "Accept-Encoding")
            .body(content.as_bytes().to_vec())
            .unwrap())
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
