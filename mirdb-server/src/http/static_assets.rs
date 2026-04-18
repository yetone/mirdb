//! Embedded static file serving
//!
//! Owner: Scenario 9 (Static Web UI Serving)
//!
//! Expected functionality:
//! - Embed HTML/CSS/JS using include_str! or rust-embed
//! - Serve index.html at root path
//! - Set appropriate Content-Type headers
//! - Add caching headers for static assets

use hyper::{Body, Response, StatusCode};

// Embed static files at compile time
const INDEX_HTML: &str = include_str!("../web/index.html");
const STYLES_CSS: &str = include_str!("../web/styles.css");
const APP_JS: &str = include_str!("../web/app.js");

/// Serve the main HTML page
pub fn serve_html() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .header("Cache-Control", "no-cache")
        .body(Body::from(INDEX_HTML))
        .unwrap()
}

/// Serve the CSS stylesheet
pub fn serve_css() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/css; charset=utf-8")
        .header("Cache-Control", "max-age=3600")
        .body(Body::from(STYLES_CSS))
        .unwrap()
}

/// Serve the JavaScript application
pub fn serve_js() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/javascript; charset=utf-8")
        .header("Cache-Control", "max-age=3600")
        .body(Body::from(APP_JS))
        .unwrap()
}
