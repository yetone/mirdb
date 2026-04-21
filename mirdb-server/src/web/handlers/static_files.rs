//! Static file serving handler.
//! Owner: Scenario 2 - Homepage Static Content Serving
//!
//! Serves embedded HTML, CSS, JS assets with appropriate headers:
//! - Content-Type headers based on file extension
//! - Cache-Control headers (min 1 hour for static assets per NFR-4)
//! - Returns 404 for missing files

use axum::{
    extract::Path,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
};

use crate::web::assets::{get_asset, get_content_type};

/// Cache duration for static assets in seconds (1 hour minimum per NFR-4)
const CACHE_MAX_AGE: u32 = 3600;

/// Serve the homepage HTML
///
/// Returns the embedded index.html with Content-Type: text/html
pub async fn serve_homepage() -> impl IntoResponse {
    match get_asset("index.html") {
        Some(file) => {
            let html = String::from_utf8_lossy(&file.data).to_string();
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            // HTML pages get shorter cache (they might change more frequently)
            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=300"),
            );
            (StatusCode::OK, headers, Html(html))
        }
        None => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                headers,
                Html("<h1>500 - Homepage not found</h1>".to_string()),
            )
        }
    }
}

/// Serve static assets (CSS, JS, images, etc.)
///
/// # Arguments
/// * `path` - The path to the static file (e.g., "style.css", "main.js")
///
/// # Returns
/// The file content with appropriate Content-Type and Cache-Control headers,
/// or 404 if the file is not found.
pub async fn serve_static(Path(path): Path<String>) -> Response {
    match get_asset(&path) {
        Some(file) => {
            let content_type = get_content_type(&path);
            let body = file.data.to_vec();

            let mut headers = HeaderMap::new();

            // Set Content-Type
            if let Ok(value) = HeaderValue::from_str(content_type) {
                headers.insert(header::CONTENT_TYPE, value);
            }

            // Set Cache-Control (min 1 hour per NFR-4)
            let cache_control = format!("public, max-age={}", CACHE_MAX_AGE);
            if let Ok(value) = HeaderValue::from_str(&cache_control) {
                headers.insert(header::CACHE_CONTROL, value);
            }

            // Set ETag for conditional requests
            let hash = file.metadata.sha256_hash();
            let etag = format!("\"{}\"", hex::encode(&hash[..8]));
            if let Ok(value) = HeaderValue::from_str(&etag) {
                headers.insert(header::ETAG, value);
            }

            (StatusCode::OK, headers, body).into_response()
        }
        None => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            (
                StatusCode::NOT_FOUND,
                headers,
                "404 - File not found".as_bytes().to_vec(),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_async<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn test_serve_homepage_returns_html() {
        run_async(async {
            let response = serve_homepage().await;
            let response = response.into_response();
            assert_eq!(response.status(), StatusCode::OK);
        })
    }

    #[test]
    fn test_serve_static_not_found() {
        run_async(async {
            let response = serve_static(Path("nonexistent.css".to_string())).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        })
    }
}
