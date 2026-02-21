//! Route handlers for web server endpoints.
//! Owner: Scenario 3 - Static Asset Serving
//!
//! This module provides route handling for serving static content:
//! - GET / -> Homepage (index.html)
//! - GET /assets/* -> Static files (logo.gif, usage.gif)
//! - GET /css/* -> Stylesheets
//! - GET /js/* -> JavaScript files

use std::fs;
use std::path::{Path, PathBuf};

use super::assets::{get_cache_control, get_extension, get_mime_type, is_safe_path};

/// HTTP status codes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatusCode {
    Ok = 200,
    BadRequest = 400,
    NotFound = 404,
    InternalServerError = 500,
}

impl StatusCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusCode::Ok => "200 OK",
            StatusCode::BadRequest => "400 Bad Request",
            StatusCode::NotFound => "404 Not Found",
            StatusCode::InternalServerError => "500 Internal Server Error",
        }
    }
}

/// HTTP response
#[derive(Debug)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub content_type: String,
    pub cache_control: Option<String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Create a new HTTP response
    pub fn new(status: StatusCode, content_type: &str, body: Vec<u8>) -> Self {
        HttpResponse {
            status,
            content_type: content_type.to_string(),
            cache_control: None,
            body,
        }
    }

    /// Add Cache-Control header
    pub fn with_cache_control(mut self, cache_control: &str) -> Self {
        self.cache_control = Some(cache_control.to_string());
        self
    }

    /// Create a 404 Not Found response
    pub fn not_found() -> Self {
        let body = b"<!DOCTYPE html>\n<html>\n<head><title>404 Not Found</title></head>\n<body>\n<h1>404 Not Found</h1>\n<p>The requested resource was not found on this server.</p>\n</body>\n</html>".to_vec();
        HttpResponse::new(StatusCode::NotFound, "text/html; charset=utf-8", body)
    }

    /// Create a 400 Bad Request response
    pub fn bad_request() -> Self {
        let body = b"<!DOCTYPE html>\n<html>\n<head><title>400 Bad Request</title></head>\n<body>\n<h1>400 Bad Request</h1>\n<p>The request could not be understood by the server.</p>\n</body>\n</html>".to_vec();
        HttpResponse::new(StatusCode::BadRequest, "text/html; charset=utf-8", body)
    }

    /// Create a 500 Internal Server Error response
    pub fn internal_error() -> Self {
        let body = b"<!DOCTYPE html>\n<html>\n<head><title>500 Internal Server Error</title></head>\n<body>\n<h1>500 Internal Server Error</h1>\n<p>An unexpected error occurred.</p>\n</body>\n</html>".to_vec();
        HttpResponse::new(StatusCode::InternalServerError, "text/html; charset=utf-8", body)
    }

    /// Format the response as HTTP
    pub fn to_http_bytes(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
            self.status.as_str(),
            self.content_type,
            self.body.len()
        );

        if let Some(ref cache_control) = self.cache_control {
            response.push_str(&format!("Cache-Control: {}\r\n", cache_control));
        }

        response.push_str("\r\n");

        let mut bytes = response.into_bytes();
        bytes.extend_from_slice(&self.body);
        bytes
    }
}

/// Route configuration
pub struct RouteConfig {
    /// Base directory for web content (index.html, css/, js/)
    pub web_dir: PathBuf,
    /// Base directory for assets (logo.gif, usage.gif)
    pub assets_dir: PathBuf,
}

impl Default for RouteConfig {
    fn default() -> Self {
        RouteConfig {
            web_dir: PathBuf::from("web"),
            assets_dir: PathBuf::from("assets"),
        }
    }
}

impl RouteConfig {
    pub fn new(web_dir: PathBuf, assets_dir: PathBuf) -> Self {
        RouteConfig { web_dir, assets_dir }
    }
}

/// Route handler for incoming HTTP requests
pub struct Router {
    config: RouteConfig,
}

impl Router {
    pub fn new(config: RouteConfig) -> Self {
        Router { config }
    }

    /// Handle an incoming request path
    pub fn handle(&self, path: &str) -> HttpResponse {
        // Security check for path traversal
        if !is_safe_path(path) {
            return HttpResponse::bad_request();
        }

        // Normalize path
        let path = if path.starts_with('/') {
            &path[1..]
        } else {
            path
        };

        // Route to appropriate handler
        match path {
            "" | "index.html" => self.serve_homepage(),
            p if p.starts_with("assets/") => self.serve_asset(&p[7..]),
            p if p.starts_with("css/") => self.serve_web_file(p),
            p if p.starts_with("js/") => self.serve_web_file(p),
            _ => HttpResponse::not_found(),
        }
    }

    /// Serve the homepage (index.html)
    fn serve_homepage(&self) -> HttpResponse {
        let file_path = self.config.web_dir.join("index.html");
        self.serve_file(&file_path, ".html")
    }

    /// Serve a file from the assets directory
    fn serve_asset(&self, asset_path: &str) -> HttpResponse {
        // Additional security check for asset path
        if !is_safe_path(asset_path) {
            return HttpResponse::bad_request();
        }

        let file_path = self.config.assets_dir.join(asset_path);
        let extension = get_extension(asset_path);
        self.serve_file(&file_path, extension)
    }

    /// Serve a file from the web directory (css/, js/)
    fn serve_web_file(&self, relative_path: &str) -> HttpResponse {
        // Additional security check
        if !is_safe_path(relative_path) {
            return HttpResponse::bad_request();
        }

        let file_path = self.config.web_dir.join(relative_path);
        let extension = get_extension(relative_path);
        self.serve_file(&file_path, extension)
    }

    /// Serve a file with appropriate headers
    fn serve_file(&self, path: &Path, extension: &str) -> HttpResponse {
        // Check if file exists
        if !path.exists() || !path.is_file() {
            return HttpResponse::not_found();
        }

        // Read file content
        match fs::read(path) {
            Ok(content) => {
                let mime_type = get_mime_type(extension);
                let cache_control = get_cache_control(extension);
                HttpResponse::new(StatusCode::Ok, mime_type, content)
                    .with_cache_control(cache_control)
            }
            Err(_) => HttpResponse::internal_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    fn setup_test_dirs() -> (tempfile::TempDir, RouteConfig) {
        let temp_dir = tempdir().unwrap();

        // Create web directory structure
        let web_dir = temp_dir.path().join("web");
        let css_dir = web_dir.join("css");
        let js_dir = web_dir.join("js");
        let assets_dir = temp_dir.path().join("assets");

        fs::create_dir_all(&css_dir).unwrap();
        fs::create_dir_all(&js_dir).unwrap();
        fs::create_dir_all(&assets_dir).unwrap();

        // Create test files
        let mut index = File::create(web_dir.join("index.html")).unwrap();
        index.write_all(b"<html><body>Test</body></html>").unwrap();

        let mut css = File::create(css_dir.join("style.css")).unwrap();
        css.write_all(b"body { color: black; }").unwrap();

        let mut js = File::create(js_dir.join("main.js")).unwrap();
        js.write_all(b"console.log('test');").unwrap();

        // Create a test asset (small GIF)
        let mut gif = File::create(assets_dir.join("logo.gif")).unwrap();
        // Minimal valid GIF
        gif.write_all(&[
            0x47, 0x49, 0x46, 0x38, 0x39, 0x61, // GIF89a
            0x01, 0x00, 0x01, 0x00, // 1x1 pixel
            0x00, 0x00, 0x00, // Global color table flag
            0x3B, // Trailer
        ])
        .unwrap();

        let config = RouteConfig::new(web_dir, assets_dir);
        (temp_dir, config)
    }

    #[test]
    fn test_handle_homepage() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        let response = router.handle("/");
        assert_eq!(response.status, StatusCode::Ok);
        assert_eq!(response.content_type, "text/html; charset=utf-8");
        assert!(response.cache_control.is_some());
    }

    #[test]
    fn test_handle_index_html() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        let response = router.handle("/index.html");
        assert_eq!(response.status, StatusCode::Ok);
        assert_eq!(response.content_type, "text/html; charset=utf-8");
    }

    #[test]
    fn test_handle_css() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        let response = router.handle("/css/style.css");
        assert_eq!(response.status, StatusCode::Ok);
        assert_eq!(response.content_type, "text/css; charset=utf-8");
        assert!(response.cache_control.is_some());
    }

    #[test]
    fn test_handle_js() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        let response = router.handle("/js/main.js");
        assert_eq!(response.status, StatusCode::Ok);
        assert_eq!(response.content_type, "application/javascript; charset=utf-8");
    }

    #[test]
    fn test_handle_asset() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        let response = router.handle("/assets/logo.gif");
        assert_eq!(response.status, StatusCode::Ok);
        assert_eq!(response.content_type, "image/gif");
        assert!(response.cache_control.is_some());
    }

    #[test]
    fn test_handle_not_found() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        let response = router.handle("/nonexistent.html");
        assert_eq!(response.status, StatusCode::NotFound);
    }

    #[test]
    fn test_handle_path_traversal() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        // Various path traversal attempts
        let response = router.handle("/assets/../../../etc/passwd");
        assert!(response.status == StatusCode::BadRequest || response.status == StatusCode::NotFound);

        let response = router.handle("/../etc/passwd");
        assert!(response.status == StatusCode::BadRequest || response.status == StatusCode::NotFound);

        let response = router.handle("/..%2f..%2fetc/passwd");
        // URL decoding should happen before this, but raw % encoded paths
        // may be treated as valid or invalid depending on implementation
        assert!(response.status == StatusCode::BadRequest || response.status == StatusCode::NotFound);
    }

    #[test]
    fn test_cache_control_header() {
        let (_temp_dir, config) = setup_test_dirs();
        let router = Router::new(config);

        // GIF should have long cache
        let response = router.handle("/assets/logo.gif");
        assert_eq!(
            response.cache_control,
            Some("public, max-age=31536000".to_string())
        );

        // CSS should have medium cache
        let response = router.handle("/css/style.css");
        assert_eq!(
            response.cache_control,
            Some("public, max-age=86400".to_string())
        );

        // HTML should have short cache
        let response = router.handle("/");
        assert_eq!(
            response.cache_control,
            Some("public, max-age=300".to_string())
        );
    }

    #[test]
    fn test_http_response_format() {
        let response = HttpResponse::new(StatusCode::Ok, "text/plain", b"Hello".to_vec())
            .with_cache_control("public, max-age=3600");

        let bytes = response.to_http_bytes();
        let text = String::from_utf8_lossy(&bytes);

        assert!(text.contains("HTTP/1.1 200 OK"));
        assert!(text.contains("Content-Type: text/plain"));
        assert!(text.contains("Content-Length: 5"));
        assert!(text.contains("Cache-Control: public, max-age=3600"));
        assert!(text.ends_with("Hello"));
    }
}
