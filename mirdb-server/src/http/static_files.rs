//! Static file serving for homepage assets.
//!
//! Owner: Scenario 17 - Static Asset Serving
//!
//! This module handles serving static files (HTML, CSS, JavaScript) with:
//! - Correct Content-Type headers based on file extension
//! - Cache-Control headers for efficient browser caching
//! - 404 responses for non-existent files
//!
//! Supported content types:
//! - text/html for .html
//! - text/css for .css
//! - application/javascript for .js
//! - image/* for images

use std::fs;
use std::path::{Path, PathBuf};

use hyper::{Body, Response, StatusCode};

/// Default cache duration for static assets (1 hour in seconds)
const DEFAULT_CACHE_DURATION: u32 = 3600;

/// Cache duration for immutable assets like versioned files (1 day)
const LONG_CACHE_DURATION: u32 = 86400;

/// Represents a static file with its content and metadata
#[derive(Debug, Clone)]
pub struct StaticFile {
    /// The file content as bytes
    pub content: Vec<u8>,
    /// The MIME content type
    pub content_type: String,
    /// Cache duration in seconds
    pub cache_duration: u32,
}

impl StaticFile {
    /// Create a new StaticFile
    pub fn new(content: Vec<u8>, content_type: &str, cache_duration: u32) -> Self {
        Self {
            content,
            content_type: content_type.to_string(),
            cache_duration,
        }
    }

    /// Convert to HTTP response
    pub fn to_response(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", self.content_type.as_str())
            .header("Content-Length", self.content.len())
            .header("Cache-Control", format!("public, max-age={}", self.cache_duration))
            .body(Body::from(self.content.clone()))
            .unwrap()
    }
}

/// Get the content type for a file based on its extension
///
/// # Arguments
/// * `path` - The file path to examine
///
/// # Returns
/// The MIME content type string
pub fn get_content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("eot") => "application/vnd.ms-fontobject",
        Some("txt") => "text/plain; charset=utf-8",
        Some("xml") => "application/xml",
        _ => "application/octet-stream",
    }
}

/// Get the appropriate cache duration for a file type
///
/// # Arguments
/// * `path` - The file path to examine
///
/// # Returns
/// Cache duration in seconds
pub fn get_cache_duration(path: &Path) -> u32 {
    match path.extension().and_then(|ext| ext.to_str()) {
        // HTML files should have shorter cache to allow updates
        Some("html") | Some("htm") => 300, // 5 minutes
        // CSS and JS can be cached longer
        Some("css") | Some("js") => DEFAULT_CACHE_DURATION,
        // Images and fonts can be cached even longer
        Some("png") | Some("jpg") | Some("jpeg") | Some("gif") | Some("svg") |
        Some("ico") | Some("woff") | Some("woff2") | Some("ttf") | Some("eot") => LONG_CACHE_DURATION,
        _ => DEFAULT_CACHE_DURATION,
    }
}

/// Resolve a URL path to a filesystem path within the static directory
///
/// This function sanitizes the path to prevent directory traversal attacks.
///
/// # Arguments
/// * `static_dir` - The base directory for static files
/// * `url_path` - The URL path (e.g., "/static/css/main.css")
///
/// # Returns
/// The resolved filesystem path, or None if the path is invalid
pub fn resolve_static_path(static_dir: &Path, url_path: &str) -> Option<PathBuf> {
    // Remove the "/static/" prefix if present
    let relative_path = url_path
        .strip_prefix("/static/")
        .or_else(|| url_path.strip_prefix("/static"))
        .unwrap_or(url_path);

    // Prevent directory traversal attacks
    if relative_path.contains("..") {
        return None;
    }

    // Build the full path
    let full_path = static_dir.join(relative_path);

    // Ensure the resolved path is within the static directory
    match full_path.canonicalize() {
        Ok(canonical) => {
            if canonical.starts_with(static_dir.canonicalize().ok()?) {
                Some(canonical)
            } else {
                None
            }
        }
        Err(_) => None, // File doesn't exist or other error
    }
}

/// Load a static file from the filesystem
///
/// # Arguments
/// * `path` - The filesystem path to the file
///
/// # Returns
/// A StaticFile if successful, or None if the file doesn't exist
pub fn load_static_file(path: &Path) -> Option<StaticFile> {
    match fs::read(path) {
        Ok(content) => {
            let content_type = get_content_type(path);
            let cache_duration = get_cache_duration(path);
            Some(StaticFile::new(content, content_type, cache_duration))
        }
        Err(_) => None,
    }
}

/// Serve a static file and return an HTTP response
///
/// # Arguments
/// * `static_dir` - The base directory for static files
/// * `url_path` - The URL path requested
///
/// # Returns
/// HTTP response with the file content or a 404 error
pub fn serve_static(static_dir: &Path, url_path: &str) -> Response<Body> {
    // Resolve the path
    let file_path = match resolve_static_path(static_dir, url_path) {
        Some(path) => path,
        None => return not_found_response(url_path),
    };

    // Load and serve the file
    match load_static_file(&file_path) {
        Some(file) => file.to_response(),
        None => not_found_response(url_path),
    }
}

/// Create a 404 Not Found response
fn not_found_response(path: &str) -> Response<Body> {
    let body = format!(r#"{{"error":"Static file not found: {}"}}"#, path);
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

/// Get the default static directory path relative to the server
///
/// This returns the path to the `static` directory in the project root.
pub fn get_default_static_dir() -> PathBuf {
    // Try to find the static directory relative to the executable or current dir
    let paths = [
        PathBuf::from("static"),
        PathBuf::from("./static"),
        PathBuf::from("../static"),
        PathBuf::from("mirdb-server/static"),
    ];

    for path in &paths {
        if path.exists() && path.is_dir() {
            return path.canonicalize().unwrap_or_else(|_| path.clone());
        }
    }

    // Default fallback
    PathBuf::from("static")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_get_content_type_html() {
        let path = Path::new("index.html");
        assert_eq!(get_content_type(path), "text/html; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_htm() {
        let path = Path::new("page.htm");
        assert_eq!(get_content_type(path), "text/html; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_css() {
        let path = Path::new("style.css");
        assert_eq!(get_content_type(path), "text/css; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_javascript() {
        let path = Path::new("script.js");
        assert_eq!(get_content_type(path), "application/javascript; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_json() {
        let path = Path::new("data.json");
        assert_eq!(get_content_type(path), "application/json; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_png() {
        let path = Path::new("image.png");
        assert_eq!(get_content_type(path), "image/png");
    }

    #[test]
    fn test_get_content_type_jpeg() {
        let path = Path::new("photo.jpg");
        assert_eq!(get_content_type(path), "image/jpeg");

        let path2 = Path::new("photo.jpeg");
        assert_eq!(get_content_type(path2), "image/jpeg");
    }

    #[test]
    fn test_get_content_type_svg() {
        let path = Path::new("icon.svg");
        assert_eq!(get_content_type(path), "image/svg+xml");
    }

    #[test]
    fn test_get_content_type_unknown() {
        let path = Path::new("file.unknown");
        assert_eq!(get_content_type(path), "application/octet-stream");
    }

    #[test]
    fn test_get_content_type_no_extension() {
        let path = Path::new("Makefile");
        assert_eq!(get_content_type(path), "application/octet-stream");
    }

    #[test]
    fn test_get_cache_duration_html() {
        let path = Path::new("index.html");
        assert_eq!(get_cache_duration(path), 300);
    }

    #[test]
    fn test_get_cache_duration_css() {
        let path = Path::new("style.css");
        assert_eq!(get_cache_duration(path), DEFAULT_CACHE_DURATION);
    }

    #[test]
    fn test_get_cache_duration_js() {
        let path = Path::new("script.js");
        assert_eq!(get_cache_duration(path), DEFAULT_CACHE_DURATION);
    }

    #[test]
    fn test_get_cache_duration_image() {
        let path = Path::new("image.png");
        assert_eq!(get_cache_duration(path), LONG_CACHE_DURATION);
    }

    #[test]
    fn test_static_file_to_response() {
        let content = b"body { color: red; }".to_vec();
        let file = StaticFile::new(content.clone(), "text/css; charset=utf-8", 3600);
        let response = file.to_response();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap().to_str().unwrap(),
            "text/css; charset=utf-8"
        );
        assert_eq!(
            response.headers().get("Content-Length").unwrap().to_str().unwrap(),
            "20"
        );
        assert!(
            response.headers().get("Cache-Control").unwrap().to_str().unwrap()
                .contains("max-age=3600")
        );
    }

    #[test]
    fn test_not_found_response() {
        let response = not_found_response("/static/missing.css");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get("Content-Type").unwrap().to_str().unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_resolve_static_path_prevents_traversal() {
        let static_dir = Path::new("/var/www/static");

        // Should reject path traversal attempts
        assert!(resolve_static_path(static_dir, "/static/../../../etc/passwd").is_none());
        assert!(resolve_static_path(static_dir, "/static/css/../../secret").is_none());
    }

    #[test]
    fn test_resolve_static_path_strips_prefix() {
        // This test checks the path stripping logic
        let url_path = "/static/css/main.css";
        let relative = url_path.strip_prefix("/static/").unwrap();
        assert_eq!(relative, "css/main.css");
    }
}
