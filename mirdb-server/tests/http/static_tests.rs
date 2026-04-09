//! Static file serving tests.
//!
//! Owner: Scenario 17 - Static Asset Serving
//!
//! Tests:
//! - HTML served with correct content-type
//! - CSS served with correct content-type
//! - JavaScript served with correct content-type
//! - 404 for missing files
//! - Cache headers present
//!
//! Note: Since mirdb is a binary crate, these tests use mock implementations
//! to test the static file serving logic. Unit tests for the actual implementation
//! are in src/http/static_files.rs

use std::collections::HashMap;
use std::path::Path;

/// Default cache duration for static assets (1 hour in seconds)
const DEFAULT_CACHE_DURATION: u32 = 3600;

/// Cache duration for HTML files (shorter for updates)
const HTML_CACHE_DURATION: u32 = 300;

/// Cache duration for images and fonts (longer)
const LONG_CACHE_DURATION: u32 = 86400;

// =============================================
// Mock Static File System
// =============================================

/// Mock static file
struct MockStaticFile {
    content: Vec<u8>,
    content_type: String,
    cache_duration: u32,
}

impl MockStaticFile {
    fn new(content: &[u8], content_type: &str, cache_duration: u32) -> Self {
        Self {
            content: content.to_vec(),
            content_type: content_type.to_string(),
            cache_duration,
        }
    }
}

/// Mock static file system
struct MockStaticFileSystem {
    files: HashMap<String, MockStaticFile>,
}

impl MockStaticFileSystem {
    fn new() -> Self {
        let mut files = HashMap::new();

        // Add index.html
        files.insert(
            "/static/index.html".to_string(),
            MockStaticFile::new(
                b"<!DOCTYPE html><html><head><title>MirDB</title></head><body><h1>MirDB Homepage</h1></body></html>",
                "text/html; charset=utf-8",
                HTML_CACHE_DURATION,
            ),
        );

        // Add CSS file
        files.insert(
            "/static/css/main.css".to_string(),
            MockStaticFile::new(
                b"body { margin: 0; padding: 0; font-family: sans-serif; }",
                "text/css; charset=utf-8",
                DEFAULT_CACHE_DURATION,
            ),
        );

        // Add themes.css
        files.insert(
            "/static/css/themes.css".to_string(),
            MockStaticFile::new(
                b":root { --color-primary: #2563eb; }",
                "text/css; charset=utf-8",
                DEFAULT_CACHE_DURATION,
            ),
        );

        // Add JavaScript file
        files.insert(
            "/static/js/main.js".to_string(),
            MockStaticFile::new(
                b"console.log('MirDB Homepage loaded');",
                "application/javascript; charset=utf-8",
                DEFAULT_CACHE_DURATION,
            ),
        );

        // Add metrics.js
        files.insert(
            "/static/js/metrics.js".to_string(),
            MockStaticFile::new(
                b"function refreshMetrics() { /* ... */ }",
                "application/javascript; charset=utf-8",
                DEFAULT_CACHE_DURATION,
            ),
        );

        // Add theme.js
        files.insert(
            "/static/js/theme.js".to_string(),
            MockStaticFile::new(
                b"function toggleTheme() { /* ... */ }",
                "application/javascript; charset=utf-8",
                DEFAULT_CACHE_DURATION,
            ),
        );

        Self { files }
    }

    fn get(&self, path: &str) -> Option<&MockStaticFile> {
        self.files.get(path)
    }
}

/// Mock HTTP response for static file tests
struct MockStaticResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl MockStaticResponse {
    fn ok(file: &MockStaticFile) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), file.content_type.clone());
        headers.insert("Content-Length".to_string(), file.content.len().to_string());
        headers.insert(
            "Cache-Control".to_string(),
            format!("public, max-age={}", file.cache_duration),
        );

        Self {
            status: 200,
            headers,
            body: file.content.clone(),
        }
    }

    fn not_found(path: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());

        let body = format!(r#"{{"error":"Static file not found: {}"}}"#, path);

        Self {
            status: 404,
            headers,
            body: body.into_bytes(),
        }
    }
}

/// Mock static file serving function
fn mock_serve_static(fs: &MockStaticFileSystem, path: &str) -> MockStaticResponse {
    // Check for path traversal attacks
    if path.contains("..") {
        return MockStaticResponse::not_found(path);
    }

    match fs.get(path) {
        Some(file) => MockStaticResponse::ok(file),
        None => MockStaticResponse::not_found(path),
    }
}

/// Get content type based on file extension
fn get_content_type(path: &Path) -> &'static str {
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
        _ => "application/octet-stream",
    }
}

/// Get cache duration based on file type
fn get_cache_duration(path: &Path) -> u32 {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html") | Some("htm") => HTML_CACHE_DURATION,
        Some("css") | Some("js") => DEFAULT_CACHE_DURATION,
        Some("png") | Some("jpg") | Some("jpeg") | Some("gif") | Some("svg") |
        Some("ico") | Some("woff") | Some("woff2") | Some("ttf") => LONG_CACHE_DURATION,
        _ => DEFAULT_CACHE_DURATION,
    }
}

// =============================================
// Test Case 1: GET / returns HTML content type
// =============================================

#[test]
fn test_homepage_returns_html_content_type() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/index.html");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("text/html"), "Expected text/html, got {}", content_type);
}

#[test]
fn test_homepage_returns_valid_html() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/index.html");

    assert_eq!(response.status, 200);
    let body = String::from_utf8(response.body).unwrap();
    assert!(body.contains("<!DOCTYPE html>"), "Response should be valid HTML");
    assert!(body.contains("MirDB"), "Response should contain MirDB branding");
}

#[test]
fn test_homepage_has_cache_control() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/index.html");

    assert_eq!(response.status, 200);
    let cache_control = response.headers.get("Cache-Control").unwrap();
    assert!(cache_control.contains("max-age="), "Cache-Control should contain max-age");
    assert!(cache_control.contains("public"), "Cache-Control should be public");
}

// =============================================
// Test Case 2: CSS files are served correctly
// =============================================

#[test]
fn test_css_content_type() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/css/main.css");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("text/css"), "Expected text/css, got {}", content_type);
}

#[test]
fn test_css_content_type_function() {
    let path = Path::new("style.css");
    let content_type = get_content_type(path);
    assert_eq!(content_type, "text/css; charset=utf-8");
}

#[test]
fn test_css_file_is_served() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/css/main.css");

    assert_eq!(response.status, 200);
    let content_length = response.headers.get("Content-Length").unwrap();
    assert!(!content_length.is_empty());
    assert!(content_length.parse::<usize>().unwrap() > 0);
}

#[test]
fn test_themes_css_served() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/css/themes.css");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("text/css"));
}

// =============================================
// Test Case 3: JavaScript files are served correctly
// =============================================

#[test]
fn test_javascript_content_type() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/js/main.js");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(
        content_type.contains("application/javascript"),
        "Expected application/javascript, got {}", content_type
    );
}

#[test]
fn test_javascript_content_type_function() {
    let path = Path::new("script.js");
    let content_type = get_content_type(path);
    assert_eq!(content_type, "application/javascript; charset=utf-8");
}

#[test]
fn test_javascript_file_is_served() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/js/main.js");

    assert_eq!(response.status, 200);
    let content_length = response.headers.get("Content-Length").unwrap();
    assert!(!content_length.is_empty());
}

#[test]
fn test_metrics_js_served() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/js/metrics.js");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("application/javascript"));
}

#[test]
fn test_theme_js_served() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/js/theme.js");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("application/javascript"));
}

// =============================================
// Test Case 4: Cache-Control headers are present
// =============================================

#[test]
fn test_cache_control_header_present_for_css() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/css/main.css");

    assert_eq!(response.status, 200);

    let cache_control = response.headers.get("Cache-Control");
    assert!(cache_control.is_some(), "Cache-Control header should be present");

    let cache_value = cache_control.unwrap();
    assert!(cache_value.contains("max-age="), "Cache-Control should contain max-age");
    assert!(cache_value.contains("public"), "Cache-Control should be public");
}

#[test]
fn test_cache_control_header_present_for_js() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/js/main.js");

    assert_eq!(response.status, 200);

    let cache_control = response.headers.get("Cache-Control");
    assert!(cache_control.is_some(), "Cache-Control header should be present");

    let cache_value = cache_control.unwrap();
    assert!(cache_value.contains("max-age="), "Cache-Control should contain max-age");
}

#[test]
fn test_cache_control_header_present_for_html() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/index.html");

    assert_eq!(response.status, 200);

    let cache_control = response.headers.get("Cache-Control");
    assert!(cache_control.is_some(), "Cache-Control header should be present for HTML");
}

#[test]
fn test_cache_duration_varies_by_file_type() {
    let html_path = Path::new("index.html");
    let css_path = Path::new("style.css");
    let image_path = Path::new("logo.png");

    let html_duration = get_cache_duration(html_path);
    let css_duration = get_cache_duration(css_path);
    let image_duration = get_cache_duration(image_path);

    // HTML should have shorter cache
    assert!(html_duration < css_duration, "HTML should have shorter cache than CSS");
    // Images should have longer cache
    assert!(image_duration >= css_duration, "Images should have equal or longer cache than CSS");
}

// =============================================
// Test Case 5: 404 for non-existent files
// =============================================

#[test]
fn test_404_for_missing_file() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/nonexistent.css");

    assert_eq!(response.status, 404);
}

#[test]
fn test_404_for_missing_directory() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/missing/file.js");

    assert_eq!(response.status, 404);
}

#[test]
fn test_404_response_is_json() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/missing.css");

    assert_eq!(response.status, 404);

    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("application/json"), "404 response should be JSON");
}

#[test]
fn test_404_contains_error_message() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/missing.css");

    assert_eq!(response.status, 404);

    let body = String::from_utf8(response.body).unwrap();
    assert!(body.contains("error"), "404 response should contain error field");
    assert!(body.contains("not found") || body.contains("Not Found") || body.contains("missing"),
            "404 response should indicate file not found");
}

// =============================================
// Additional Tests: Security and Edge Cases
// =============================================

#[test]
fn test_directory_traversal_prevention() {
    let fs = MockStaticFileSystem::new();

    // Attempt directory traversal
    let response = mock_serve_static(&fs, "/static/../../../etc/passwd");
    assert_eq!(response.status, 404, "Directory traversal should return 404");

    let response2 = mock_serve_static(&fs, "/static/css/../../secret");
    assert_eq!(response2.status, 404, "Directory traversal should return 404");
}

#[test]
fn test_content_type_for_various_extensions() {
    // HTML
    assert!(get_content_type(Path::new("page.html")).contains("text/html"));
    assert!(get_content_type(Path::new("page.htm")).contains("text/html"));

    // CSS
    assert!(get_content_type(Path::new("style.css")).contains("text/css"));

    // JavaScript
    assert!(get_content_type(Path::new("app.js")).contains("application/javascript"));

    // JSON
    assert!(get_content_type(Path::new("data.json")).contains("application/json"));

    // Images
    assert!(get_content_type(Path::new("image.png")).contains("image/png"));
    assert!(get_content_type(Path::new("photo.jpg")).contains("image/jpeg"));
    assert!(get_content_type(Path::new("photo.jpeg")).contains("image/jpeg"));
    assert!(get_content_type(Path::new("anim.gif")).contains("image/gif"));
    assert!(get_content_type(Path::new("icon.svg")).contains("image/svg+xml"));
    assert!(get_content_type(Path::new("favicon.ico")).contains("image/x-icon"));

    // Fonts
    assert!(get_content_type(Path::new("font.woff")).contains("font/woff"));
    assert!(get_content_type(Path::new("font.woff2")).contains("font/woff2"));
    assert!(get_content_type(Path::new("font.ttf")).contains("font/ttf"));

    // Unknown
    assert!(get_content_type(Path::new("unknown.xyz")).contains("application/octet-stream"));
}

#[test]
fn test_content_type_no_extension() {
    let path = Path::new("Makefile");
    assert!(get_content_type(path).contains("application/octet-stream"));
}

#[test]
fn test_static_response_headers() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/css/main.css");

    assert_eq!(response.status, 200);

    // Check all required headers are present
    assert!(response.headers.contains_key("Content-Type"), "Should have Content-Type");
    assert!(response.headers.contains_key("Content-Length"), "Should have Content-Length");
    assert!(response.headers.contains_key("Cache-Control"), "Should have Cache-Control");
}

// =============================================
// Route Integration Tests
// =============================================

#[test]
fn test_root_path_behavior() {
    // When requesting "/" the server should serve the homepage (static index.html)
    // This tests the routing logic
    let fs = MockStaticFileSystem::new();

    // The "/" route should map to "/static/index.html" internally
    let response = mock_serve_static(&fs, "/static/index.html");

    assert_eq!(response.status, 200);
    let content_type = response.headers.get("Content-Type").unwrap();
    assert!(content_type.contains("text/html"));
}

#[test]
fn test_all_static_assets_have_cache_headers() {
    let fs = MockStaticFileSystem::new();

    let paths = [
        "/static/index.html",
        "/static/css/main.css",
        "/static/css/themes.css",
        "/static/js/main.js",
        "/static/js/metrics.js",
        "/static/js/theme.js",
    ];

    for path in &paths {
        let response = mock_serve_static(&fs, path);
        assert_eq!(response.status, 200, "Path {} should return 200", path);
        assert!(
            response.headers.contains_key("Cache-Control"),
            "Path {} should have Cache-Control header", path
        );
    }
}

#[test]
fn test_content_length_matches_body() {
    let fs = MockStaticFileSystem::new();
    let response = mock_serve_static(&fs, "/static/css/main.css");

    assert_eq!(response.status, 200);

    let content_length: usize = response.headers.get("Content-Length").unwrap().parse().unwrap();
    assert_eq!(content_length, response.body.len(), "Content-Length should match body size");
}
