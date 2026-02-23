//! HTTP request handlers for static content and assets.
//! Owner: Scenario 2 - Homepage Static Content Rendering
//! Co-owners: Scenario 3 (assets), Scenario 12 (error responses)
//!
//! This module provides request handling and routing for the HTTP server.

use std::sync::Arc;

use crate::store::Store;
use super::assets;

/// HTTP Request representation
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<String>,
    pub body: Vec<u8>,
}

/// HTTP Response representation
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub status_text: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Create a new HTTP response
    pub fn new(status_code: u16, status_text: &str, content_type: &str, body: Vec<u8>) -> Self {
        HttpResponse {
            status_code,
            status_text: status_text.to_string(),
            headers: vec![
                ("Content-Type".to_string(), content_type.to_string()),
                ("Connection".to_string(), "close".to_string()),
            ],
            body,
        }
    }

    /// Create a 200 OK response with HTML content
    pub fn ok_html(body: &str) -> Self {
        HttpResponse::new(200, "OK", "text/html; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 200 OK response with CSS content
    pub fn ok_css(body: &str) -> Self {
        HttpResponse::new(200, "OK", "text/css; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 200 OK response with JavaScript content
    pub fn ok_js(body: &str) -> Self {
        HttpResponse::new(200, "OK", "application/javascript; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 200 OK response with JSON content
    pub fn ok_json(body: &str) -> Self {
        HttpResponse::new(200, "OK", "application/json; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 200 OK response with binary content
    pub fn ok_binary(content_type: &str, body: Vec<u8>) -> Self {
        HttpResponse::new(200, "OK", content_type, body)
    }

    /// Create a 404 Not Found response
    pub fn not_found(message: &str) -> Self {
        let body = format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>404 Not Found</title>
    <style>
        body {{ font-family: sans-serif; text-align: center; padding: 50px; }}
        h1 {{ font-size: 48px; color: #333; }}
        p {{ color: #666; }}
    </style>
</head>
<body>
    <h1>404</h1>
    <p>{}</p>
    <p><a href="/">Return to homepage</a></p>
</body>
</html>"#,
            message
        );
        HttpResponse::new(404, "Not Found", "text/html; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 400 Bad Request response
    pub fn bad_request(message: &str) -> Self {
        let body = format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>400 Bad Request</title>
</head>
<body>
    <h1>400 Bad Request</h1>
    <p>{}</p>
</body>
</html>"#,
            message
        );
        HttpResponse::new(400, "Bad Request", "text/html; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 500 Internal Server Error response
    pub fn internal_error(message: &str) -> Self {
        let body = format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>500 Internal Server Error</title>
</head>
<body>
    <h1>500 Internal Server Error</h1>
    <p>{}</p>
</body>
</html>"#,
            message
        );
        HttpResponse::new(500, "Internal Server Error", "text/html; charset=utf-8", body.as_bytes().to_vec())
    }

    /// Create a 405 Method Not Allowed response
    pub fn method_not_allowed() -> Self {
        let body = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>405 Method Not Allowed</title>
</head>
<body>
    <h1>405 Method Not Allowed</h1>
</body>
</html>"#;
        HttpResponse::new(405, "Method Not Allowed", "text/html; charset=utf-8", body.as_bytes().to_vec())
    }
}

/// Route and handle an HTTP request
pub fn handle_request(request: &HttpRequest, store: &Arc<Store>) -> HttpResponse {
    // Route based on path and method
    match (request.method.as_str(), request.path.as_str()) {
        // Homepage
        ("GET", "/") | ("GET", "/index.html") => {
            handle_homepage()
        }

        // Static assets
        ("GET", "/styles.css") => {
            handle_styles()
        }
        ("GET", "/console.js") => {
            handle_console_js()
        }
        ("GET", "/logo.gif") => {
            handle_logo()
        }

        // Console API - handled by console_api module
        ("POST", "/api/console") => {
            super::console_api::handle_console_command(request, store)
        }

        // 404 for unknown paths
        ("GET", path) => {
            HttpResponse::not_found(&format!("Page not found: {}", path))
        }

        // 405 for unsupported methods
        (_, _) => {
            HttpResponse::method_not_allowed()
        }
    }
}

/// Handle homepage request
fn handle_homepage() -> HttpResponse {
    HttpResponse::ok_html(assets::HOMEPAGE_HTML)
}

/// Handle styles.css request
fn handle_styles() -> HttpResponse {
    HttpResponse::ok_css(assets::STYLES_CSS)
}

/// Handle console.js request
fn handle_console_js() -> HttpResponse {
    HttpResponse::ok_js(assets::CONSOLE_JS)
}

/// Handle logo.gif request
fn handle_logo() -> HttpResponse {
    HttpResponse::ok_binary("image/gif", assets::LOGO_GIF.to_vec())
}

/// Returns the homepage HTML content (for testing)
pub fn get_homepage_html() -> &'static str {
    assets::HOMEPAGE_HTML
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_response_ok_html() {
        let response = HttpResponse::ok_html("<h1>Test</h1>");
        assert_eq!(response.status_code, 200);
        assert_eq!(response.status_text, "OK");
        assert!(response.headers.iter().any(|(k, v)| k == "Content-Type" && v.contains("text/html")));
    }

    #[test]
    fn test_http_response_not_found() {
        let response = HttpResponse::not_found("Page missing");
        assert_eq!(response.status_code, 404);
        assert_eq!(response.status_text, "Not Found");
    }

    #[test]
    fn test_http_response_bad_request() {
        let response = HttpResponse::bad_request("Invalid input");
        assert_eq!(response.status_code, 400);
        assert_eq!(response.status_text, "Bad Request");
    }

    #[test]
    fn test_homepage_contains_logo() {
        let html = get_homepage_html();
        assert!(html.contains("assets/logo.gif"), "Homepage should contain logo reference");
    }

    #[test]
    fn test_homepage_contains_title() {
        let html = get_homepage_html();
        assert!(
            html.contains("MirDB: A Persistent Key-Value Store with Memcached Protocol"),
            "Homepage should contain the project title"
        );
    }

    #[test]
    fn test_homepage_contains_nav_features() {
        let html = get_homepage_html();
        assert!(html.contains("Features"), "Homepage should contain Features navigation link");
    }

    #[test]
    fn test_homepage_contains_nav_documentation() {
        let html = get_homepage_html();
        assert!(html.contains("Documentation"), "Homepage should contain Documentation navigation link");
    }

    #[test]
    fn test_homepage_contains_nav_github() {
        let html = get_homepage_html();
        assert!(html.contains("GitHub"), "Homepage should contain GitHub navigation link");
    }

    #[test]
    fn test_homepage_contains_feature_memcached() {
        let html = get_homepage_html();
        assert!(
            html.contains("Memcached Protocol"),
            "Homepage should contain memcached protocol feature"
        );
    }

    #[test]
    fn test_homepage_contains_feature_skiplist() {
        let html = get_homepage_html();
        assert!(
            html.contains("Skip-List Memtable") || html.contains("skip-list"),
            "Homepage should contain skip-list memtable feature"
        );
    }

    #[test]
    fn test_homepage_contains_feature_compaction() {
        let html = get_homepage_html();
        assert!(
            html.contains("Compaction") || html.contains("compaction"),
            "Homepage should contain compaction feature"
        );
    }

    #[test]
    fn test_homepage_contains_feature_persistence() {
        let html = get_homepage_html();
        assert!(
            html.contains("Persistence") || html.contains("persistent"),
            "Homepage should contain persistence feature"
        );
    }

    #[test]
    fn test_homepage_contains_github_link() {
        let html = get_homepage_html();
        assert!(
            html.contains("github.com"),
            "Homepage should contain GitHub repository link"
        );
    }

    #[test]
    fn test_homepage_has_valid_doctype() {
        let html = get_homepage_html();
        assert!(
            html.trim_start().starts_with("<!DOCTYPE html>"),
            "Homepage should have valid HTML5 doctype"
        );
    }

    #[test]
    fn test_homepage_has_lang_attribute() {
        let html = get_homepage_html();
        assert!(
            html.contains("<html lang="),
            "Homepage should have lang attribute for accessibility"
        );
    }

    #[test]
    fn test_homepage_has_h1_heading() {
        let html = get_homepage_html();
        assert!(
            html.contains("<h1>") && html.contains("</h1>"),
            "Homepage should have an h1 heading"
        );
    }

    #[test]
    fn test_homepage_has_main_element() {
        let html = get_homepage_html();
        assert!(
            html.contains("<main") && html.contains("</main>"),
            "Homepage should have semantic main element"
        );
    }

    #[test]
    fn test_homepage_has_header_element() {
        let html = get_homepage_html();
        assert!(
            html.contains("<header") && html.contains("</header>"),
            "Homepage should have semantic header element"
        );
    }

    #[test]
    fn test_homepage_has_footer_element() {
        let html = get_homepage_html();
        assert!(
            html.contains("<footer") && html.contains("</footer>"),
            "Homepage should have semantic footer element"
        );
    }

    #[test]
    fn test_homepage_has_nav_element() {
        let html = get_homepage_html();
        assert!(
            html.contains("<nav") && html.contains("</nav>"),
            "Homepage should have semantic nav element"
        );
    }

    #[test]
    fn test_homepage_has_features_section() {
        let html = get_homepage_html();
        assert!(
            html.contains("id=\"features\""),
            "Homepage should have features section with id"
        );
    }

    #[test]
    fn test_homepage_heading_hierarchy() {
        let html = get_homepage_html();
        // Check that h1 exists and h2 elements follow
        let h1_pos = html.find("<h1>").expect("h1 should exist");
        let h2_pos = html.find("<h2").expect("h2 should exist");
        assert!(h1_pos < h2_pos, "h1 should appear before h2 for proper heading hierarchy");
    }

    // === Documentation Section Tests (Scenario 8) ===

    #[test]
    fn test_documentation_section_contains_cargo_build_instructions() {
        let html = get_homepage_html();
        assert!(
            html.contains("cargo build"),
            "Documentation section should contain cargo build command"
        );
        assert!(
            html.contains("cargo build --release"),
            "Documentation section should contain 'cargo build --release' command"
        );
    }

    #[test]
    fn test_documentation_section_contains_config_file_instructions() {
        let html = get_homepage_html();
        assert!(
            html.contains("etc/mirdb.toml"),
            "Documentation section should contain configuration file path etc/mirdb.toml"
        );
    }

    #[test]
    fn test_documentation_section_contains_set_command_example() {
        let html = get_homepage_html();
        // Check for SET command in documentation section
        assert!(
            html.contains("SET Command") || html.contains("set mykey"),
            "Documentation section should contain SET command example"
        );
        assert!(
            html.contains("STORED"),
            "Documentation section should contain STORED response"
        );
    }

    #[test]
    fn test_documentation_section_contains_get_command_example() {
        let html = get_homepage_html();
        // Check for GET command in documentation section
        assert!(
            html.contains("GET Command") || html.contains("get mykey"),
            "Documentation section should contain GET command example"
        );
        assert!(
            html.contains("VALUE mykey"),
            "Documentation section should contain VALUE response"
        );
    }

    #[test]
    fn test_documentation_section_contains_delete_command_example() {
        let html = get_homepage_html();
        // Check for DELETE command in documentation section
        assert!(
            html.contains("DELETE Command") || html.contains("delete mykey"),
            "Documentation section should contain DELETE command example"
        );
        assert!(
            html.contains("DELETED"),
            "Documentation section should contain DELETED response"
        );
    }

    #[test]
    fn test_documentation_section_contains_server_start_command() {
        let html = get_homepage_html();
        assert!(
            html.contains("mirdb-server"),
            "Documentation section should contain server start command"
        );
        assert!(
            html.contains("--port") || html.contains("--http-port") || html.contains("http_port"),
            "Documentation section should contain port configuration"
        );
    }

    #[test]
    fn test_documentation_section_has_installation_subsection() {
        let html = get_homepage_html();
        assert!(
            html.contains("id=\"installation\""),
            "Documentation section should have installation subsection"
        );
        assert!(
            html.contains("Installation"),
            "Documentation section should have Installation heading"
        );
    }

    #[test]
    fn test_documentation_section_has_configuration_subsection() {
        let html = get_homepage_html();
        assert!(
            html.contains("id=\"configuration\""),
            "Documentation section should have configuration subsection"
        );
        assert!(
            html.contains("Configuration"),
            "Documentation section should have Configuration heading"
        );
    }

    #[test]
    fn test_documentation_section_has_commands_subsection() {
        let html = get_homepage_html();
        assert!(
            html.contains("id=\"commands\""),
            "Documentation section should have commands subsection"
        );
        assert!(
            html.contains("Command Examples"),
            "Documentation section should have Command Examples heading"
        );
    }

    #[test]
    fn test_documentation_contains_clone_instructions() {
        let html = get_homepage_html();
        assert!(
            html.contains("git clone"),
            "Documentation section should contain git clone command"
        );
    }

    #[test]
    fn test_documentation_config_contains_listen_address() {
        let html = get_homepage_html();
        assert!(
            html.contains("listen_address"),
            "Documentation configuration section should contain listen_address setting"
        );
    }
}
