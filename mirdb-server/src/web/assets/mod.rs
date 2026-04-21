//! Embedded static assets.
//! Owner: Scenario 2 - Homepage Static Content Serving
//!
//! Uses rust_embed to embed:
//! - index.html: Homepage HTML
//! - style.css: Stylesheet with theme variables
//! - main.js: Client-side JavaScript

use rust_embed::Embed;

/// Embedded asset folder containing static files
#[derive(Embed)]
#[folder = "src/web/assets/static/"]
pub struct Asset;

/// Get an embedded asset by path
///
/// # Arguments
/// * `path` - The path to the asset (e.g., "index.html", "style.css")
///
/// # Returns
/// The asset data if found, None otherwise
pub fn get_asset(path: &str) -> Option<rust_embed::EmbeddedFile> {
    Asset::get(path)
}

/// Get the content type for a given file path based on extension
///
/// # Arguments
/// * `path` - The file path to determine content type for
///
/// # Returns
/// The MIME content type string
pub fn get_content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_content_type_html() {
        assert_eq!(get_content_type("index.html"), "text/html; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_css() {
        assert_eq!(get_content_type("style.css"), "text/css; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_js() {
        assert_eq!(
            get_content_type("main.js"),
            "application/javascript; charset=utf-8"
        );
    }

    #[test]
    fn test_get_content_type_unknown() {
        assert_eq!(get_content_type("file.xyz"), "application/octet-stream");
    }
}
