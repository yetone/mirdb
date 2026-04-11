//! Static Asset Serving
//! Owner: Scenario 19 - Static Asset Serving
//! Co-owner: Scenario 14 - Performance Page Load
//!
//! Expected exports:
//! - serve_static(path: &str) -> Response
//! - Proper MIME types and caching headers
//! - rust-embed integration for binary embedding

use std::collections::HashMap;

/// Get MIME type for file extension
pub fn get_mime_type(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "json" => "application/json",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

/// Static assets embedded in binary
/// This is a placeholder - in production, use rust-embed
pub struct StaticAssets {
    files: HashMap<String, &'static [u8]>,
}

impl StaticAssets {
    /// Create new static assets store
    pub fn new() -> Self {
        StaticAssets {
            files: HashMap::new(),
        }
    }

    /// Get file content by path
    pub fn get(&self, path: &str) -> Option<&[u8]> {
        self.files.get(path).copied()
    }
}

impl Default for StaticAssets {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_type_html() {
        assert_eq!(get_mime_type("index.html"), "text/html; charset=utf-8");
    }

    #[test]
    fn test_mime_type_css() {
        assert_eq!(get_mime_type("styles/main.css"), "text/css; charset=utf-8");
    }

    #[test]
    fn test_mime_type_js() {
        assert_eq!(
            get_mime_type("scripts/main.js"),
            "application/javascript; charset=utf-8"
        );
    }

    #[test]
    fn test_mime_type_unknown() {
        assert_eq!(get_mime_type("file.xyz"), "application/octet-stream");
    }
}
