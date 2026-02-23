//! Static asset embedding and serving.
//! Owner: Scenario 3 - Static Asset Serving
//!
//! This module embeds static assets (HTML, CSS, JS, images) directly
//! into the binary using include_str! and include_bytes! macros.

/// Embedded homepage HTML
pub const HOMEPAGE_HTML: &str = include_str!("../../assets/index.html");

/// Embedded stylesheet
pub const STYLES_CSS: &str = include_str!("../../assets/styles.css");

/// Embedded console JavaScript
pub const CONSOLE_JS: &str = include_str!("../../assets/console.js");

/// Embedded logo image
pub const LOGO_GIF: &[u8] = include_bytes!("../../../assets/logo.gif");

/// Get the content type for a given file path
pub fn get_content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("gif") => "image/gif",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json; charset=utf-8",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_content_type_html() {
        assert_eq!(get_content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(get_content_type("page.htm"), "text/html; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_css() {
        assert_eq!(get_content_type("styles.css"), "text/css; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_js() {
        assert_eq!(get_content_type("app.js"), "application/javascript; charset=utf-8");
    }

    #[test]
    fn test_get_content_type_images() {
        assert_eq!(get_content_type("logo.gif"), "image/gif");
        assert_eq!(get_content_type("photo.png"), "image/png");
        assert_eq!(get_content_type("image.jpg"), "image/jpeg");
    }

    #[test]
    fn test_get_content_type_unknown() {
        assert_eq!(get_content_type("file.xyz"), "application/octet-stream");
    }
}
