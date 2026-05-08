/**
 * Static asset management for the MirDB homepage.
 * Owner: Shared - First Builder
 *
 * Embeds CSS and image files into the binary at compile time.
 *
 * Expected exports:
 * - styles_css() -> &'static str: Returns the embedded CSS content
 * - logo_data() -> &'static [u8]: Returns the embedded logo image bytes
 * - content_type(path) -> &'static str: Maps file extensions to MIME types
 */

/// Returns the embedded CSS content
pub fn styles_css() -> &'static str {
    include_str!("../../static/styles.css")
}

/// Returns the embedded logo image bytes
pub fn logo_data() -> &'static [u8] {
    include_bytes!("../../../assets/logo.gif")
}

/// Maps file extensions to MIME types
pub fn content_type(path: &str) -> &'static str {
    if path.ends_with(".css") {
        "text/css"
    } else if path.ends_with(".html") || path.ends_with(".htm") {
        "text/html"
    } else if path.ends_with(".js") {
        "application/javascript"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        "image/jpeg"
    } else if path.ends_with(".gif") {
        "image/gif"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else {
        "application/octet-stream"
    }
}
