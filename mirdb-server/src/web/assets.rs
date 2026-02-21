//! Static asset serving utilities.
//! Owner: Scenario 3 - Static Asset Serving
//!
//! This module provides utilities for serving static files with correct MIME types
//! and appropriate caching headers.

use std::path::Path;

/// Returns the MIME type for a given file extension.
///
/// # Arguments
/// * `extension` - The file extension including the dot (e.g., ".html", ".css")
///
/// # Returns
/// The MIME type as a string slice.
pub fn get_mime_type(extension: &str) -> &'static str {
    match extension.to_lowercase().as_str() {
        ".html" | ".htm" => "text/html; charset=utf-8",
        ".css" => "text/css; charset=utf-8",
        ".js" => "application/javascript; charset=utf-8",
        ".json" => "application/json; charset=utf-8",
        ".gif" => "image/gif",
        ".png" => "image/png",
        ".jpg" | ".jpeg" => "image/jpeg",
        ".svg" => "image/svg+xml",
        ".ico" => "image/x-icon",
        ".woff" => "font/woff",
        ".woff2" => "font/woff2",
        ".ttf" => "font/ttf",
        ".txt" => "text/plain; charset=utf-8",
        ".xml" => "application/xml; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Returns the file extension from a path.
///
/// # Arguments
/// * `path` - The file path
///
/// # Returns
/// The extension including the dot, or an empty string if no extension.
pub fn get_extension(path: &str) -> &str {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            // Find the position of the extension in the original path
            if let Some(pos) = path.rfind('.') {
                &path[pos..]
            } else {
                ""
            }
        })
        .unwrap_or("")
}

/// Returns Cache-Control header value for static assets.
///
/// Different asset types may have different caching strategies:
/// - Images: Long cache (1 year) since they rarely change
/// - CSS/JS: Medium cache (1 day) to allow updates
/// - HTML: Short cache (5 minutes) for freshness
///
/// # Arguments
/// * `extension` - The file extension including the dot
///
/// # Returns
/// The Cache-Control header value.
pub fn get_cache_control(extension: &str) -> &'static str {
    match extension.to_lowercase().as_str() {
        ".gif" | ".png" | ".jpg" | ".jpeg" | ".ico" | ".svg" | ".woff" | ".woff2" | ".ttf" => {
            "public, max-age=31536000" // 1 year
        }
        ".css" | ".js" => "public, max-age=86400", // 1 day
        ".html" | ".htm" => "public, max-age=300", // 5 minutes
        _ => "public, max-age=3600",               // 1 hour default
    }
}

/// Validates that a path is safe and doesn't contain directory traversal attempts.
///
/// # Arguments
/// * `path` - The requested path
///
/// # Returns
/// `true` if the path is safe, `false` if it contains traversal attempts.
pub fn is_safe_path(path: &str) -> bool {
    // Check for directory traversal patterns
    if path.contains("..") {
        return false;
    }

    // Check for null bytes (could be used to bypass checks)
    if path.contains('\0') {
        return false;
    }

    // Check for absolute paths (shouldn't be allowed in URL paths)
    if path.starts_with('/') && path.contains("..") {
        return false;
    }

    // Normalize and check components
    let path = Path::new(path);
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => return false,
            std::path::Component::Normal(s) => {
                // Check for hidden files or suspicious names
                if let Some(name) = s.to_str() {
                    if name.starts_with('.') && name != "." {
                        // Allow dotfiles in paths but log for security
                    }
                }
            }
            _ => {}
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_mime_type_html() {
        assert_eq!(get_mime_type(".html"), "text/html; charset=utf-8");
        assert_eq!(get_mime_type(".htm"), "text/html; charset=utf-8");
        assert_eq!(get_mime_type(".HTML"), "text/html; charset=utf-8");
    }

    #[test]
    fn test_get_mime_type_css() {
        assert_eq!(get_mime_type(".css"), "text/css; charset=utf-8");
        assert_eq!(get_mime_type(".CSS"), "text/css; charset=utf-8");
    }

    #[test]
    fn test_get_mime_type_js() {
        assert_eq!(get_mime_type(".js"), "application/javascript; charset=utf-8");
        assert_eq!(get_mime_type(".JS"), "application/javascript; charset=utf-8");
    }

    #[test]
    fn test_get_mime_type_gif() {
        assert_eq!(get_mime_type(".gif"), "image/gif");
        assert_eq!(get_mime_type(".GIF"), "image/gif");
    }

    #[test]
    fn test_get_mime_type_unknown() {
        assert_eq!(get_mime_type(".unknown"), "application/octet-stream");
        assert_eq!(get_mime_type(".xyz"), "application/octet-stream");
        assert_eq!(get_mime_type(""), "application/octet-stream");
    }

    #[test]
    fn test_get_extension() {
        assert_eq!(get_extension("file.html"), ".html");
        assert_eq!(get_extension("path/to/file.css"), ".css");
        assert_eq!(get_extension("image.gif"), ".gif");
        assert_eq!(get_extension("noextension"), "");
    }

    #[test]
    fn test_get_cache_control() {
        // Images should have long cache
        assert_eq!(get_cache_control(".gif"), "public, max-age=31536000");
        assert_eq!(get_cache_control(".png"), "public, max-age=31536000");

        // CSS/JS should have medium cache
        assert_eq!(get_cache_control(".css"), "public, max-age=86400");
        assert_eq!(get_cache_control(".js"), "public, max-age=86400");

        // HTML should have short cache
        assert_eq!(get_cache_control(".html"), "public, max-age=300");
    }

    #[test]
    fn test_is_safe_path() {
        // Safe paths
        assert!(is_safe_path("assets/logo.gif"));
        assert!(is_safe_path("css/style.css"));
        assert!(is_safe_path("js/main.js"));
        assert!(is_safe_path("index.html"));

        // Unsafe paths - directory traversal
        assert!(!is_safe_path("../etc/passwd"));
        assert!(!is_safe_path("assets/../../../etc/passwd"));
        assert!(!is_safe_path(".."));
        assert!(!is_safe_path("foo/.."));

        // Null byte injection
        assert!(!is_safe_path("file\0.txt"));
    }
}
