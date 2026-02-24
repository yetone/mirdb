//! Embedded static assets using include_str! macro.
//!
//! Owner: Scenario 5 - Asset Embedding
//!
//! This module embeds HTML, CSS, and JS assets at compile time
//! using include_str!. This ensures the binary is self-contained
//! with no external file dependencies at runtime.
//!
//! Assets are sourced from mirdb-server/src/web/ directory.

/// The main index.html page content, embedded at compile time.
pub const INDEX_HTML: &str = include_str!("../web/index.html");

/// The styles.css stylesheet content, embedded at compile time.
pub const STYLES_CSS: &str = include_str!("../web/styles.css");

/// The script.js JavaScript content, embedded at compile time.
pub const SCRIPT_JS: &str = include_str!("../web/script.js");

/// Content types for HTTP responses.
pub const CONTENT_TYPE_HTML: &str = "text/html; charset=utf-8";
pub const CONTENT_TYPE_CSS: &str = "text/css; charset=utf-8";
pub const CONTENT_TYPE_JS: &str = "application/javascript; charset=utf-8";

/// Get the asset content and content-type for a given path.
///
/// This function maps URL paths to embedded assets and their corresponding
/// MIME types. It supports the following routes:
/// - `/` or `/index.html` -> HTML content
/// - `/styles.css` -> CSS content
/// - `/script.js` -> JavaScript content
///
/// # Arguments
///
/// * `path` - The URL path to look up (e.g., "/", "/styles.css", "/script.js")
///
/// # Returns
///
/// * `Some((content, content_type))` - The asset content and its MIME type
/// * `None` - If the path doesn't match any known asset
///
/// # Examples
///
/// ```
/// use mirdb::http::assets::get_asset;
///
/// // Root path returns index.html
/// let (content, content_type) = get_asset("/").unwrap();
/// assert!(content.contains("<!DOCTYPE html>"));
/// assert_eq!(content_type, "text/html; charset=utf-8");
///
/// // CSS path returns stylesheet
/// let (content, content_type) = get_asset("/styles.css").unwrap();
/// assert_eq!(content_type, "text/css; charset=utf-8");
///
/// // JS path returns script
/// let (content, content_type) = get_asset("/script.js").unwrap();
/// assert_eq!(content_type, "application/javascript; charset=utf-8");
///
/// // Unknown paths return None
/// assert!(get_asset("/unknown").is_none());
/// ```
pub fn get_asset(path: &str) -> Option<(&'static str, &'static str)> {
    match path {
        "/" | "/index.html" => Some((INDEX_HTML, CONTENT_TYPE_HTML)),
        "/styles.css" => Some((STYLES_CSS, CONTENT_TYPE_CSS)),
        "/script.js" => Some((SCRIPT_JS, CONTENT_TYPE_JS)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_html_is_embedded() {
        assert!(!INDEX_HTML.is_empty(), "INDEX_HTML should not be empty");
        assert!(
            INDEX_HTML.contains("<!DOCTYPE html>"),
            "INDEX_HTML should contain DOCTYPE"
        );
        assert!(
            INDEX_HTML.contains("MirDB"),
            "INDEX_HTML should contain MirDB branding"
        );
    }

    #[test]
    fn test_styles_css_is_embedded() {
        assert!(!STYLES_CSS.is_empty(), "STYLES_CSS should not be empty");
        assert!(
            STYLES_CSS.contains(":root"),
            "STYLES_CSS should contain CSS root variables"
        );
        assert!(
            STYLES_CSS.contains("--bg-primary"),
            "STYLES_CSS should contain theme variables"
        );
    }

    #[test]
    fn test_script_js_is_embedded() {
        assert!(!SCRIPT_JS.is_empty(), "SCRIPT_JS should not be empty");
        assert!(
            SCRIPT_JS.contains("toggleTheme"),
            "SCRIPT_JS should contain toggleTheme function"
        );
    }

    #[test]
    fn test_get_asset_root_path() {
        let result = get_asset("/");
        assert!(result.is_some(), "Root path should return asset");
        let (content, content_type) = result.unwrap();
        assert!(content.contains("<!DOCTYPE html>"));
        assert_eq!(content_type, CONTENT_TYPE_HTML);
    }

    #[test]
    fn test_get_asset_index_html_path() {
        let result = get_asset("/index.html");
        assert!(result.is_some(), "/index.html should return asset");
        let (content, content_type) = result.unwrap();
        assert!(content.contains("<!DOCTYPE html>"));
        assert_eq!(content_type, CONTENT_TYPE_HTML);
    }

    #[test]
    fn test_get_asset_styles_css() {
        let result = get_asset("/styles.css");
        assert!(result.is_some(), "/styles.css should return asset");
        let (content, content_type) = result.unwrap();
        assert!(content.contains(":root"));
        assert_eq!(content_type, CONTENT_TYPE_CSS);
    }

    #[test]
    fn test_get_asset_script_js() {
        let result = get_asset("/script.js");
        assert!(result.is_some(), "/script.js should return asset");
        let (content, content_type) = result.unwrap();
        assert!(content.contains("toggleTheme"));
        assert_eq!(content_type, CONTENT_TYPE_JS);
    }

    #[test]
    fn test_get_asset_unknown_path_returns_none() {
        assert!(get_asset("/unknown").is_none());
        assert!(get_asset("/nonexistent.html").is_none());
        assert!(get_asset("/favicon.ico").is_none());
        assert!(get_asset("").is_none());
        assert!(get_asset("/styles").is_none());
        assert!(get_asset("/script").is_none());
        assert!(get_asset("/some/nested/path").is_none());
    }

    #[test]
    fn test_content_types_are_correct() {
        assert_eq!(CONTENT_TYPE_HTML, "text/html; charset=utf-8");
        assert_eq!(CONTENT_TYPE_CSS, "text/css; charset=utf-8");
        assert_eq!(CONTENT_TYPE_JS, "application/javascript; charset=utf-8");
    }

    #[test]
    fn test_assets_are_static_str() {
        // Verify all assets are 'static lifetime (compile-time embedded)
        let html: &'static str = INDEX_HTML;
        let css: &'static str = STYLES_CSS;
        let js: &'static str = SCRIPT_JS;

        // If this compiles, the assets are correctly embedded at compile time
        assert!(!html.is_empty());
        assert!(!css.is_empty());
        assert!(!js.is_empty());
    }

    #[test]
    fn test_assets_are_valid_utf8() {
        // Since we use include_str!, UTF-8 validity is guaranteed at compile time.
        // This test verifies that the content is non-empty and reasonable.
        assert!(!INDEX_HTML.is_empty());
        assert!(!STYLES_CSS.is_empty());
        assert!(!SCRIPT_JS.is_empty());

        // Verify we can iterate over characters (valid UTF-8)
        assert!(INDEX_HTML.chars().count() > 0);
        assert!(STYLES_CSS.chars().count() > 0);
        assert!(SCRIPT_JS.chars().count() > 0);
    }

    #[test]
    fn test_html_contains_expected_structure() {
        assert!(INDEX_HTML.contains("<html"));
        assert!(INDEX_HTML.contains("<head>"));
        assert!(INDEX_HTML.contains("<body>"));
        assert!(INDEX_HTML.contains("</html>"));
    }

    #[test]
    fn test_html_references_css_and_js() {
        assert!(INDEX_HTML.contains("styles.css"));
        assert!(INDEX_HTML.contains("script.js"));
    }
}
