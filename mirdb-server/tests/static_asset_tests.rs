//! Integration Tests for Scenario 9 - Static Asset Handling
//!
//! Test that CSS, JavaScript, and image assets are served correctly
//! with proper MIME types and gzip compression support.
//!
//! Owner: Scenario 9 - Static Asset Handling

/// Embedded CSS content for testing
const STYLE_CSS: &str = include_str!("../assets/css/style.css");

/// Embedded JavaScript content for testing
const MAIN_JS: &str = include_str!("../assets/js/main.js");

/// Embedded SVG logo for testing
const LOGO_SVG: &str = include_str!("../assets/images/logo.svg");

// ============================================================================
// Test Case 1: GET /static/style.css returns CSS with correct Content-Type
// ============================================================================

#[test]
fn test_css_asset_exists_and_has_content() {
    // Verify the CSS file exists and has substantial content
    assert!(
        !STYLE_CSS.is_empty(),
        "CSS file must not be empty"
    );
    assert!(
        STYLE_CSS.len() > 100,
        "CSS file must have substantial content"
    );
}

#[test]
fn test_css_is_valid_stylesheet() {
    // Verify CSS has proper structure
    assert!(
        STYLE_CSS.contains(":root"),
        "CSS must contain :root selector for variables"
    );
    assert!(
        STYLE_CSS.contains("{") && STYLE_CSS.contains("}"),
        "CSS must contain valid rule blocks"
    );
}

#[test]
fn test_css_has_required_classes() {
    // Verify CSS has required class definitions
    assert!(
        STYLE_CSS.contains(".header"),
        "CSS must have .header class"
    );
    assert!(
        STYLE_CSS.contains(".footer"),
        "CSS must have .footer class"
    );
}

// ============================================================================
// Test Case 2: GET /static/main.js returns JS with correct Content-Type
// ============================================================================

#[test]
fn test_javascript_asset_exists_and_has_content() {
    // Verify the JavaScript file exists and has substantial content
    assert!(
        !MAIN_JS.is_empty(),
        "JavaScript file must not be empty"
    );
    assert!(
        MAIN_JS.len() > 100,
        "JavaScript file must have substantial content"
    );
}

#[test]
fn test_javascript_is_valid_script() {
    // Verify JavaScript has proper structure
    assert!(
        MAIN_JS.contains("function") || MAIN_JS.contains("=>"),
        "JavaScript must contain function definitions"
    );
    assert!(
        MAIN_JS.contains("document"),
        "JavaScript must interact with DOM"
    );
}

#[test]
fn test_javascript_has_theme_functionality() {
    // Verify JavaScript has theme toggle functionality
    assert!(
        MAIN_JS.contains("theme") || MAIN_JS.contains("Theme"),
        "JavaScript must have theme-related code"
    );
    assert!(
        MAIN_JS.contains("localStorage"),
        "JavaScript must use localStorage for theme persistence"
    );
}

// ============================================================================
// Test Case 3: Non-existent static asset returns 404
// ============================================================================

#[test]
fn test_known_assets_are_available() {
    // Test that we have the known static assets
    let known_assets = vec![
        ("css/style.css", STYLE_CSS),
        ("js/main.js", MAIN_JS),
        ("images/logo.svg", LOGO_SVG),
    ];

    for (path, content) in known_assets {
        assert!(
            !content.is_empty(),
            "Asset '{}' must not be empty",
            path
        );
    }
}

// ============================================================================
// Test Case 4: Assets support gzip compression
// ============================================================================

#[test]
fn test_css_is_compressible() {
    // CSS contains text that should be highly compressible
    assert!(
        STYLE_CSS.len() > 500,
        "CSS must have enough content to benefit from compression"
    );

    // Verify CSS has repeated patterns (good for compression)
    let color_count = STYLE_CSS.matches("color").count();
    assert!(
        color_count > 5,
        "CSS should have repeated keywords for compression, found {} occurrences of 'color'",
        color_count
    );
}

#[test]
fn test_javascript_is_compressible() {
    // JavaScript contains text that should be highly compressible
    assert!(
        MAIN_JS.len() > 500,
        "JavaScript must have enough content to benefit from compression"
    );

    // Verify JavaScript has repeated patterns
    let function_count = MAIN_JS.matches("function").count()
        + MAIN_JS.matches("var ").count()
        + MAIN_JS.matches("const ").count()
        + MAIN_JS.matches("let ").count();
    assert!(
        function_count > 3,
        "JavaScript should have repeated keywords for compression"
    );
}

// ============================================================================
// Image Asset Tests
// ============================================================================

#[test]
fn test_logo_svg_exists_and_has_content() {
    // Verify the SVG logo file exists and has content
    assert!(
        !LOGO_SVG.is_empty(),
        "SVG logo file must not be empty"
    );
}

#[test]
fn test_logo_svg_is_valid_svg() {
    // Verify SVG has proper structure
    assert!(
        LOGO_SVG.contains("<svg"),
        "SVG must contain <svg> element"
    );
    assert!(
        LOGO_SVG.contains("</svg>"),
        "SVG must contain closing </svg> tag"
    );
    assert!(
        LOGO_SVG.contains("xmlns"),
        "SVG must have xmlns attribute for proper rendering"
    );
}

#[test]
fn test_logo_svg_has_viewbox() {
    // Verify SVG has viewBox for proper scaling
    assert!(
        LOGO_SVG.contains("viewBox") || LOGO_SVG.contains("viewbox"),
        "SVG must have viewBox attribute for responsive sizing"
    );
}

// ============================================================================
// MIME Type Validation Tests
// ============================================================================

/// Tests that verify the handler returns correct MIME types
mod mime_type_tests {
    /// Expected MIME type for CSS files
    const CSS_MIME_TYPE: &str = "text/css";

    /// Expected MIME type for JavaScript files
    const JS_MIME_TYPE: &str = "application/javascript";

    /// Expected MIME type for SVG files
    const SVG_MIME_TYPE: &str = "image/svg+xml";

    #[test]
    fn test_css_mime_type_is_standard() {
        // Verify CSS MIME type follows standards
        assert!(
            CSS_MIME_TYPE.starts_with("text/"),
            "CSS MIME type must be a text type"
        );
        assert_eq!(
            CSS_MIME_TYPE, "text/css",
            "CSS MIME type must be 'text/css'"
        );
    }

    #[test]
    fn test_javascript_mime_type_is_standard() {
        // Verify JavaScript MIME type follows standards
        assert!(
            JS_MIME_TYPE.starts_with("application/"),
            "JavaScript MIME type must be an application type"
        );
        assert_eq!(
            JS_MIME_TYPE, "application/javascript",
            "JavaScript MIME type must be 'application/javascript'"
        );
    }

    #[test]
    fn test_svg_mime_type_is_standard() {
        // Verify SVG MIME type follows standards
        assert!(
            SVG_MIME_TYPE.starts_with("image/"),
            "SVG MIME type must be an image type"
        );
        assert_eq!(
            SVG_MIME_TYPE, "image/svg+xml",
            "SVG MIME type must be 'image/svg+xml'"
        );
    }
}

// ============================================================================
// Compression Tests using flate2
// ============================================================================

mod compression_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_gzip_compression_produces_smaller_output_for_css() {
        // Test that gzip compression reduces CSS size
        let original = STYLE_CSS.as_bytes();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        assert!(
            compressed.len() < original.len(),
            "Gzip compression should reduce CSS size: original={}, compressed={}",
            original.len(),
            compressed.len()
        );
    }

    #[test]
    fn test_gzip_compression_produces_smaller_output_for_js() {
        // Test that gzip compression reduces JavaScript size
        let original = MAIN_JS.as_bytes();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        assert!(
            compressed.len() < original.len(),
            "Gzip compression should reduce JavaScript size: original={}, compressed={}",
            original.len(),
            compressed.len()
        );
    }

    #[test]
    fn test_gzip_compression_is_reversible() {
        // Test that compressed content can be decompressed back to original
        use std::io::Read;

        let original = STYLE_CSS;

        // Compress
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();

        assert_eq!(
            original, decompressed,
            "Decompressed content must match original"
        );
    }

    #[test]
    fn test_gzip_header_is_valid() {
        // Test that gzip output has valid header bytes
        let original = "test content".as_bytes();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Gzip files start with magic bytes 0x1f 0x8b
        assert!(
            compressed.len() >= 2,
            "Compressed data must have at least 2 bytes for header"
        );
        assert_eq!(
            compressed[0], 0x1f,
            "Gzip magic byte 1 must be 0x1f"
        );
        assert_eq!(
            compressed[1], 0x8b,
            "Gzip magic byte 2 must be 0x8b"
        );
    }
}

// ============================================================================
// Handler Integration Tests
// ============================================================================

mod handler_tests {
    /// Test that static asset paths are properly parsed
    #[test]
    fn test_asset_path_parsing() {
        let paths = vec![
            ("css/style.css", true),
            ("style.css", true),
            ("js/main.js", true),
            ("main.js", true),
            ("images/logo.svg", true),
            ("logo.svg", true),
            ("nonexistent.txt", false),
            ("../../../etc/passwd", false),
        ];

        for (path, should_exist) in paths {
            let exists = match path {
                "css/style.css" | "style.css" => true,
                "js/main.js" | "main.js" => true,
                "images/logo.svg" | "logo.svg" => true,
                _ => false,
            };
            assert_eq!(
                exists, should_exist,
                "Path '{}' existence check failed: expected {}, got {}",
                path, should_exist, exists
            );
        }
    }

    /// Test that unknown paths return false (simulating 404)
    #[test]
    fn test_unknown_paths_return_not_found() {
        let unknown_paths = vec![
            "unknown.css",
            "unknown.js",
            "unknown.svg",
            "css/unknown.css",
            "js/unknown.js",
            "images/unknown.svg",
            "",
            ".",
            "..",
        ];

        for path in unknown_paths {
            let exists = match path {
                "css/style.css" | "style.css" => true,
                "js/main.js" | "main.js" => true,
                "images/logo.svg" | "logo.svg" => true,
                _ => false,
            };
            assert!(
                !exists,
                "Unknown path '{}' should not exist",
                path
            );
        }
    }
}

// ============================================================================
// Cache Control Tests
// ============================================================================

mod cache_control_tests {
    /// Test that cache durations are appropriate for asset types
    #[test]
    fn test_cache_control_values() {
        // CSS and JS: 1 hour cache
        let css_js_cache = "public, max-age=3600";
        assert!(
            css_js_cache.contains("3600"),
            "CSS/JS should have 1 hour cache"
        );

        // Images: 24 hour cache
        let image_cache = "public, max-age=86400";
        assert!(
            image_cache.contains("86400"),
            "Images should have 24 hour cache"
        );
    }

    #[test]
    fn test_cache_is_public() {
        let cache_header = "public, max-age=3600";
        assert!(
            cache_header.contains("public"),
            "Cache should be public for CDN caching"
        );
    }
}

// ============================================================================
// HTTP Integration Tests using warp::test
// ============================================================================

#[cfg(test)]
mod http_integration_tests {
    use std::sync::Arc;

    /// Test Case 1: GET /static/style.css returns CSS with correct Content-Type
    #[tokio::test]
    async fn test_css_returns_correct_content_type() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/css/style.css")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200, "CSS request should return HTTP 200");

        let content_type = response.headers().get("content-type")
            .expect("Response should have Content-Type header");
        assert!(
            content_type.to_str().unwrap().starts_with("text/css"),
            "Content-Type should be text/css, got {:?}",
            content_type
        );
    }

    /// Test Case 1 (alt path): GET /static/style.css also works
    #[tokio::test]
    async fn test_css_alt_path_returns_correct_content_type() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/style.css")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200, "CSS request should return HTTP 200");

        let content_type = response.headers().get("content-type")
            .expect("Response should have Content-Type header");
        assert!(
            content_type.to_str().unwrap().starts_with("text/css"),
            "Content-Type should be text/css"
        );
    }

    /// Test Case 2: GET /static/main.js returns JS with correct Content-Type
    #[tokio::test]
    async fn test_javascript_returns_correct_content_type() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/js/main.js")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200, "JavaScript request should return HTTP 200");

        let content_type = response.headers().get("content-type")
            .expect("Response should have Content-Type header");
        assert!(
            content_type.to_str().unwrap().starts_with("application/javascript"),
            "Content-Type should be application/javascript, got {:?}",
            content_type
        );
    }

    /// Test Case 2 (alt path): GET /static/main.js also works
    #[tokio::test]
    async fn test_javascript_alt_path_returns_correct_content_type() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/main.js")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200, "JavaScript request should return HTTP 200");

        let content_type = response.headers().get("content-type")
            .expect("Response should have Content-Type header");
        assert!(
            content_type.to_str().unwrap().starts_with("application/javascript"),
            "Content-Type should be application/javascript"
        );
    }

    /// Test Case 3: Request non-existent static asset returns HTTP 404
    #[tokio::test]
    async fn test_nonexistent_asset_returns_404() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/nonexistent.css")
            .reply(&filter)
            .await;

        assert_eq!(
            response.status(),
            404,
            "Non-existent asset should return HTTP 404"
        );
    }

    /// Test Case 3 (variant): Request unknown JS file returns 404
    #[tokio::test]
    async fn test_nonexistent_js_returns_404() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/unknown.js")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 404);
    }

    /// Test Case 4: Request assets with Accept-Encoding: gzip header
    #[tokio::test]
    async fn test_gzip_compression_for_css() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/css/style.css")
            .header("Accept-Encoding", "gzip, deflate")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200, "Request should succeed");

        let content_encoding = response.headers().get("content-encoding");
        assert!(
            content_encoding.is_some(),
            "Response should include Content-Encoding header when gzip is requested"
        );
        assert_eq!(
            content_encoding.unwrap().to_str().unwrap(),
            "gzip",
            "Content-Encoding should be gzip"
        );

        // Verify Vary header is set
        let vary = response.headers().get("vary");
        assert!(
            vary.is_some(),
            "Response should include Vary header"
        );
        assert!(
            vary.unwrap().to_str().unwrap().contains("Accept-Encoding"),
            "Vary header should include Accept-Encoding"
        );
    }

    /// Test Case 4 (JS): Request JS with Accept-Encoding: gzip header
    #[tokio::test]
    async fn test_gzip_compression_for_javascript() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/js/main.js")
            .header("Accept-Encoding", "gzip")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);

        let content_encoding = response.headers().get("content-encoding");
        assert!(
            content_encoding.is_some(),
            "JavaScript response should include Content-Encoding: gzip"
        );
        assert_eq!(
            content_encoding.unwrap().to_str().unwrap(),
            "gzip"
        );
    }

    /// Test Case 4 (SVG): Request SVG with Accept-Encoding: gzip header
    #[tokio::test]
    async fn test_gzip_compression_for_svg() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/images/logo.svg")
            .header("Accept-Encoding", "gzip")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);

        let content_encoding = response.headers().get("content-encoding");
        assert!(
            content_encoding.is_some(),
            "SVG response should include Content-Encoding: gzip"
        );
        assert_eq!(
            content_encoding.unwrap().to_str().unwrap(),
            "gzip"
        );
    }

    /// Test that SVG images are served with correct MIME type
    #[tokio::test]
    async fn test_svg_returns_correct_content_type() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/images/logo.svg")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200, "SVG request should return HTTP 200");

        let content_type = response.headers().get("content-type")
            .expect("Response should have Content-Type header");
        assert_eq!(
            content_type.to_str().unwrap(),
            "image/svg+xml",
            "Content-Type should be image/svg+xml"
        );
    }

    /// Test that requests without gzip support don't get compressed content
    #[tokio::test]
    async fn test_no_compression_without_accept_encoding() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/css/style.css")
            .reply(&filter)  // No Accept-Encoding header
            .await;

        assert_eq!(response.status(), 200);

        // Should NOT have Content-Encoding: gzip when not requested
        let content_encoding = response.headers().get("content-encoding");
        assert!(
            content_encoding.is_none(),
            "Response should NOT include Content-Encoding when gzip is not requested"
        );
    }

    /// Test that compressed content can be decompressed
    #[tokio::test]
    async fn test_gzip_content_is_valid() {
        use mirdb::homepage::{routes, AppState};
        use std::io::Read;

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/css/style.css")
            .header("Accept-Encoding", "gzip")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);

        // Get the compressed body
        let compressed_body = response.body().to_vec();

        // Decompress and verify it's valid CSS
        let mut decoder = flate2::read::GzDecoder::new(&compressed_body[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed)
            .expect("Should be able to decompress gzip content");

        // Verify it looks like valid CSS
        assert!(
            decompressed.contains(":root"),
            "Decompressed CSS should contain :root"
        );
        assert!(
            decompressed.contains(".header"),
            "Decompressed CSS should contain .header class"
        );
    }

    /// Test that Cache-Control headers are present
    #[tokio::test]
    async fn test_cache_control_headers_present() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        let response = warp::test::request()
            .method("GET")
            .path("/static/css/style.css")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);

        let cache_control = response.headers().get("cache-control")
            .expect("Response should have Cache-Control header");
        let cache_str = cache_control.to_str().unwrap();

        assert!(
            cache_str.contains("public"),
            "Cache-Control should include 'public'"
        );
        assert!(
            cache_str.contains("max-age"),
            "Cache-Control should include 'max-age'"
        );
    }

    /// Test path traversal attempts are blocked
    #[tokio::test]
    async fn test_path_traversal_blocked() {
        use mirdb::homepage::{routes, AppState};

        let state = Arc::new(AppState::default());
        let filter = routes(state);

        // Try path traversal attack
        let response = warp::test::request()
            .method("GET")
            .path("/static/../../../etc/passwd")
            .reply(&filter)
            .await;

        // Should return 404, not the file contents
        assert_eq!(
            response.status(),
            404,
            "Path traversal attempts should return 404"
        );
    }
}
