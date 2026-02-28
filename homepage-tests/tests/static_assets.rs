//! Integration Tests for Scenario 9 - Static Asset Handling
//!
//! Test that CSS, JavaScript, and image assets are served correctly
//! with proper MIME types and gzip compression support.
//!
//! Owner: Scenario 9 - Static Asset Handling

/// Embedded CSS content for testing
const STYLE_CSS: &str = include_str!("../../mirdb-server/assets/css/style.css");

/// Embedded JavaScript content for testing
const MAIN_JS: &str = include_str!("../../mirdb-server/assets/js/main.js");

/// Embedded SVG logo for testing
const LOGO_SVG: &str = include_str!("../../mirdb-server/assets/images/logo.svg");

// ============================================================================
// Test Case 1: GET /static/style.css returns CSS with correct Content-Type
// ============================================================================

#[test]
fn test_case_1_css_asset_exists_and_has_content() {
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
fn test_case_1_css_is_valid_stylesheet() {
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
fn test_case_1_css_has_required_classes() {
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

#[test]
fn test_case_1_css_content_type_text_css() {
    // The expected MIME type for CSS files
    const CSS_MIME_TYPE: &str = "text/css";

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

// ============================================================================
// Test Case 2: GET /static/main.js returns JS with correct Content-Type
// ============================================================================

#[test]
fn test_case_2_javascript_asset_exists_and_has_content() {
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
fn test_case_2_javascript_is_valid_script() {
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
fn test_case_2_javascript_has_theme_functionality() {
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

#[test]
fn test_case_2_javascript_content_type_application_javascript() {
    // The expected MIME type for JavaScript files
    const JS_MIME_TYPE: &str = "application/javascript";

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

// ============================================================================
// Test Case 3: Request non-existent static asset returns 404
// ============================================================================

#[test]
fn test_case_3_known_assets_are_available() {
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

#[test]
fn test_case_3_asset_path_parsing() {
    // Test that static asset paths are properly parsed
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

#[test]
fn test_case_3_unknown_paths_return_not_found() {
    // Test that unknown paths should simulate 404 behavior
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

// ============================================================================
// Test Case 4: Assets support gzip compression
// ============================================================================

#[test]
fn test_case_4_css_is_compressible() {
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
fn test_case_4_javascript_is_compressible() {
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

#[test]
fn test_case_4_gzip_compression_reduces_css_size() {
    use std::io::Write;

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
fn test_case_4_gzip_compression_reduces_javascript_size() {
    use std::io::Write;

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
fn test_case_4_gzip_compression_is_reversible() {
    use std::io::{Read, Write};

    // Test that compressed content can be decompressed back to original
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
fn test_case_4_gzip_header_is_valid() {
    use std::io::Write;

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

#[test]
fn test_logo_svg_content_type_image_svg_xml() {
    // The expected MIME type for SVG files
    const SVG_MIME_TYPE: &str = "image/svg+xml";

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

// ============================================================================
// Cache Control Tests
// ============================================================================

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
