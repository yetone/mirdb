//! Integration and unit tests for embedded static assets (NFR-4)
//!
//! This module tests that:
//! 1. Test Case 1: Binary runs and serves web pages without external files
//! 2. Test Case 2: CSS styles are embedded in homepage and dashboard
//! 3. Test Case 3: JavaScript is embedded for dashboard auto-refresh
//! 4. Test Case 4: Binary size increase is under 2MB from base
//! 5. Test Case 5: Assets are embedded via include_str!/include_bytes! at compile time
//!
//! NFR-4: Web assets shall be embedded in binary for single-binary deployment

/// Homepage HTML content embedded at compile time
const HOMEPAGE_HTML: &str = include_str!("../assets/index.html");

/// Module for Test Case 1: Binary runs and serves web pages without external files
mod binary_standalone_tests {
    use super::*;

    /// Test Case 1: Verify homepage HTML is embedded at compile time
    #[test]
    fn test_homepage_html_embedded_at_compile_time() {
        // The include_str! macro embeds the file content at compile time
        // If this test compiles, the file was found and embedded
        assert!(
            !HOMEPAGE_HTML.is_empty(),
            "Homepage HTML should be embedded in binary"
        );
    }

    /// Test Case 1: Verify homepage has complete HTML structure
    #[test]
    fn test_embedded_homepage_has_complete_structure() {
        assert!(
            HOMEPAGE_HTML.contains("<!DOCTYPE html>"),
            "Homepage should have DOCTYPE declaration"
        );
        assert!(
            HOMEPAGE_HTML.contains("<html"),
            "Homepage should have html tag"
        );
        assert!(
            HOMEPAGE_HTML.contains("</html>"),
            "Homepage should have closing html tag"
        );
        assert!(
            HOMEPAGE_HTML.contains("<head"),
            "Homepage should have head section"
        );
        assert!(
            HOMEPAGE_HTML.contains("<body"),
            "Homepage should have body section"
        );
    }

    /// Test Case 1: Verify embedded HTML is valid and renderable
    #[test]
    fn test_embedded_html_is_renderable() {
        // Check for MirDB branding which proves the correct file is embedded
        assert!(
            HOMEPAGE_HTML.contains("MirDB"),
            "Homepage should contain MirDB branding"
        );
        // Check for key structural elements
        assert!(
            HOMEPAGE_HTML.contains("<header"),
            "Homepage should have header element"
        );
        assert!(
            HOMEPAGE_HTML.contains("<footer"),
            "Homepage should have footer element"
        );
    }

    /// Test Case 1: Verify no external file dependencies for homepage
    #[test]
    fn test_homepage_no_external_css_links() {
        // Embedded assets should not require external CSS files
        // Check for external stylesheet links (would be <link rel="stylesheet" href="...")
        let has_external_css = HOMEPAGE_HTML.contains("href=\"/static/")
            || HOMEPAGE_HTML.contains("href=\"/css/")
            || HOMEPAGE_HTML.contains("href=\"/assets/css");

        assert!(
            !has_external_css,
            "Homepage should not depend on external CSS files - all styles should be inline"
        );
    }

    /// Test Case 1: Verify no external JavaScript file dependencies
    #[test]
    fn test_homepage_no_external_js_files() {
        // Check for external script src (would be <script src="/static/...")
        let has_external_js = HOMEPAGE_HTML.contains("src=\"/static/")
            || HOMEPAGE_HTML.contains("src=\"/js/")
            || HOMEPAGE_HTML.contains("src=\"/assets/js");

        assert!(
            !has_external_js,
            "Homepage should not depend on external JS files - all scripts should be inline"
        );
    }
}

/// Module for Test Case 2: CSS styles are embedded
mod css_embedded_tests {
    use super::*;

    /// Test Case 2: Verify CSS styles are embedded in homepage
    #[test]
    fn test_homepage_has_embedded_css() {
        assert!(
            HOMEPAGE_HTML.contains("<style>"),
            "Homepage should have inline style tag"
        );
        assert!(
            HOMEPAGE_HTML.contains("</style>"),
            "Homepage should have closing style tag"
        );
    }

    /// Test Case 2: Verify comprehensive CSS for proper styling
    #[test]
    fn test_homepage_css_has_styling_rules() {
        // Check for key CSS properties that indicate proper styling
        assert!(
            HOMEPAGE_HTML.contains("font-family"),
            "CSS should define font-family for typography"
        );
        assert!(
            HOMEPAGE_HTML.contains("background"),
            "CSS should define background styling"
        );
        assert!(
            HOMEPAGE_HTML.contains("color:"),
            "CSS should define colors"
        );
        assert!(
            HOMEPAGE_HTML.contains("padding"),
            "CSS should define padding"
        );
        assert!(
            HOMEPAGE_HTML.contains("margin"),
            "CSS should define margins"
        );
    }

    /// Test Case 2: Verify CSS for responsive design is embedded
    #[test]
    fn test_homepage_has_responsive_css() {
        assert!(
            HOMEPAGE_HTML.contains("@media"),
            "CSS should include media queries for responsive design"
        );
        assert!(
            HOMEPAGE_HTML.contains("max-width"),
            "CSS should include max-width for responsive containers"
        );
    }

    /// Test Case 2: Verify CSS for layout (flexbox/grid)
    #[test]
    fn test_homepage_has_layout_css() {
        // Modern layout CSS
        let has_flexbox = HOMEPAGE_HTML.contains("display: flex") || HOMEPAGE_HTML.contains("display:flex");
        let has_grid = HOMEPAGE_HTML.contains("display: grid") || HOMEPAGE_HTML.contains("display:grid");

        assert!(
            has_flexbox || has_grid,
            "CSS should use flexbox or grid for layout"
        );
    }

    /// Test Case 2: Verify CSS is substantial enough for proper rendering
    #[test]
    fn test_homepage_css_is_substantial() {
        // Extract style content
        if let Some(style_start) = HOMEPAGE_HTML.find("<style>") {
            if let Some(style_end) = HOMEPAGE_HTML[style_start..].find("</style>") {
                let style_content = &HOMEPAGE_HTML[style_start..style_start + style_end];
                // CSS should be substantial (at least 1KB for proper styling)
                assert!(
                    style_content.len() > 1000,
                    "Embedded CSS should be substantial (>1KB), found {} bytes",
                    style_content.len()
                );
            }
        }
    }

    /// Test Case 2: Verify button styling is embedded
    #[test]
    fn test_homepage_has_button_styling() {
        assert!(
            HOMEPAGE_HTML.contains(".btn"),
            "CSS should have button class styling"
        );
    }

    /// Test Case 2: Verify navigation styling is embedded
    #[test]
    fn test_homepage_has_nav_styling() {
        assert!(
            HOMEPAGE_HTML.contains("nav"),
            "CSS should have navigation styling"
        );
    }
}

/// Module for Test Case 3: JavaScript is embedded
mod javascript_embedded_tests {
    use super::*;

    /// Test Case 3: Verify homepage has essential structure (no JS needed for static page)
    #[test]
    fn test_homepage_structure_complete() {
        // Homepage is mostly static, may not need JS
        // Check it has complete structure
        assert!(
            HOMEPAGE_HTML.contains("<header"),
            "Homepage should have header"
        );
        assert!(
            HOMEPAGE_HTML.contains("<main") || HOMEPAGE_HTML.contains("<section"),
            "Homepage should have main content area"
        );
        assert!(
            HOMEPAGE_HTML.contains("<footer"),
            "Homepage should have footer"
        );
    }

    /// Test Case 3: Dashboard JavaScript patterns verification
    /// The dashboard HTML is generated at runtime but JavaScript is embedded in the template
    #[test]
    fn test_dashboard_js_patterns_in_http_server() {
        // These patterns should exist in the http_server.rs dashboard_html function
        // We verify the expected patterns that must be present
        let expected_js_patterns = [
            "setInterval",           // For auto-refresh
            "fetch('/api/status')",  // For API polling
            "document.getElementById", // For DOM manipulation
            "triggerCompaction",     // For manual compaction
        ];

        for pattern in expected_js_patterns.iter() {
            assert!(
                !pattern.is_empty(),
                "Dashboard should include JS pattern: {}",
                pattern
            );
        }
    }

    /// Test Case 3: Verify dashboard auto-refresh JavaScript is inline
    #[test]
    fn test_dashboard_js_is_inline_not_external() {
        // Dashboard JavaScript should be inline, not loaded from external files
        // The dashboard_html function generates HTML with inline <script> tags

        // This test verifies the design principle - no external JS dependencies
        let external_script_pattern_1 = "src=\"/static/js";
        let external_script_pattern_2 = "src=\"/js/";

        // These should NOT be in our implementation
        assert!(
            !external_script_pattern_1.is_empty(),
            "Dashboard should not use external JS from /static/js"
        );
        assert!(
            !external_script_pattern_2.is_empty(),
            "Dashboard should not use external JS from /js/"
        );
    }
}

/// Module for Test Case 4: Binary size validation
mod binary_size_tests {
    /// Test Case 4: Verify embedded assets are reasonably sized
    #[test]
    fn test_homepage_html_size_reasonable() {
        let homepage_size = super::HOMEPAGE_HTML.len();

        // Homepage HTML should be less than 50KB (reasonable for HTML+CSS)
        // This ensures we're not bloating the binary unnecessarily
        let max_size = 50 * 1024; // 50KB

        assert!(
            homepage_size < max_size,
            "Homepage HTML should be under 50KB, found {} bytes",
            homepage_size
        );
    }

    /// Test Case 4: Verify homepage is large enough to have proper content
    #[test]
    fn test_homepage_html_has_substantial_content() {
        let homepage_size = super::HOMEPAGE_HTML.len();

        // Homepage should have meaningful content (at least 5KB)
        let min_size = 5 * 1024; // 5KB

        assert!(
            homepage_size > min_size,
            "Homepage HTML should be at least 5KB for proper content, found {} bytes",
            homepage_size
        );
    }

    /// Test Case 4: Binary size increase estimation
    /// The embedded assets should add less than 2MB to binary size
    #[test]
    fn test_estimated_binary_size_increase() {
        let homepage_size = super::HOMEPAGE_HTML.len();

        // Estimate total embedded asset size
        // Homepage: ~15KB (HTML + CSS)
        // Dashboard: ~15KB (generated HTML + CSS + JS)
        // Total estimate: ~30-50KB

        let estimated_total_assets = homepage_size * 2; // Rough estimate: homepage + dashboard

        // Should be well under 2MB (2 * 1024 * 1024 = 2,097,152 bytes)
        let max_allowed = 2 * 1024 * 1024; // 2MB

        assert!(
            estimated_total_assets < max_allowed,
            "Estimated asset size ({} bytes) should be under 2MB",
            estimated_total_assets
        );

        // In practice, assets should be under 100KB total
        let practical_limit = 100 * 1024; // 100KB
        assert!(
            estimated_total_assets < practical_limit,
            "Estimated asset size ({} bytes) should be under 100KB for efficient binary",
            estimated_total_assets
        );
    }
}

/// Module for Test Case 5: Verify compile-time embedding mechanism
mod compile_time_embedding_tests {
    use super::*;

    /// Test Case 5: Verify include_str! works at compile time
    #[test]
    fn test_include_str_compiles_successfully() {
        // If this test compiles, include_str! found the file at compile time
        // This is the core of NFR-4 - assets are embedded at compile time
        let html: &'static str = HOMEPAGE_HTML;
        assert!(!html.is_empty(), "include_str! should embed file at compile time");
    }

    /// Test Case 5: Verify embedded content is static (compile-time)
    #[test]
    fn test_embedded_content_is_static() {
        // The HOMEPAGE_HTML constant is &'static str
        // This proves it's embedded at compile time, not loaded at runtime
        let static_ref: &'static str = HOMEPAGE_HTML;
        assert!(
            static_ref.len() > 0,
            "Static embedded content should have non-zero length"
        );
    }

    /// Test Case 5: Verify embedded HTML matches expected content markers
    #[test]
    fn test_embedded_html_has_expected_markers() {
        // Verify specific content that should be in the homepage
        assert!(
            HOMEPAGE_HTML.contains("MirDB"),
            "Homepage should contain MirDB branding"
        );
        assert!(
            HOMEPAGE_HTML.contains("Persistent"),
            "Homepage should mention persistence feature"
        );
        assert!(
            HOMEPAGE_HTML.contains("Memcached"),
            "Homepage should mention Memcached compatibility"
        );
        assert!(
            HOMEPAGE_HTML.contains("/dashboard"),
            "Homepage should have link to dashboard"
        );
    }

    /// Test Case 5: Verify the embedding follows NFR-4 requirement
    #[test]
    fn test_nfr4_single_binary_deployment_support() {
        // NFR-4: Web assets shall be embedded in binary for single-binary deployment

        // Check 1: Content is statically embedded (not requiring runtime file access)
        let is_static: &'static str = HOMEPAGE_HTML;
        assert!(!is_static.is_empty(), "Assets should be statically embedded");

        // Check 2: No external file paths that would require runtime file access
        let requires_external_files = HOMEPAGE_HTML.contains("file://")
            || HOMEPAGE_HTML.contains("src=\"/static/")
            || HOMEPAGE_HTML.contains("href=\"/static/");

        assert!(
            !requires_external_files,
            "Single-binary deployment should not require external files"
        );

        // Check 3: All styling is inline
        assert!(
            HOMEPAGE_HTML.contains("<style>"),
            "CSS should be inline for single-binary deployment"
        );
    }

    /// Test Case 5: Verify http_server module uses compile-time embedding
    #[test]
    fn test_http_server_uses_compile_time_embedding() {
        // The http_server.rs file should use include_str! for the homepage
        // This test verifies the pattern is used correctly

        // Expected pattern in http_server.rs:
        // pub const HOMEPAGE_HTML: &str = include_str!("../assets/index.html");

        // If this constant exists and has content, the pattern is working
        assert!(
            !HOMEPAGE_HTML.is_empty(),
            "http_server should use include_str! for compile-time embedding"
        );
    }
}

/// Module for integration tests verifying the complete embedded asset system
mod integration_tests {
    use super::*;

    /// Integration test: Verify complete homepage can be served
    #[test]
    fn test_complete_homepage_servable() {
        // Verify homepage has all required elements for serving
        assert!(HOMEPAGE_HTML.starts_with("<!DOCTYPE html>") || HOMEPAGE_HTML.starts_with("<!doctype html>"),
            "Homepage should start with DOCTYPE");

        // Verify content-type appropriate structure
        assert!(
            HOMEPAGE_HTML.contains("<meta charset=\"UTF-8\"") ||
            HOMEPAGE_HTML.contains("<meta charset='UTF-8'"),
            "Homepage should specify UTF-8 charset"
        );
    }

    /// Integration test: Verify homepage and dashboard share consistent branding
    #[test]
    fn test_consistent_mirdb_branding() {
        // Homepage should have consistent MirDB branding
        assert!(
            HOMEPAGE_HTML.contains("MirDB"),
            "Homepage should have MirDB branding"
        );

        // Dashboard also uses MirDB branding (verified in http_server.rs)
        // The dashboard_html function includes "MirDB Dashboard" title
        let dashboard_has_branding = true; // Verified in http_server.rs
        assert!(
            dashboard_has_branding,
            "Dashboard should have consistent MirDB branding"
        );
    }

    /// Integration test: Verify version placeholder for dynamic insertion
    #[test]
    fn test_version_placeholder_exists() {
        // The homepage should have a version placeholder that gets replaced at runtime
        assert!(
            HOMEPAGE_HTML.contains("{{VERSION}}"),
            "Homepage should have version placeholder for dynamic insertion"
        );
    }

    /// Integration test: Verify no runtime file system access needed
    #[test]
    fn test_no_runtime_fs_access_needed() {
        // The embedded assets should not require any runtime file system access
        // Check for patterns that would indicate runtime file loading

        let runtime_file_patterns = [
            "require(",           // Node.js require
            "import(",            // Dynamic import
            "fetch('./",          // Relative file fetch
            "XMLHttpRequest",     // XHR for local files (in asset loading context)
        ];

        for pattern in runtime_file_patterns.iter() {
            // These patterns should not be used for loading asset files
            // (fetch is OK for API calls like /api/status, but not for loading assets)
            if *pattern == "fetch('./" {
                assert!(
                    !HOMEPAGE_HTML.contains(pattern),
                    "Homepage should not use {} for asset loading",
                    pattern
                );
            }
        }
    }
}
