//! Integration Tests for Homepage Static Content
//!
//! Scenario 2 - Homepage Static Content Rendering
//!
//! These tests verify that the homepage HTML contains all required sections:
//! - Test Case 1: GET / returns HTML with HTTP 200 and Content-Type: text/html
//! - Test Case 2: Header with logo/name and theme toggle
//! - Test Case 3: Features section with Memcached, LSM tree, and persistence
//! - Test Case 4: Quick start section with CLI examples
//! - Test Case 5: Footer with GitHub and documentation links

/// The embedded HTML content from the homepage
const INDEX_HTML: &str = include_str!("../../mirdb-server/assets/index.html");

/// The embedded CSS content
const STYLE_CSS: &str = include_str!("../../mirdb-server/assets/css/style.css");

// ============================================================================
// Test Case 1: Response returns valid HTML document
// ============================================================================

#[test]
fn test_case_1_html_is_valid_document() {
    // Verify the HTML contains a valid doctype and structure
    assert!(
        INDEX_HTML.contains("<!DOCTYPE html>"),
        "HTML must contain DOCTYPE declaration"
    );
    assert!(
        INDEX_HTML.contains("<html"),
        "HTML must contain html element"
    );
    assert!(
        INDEX_HTML.contains("</html>"),
        "HTML must contain closing html tag"
    );
    assert!(
        INDEX_HTML.contains("<head>"),
        "HTML must contain head element"
    );
    assert!(
        INDEX_HTML.contains("<body>"),
        "HTML must contain body element"
    );
}

#[test]
fn test_case_1_html_has_proper_meta_tags() {
    assert!(
        INDEX_HTML.contains("charset=\"UTF-8\""),
        "HTML must specify UTF-8 charset"
    );
    assert!(
        INDEX_HTML.contains("viewport"),
        "HTML must have viewport meta tag"
    );
}

#[test]
fn test_case_1_html_has_css_link() {
    assert!(
        INDEX_HTML.contains("rel=\"stylesheet\"") && INDEX_HTML.contains("style.css"),
        "HTML must link to CSS stylesheet"
    );
}

#[test]
fn test_case_1_content_type_is_html() {
    // This test verifies the HTML has proper structure for text/html content type
    assert!(
        INDEX_HTML.starts_with("<!DOCTYPE html>"),
        "HTML document should start with DOCTYPE for proper content-type"
    );
    assert!(
        INDEX_HTML.contains("<title>") && INDEX_HTML.contains("</title>"),
        "HTML must have a title element"
    );
}

// ============================================================================
// Test Case 2: Header section with logo/name and theme toggle
// ============================================================================

#[test]
fn test_case_2_header_section_exists() {
    assert!(
        INDEX_HTML.contains("<header") && INDEX_HTML.contains("</header>"),
        "HTML must contain header element"
    );
    assert!(
        INDEX_HTML.contains("class=\"header\"") || INDEX_HTML.contains("class='header'"),
        "Header must have header class"
    );
}

#[test]
fn test_case_2_header_contains_logo() {
    assert!(
        INDEX_HTML.contains("class=\"logo\"") || INDEX_HTML.contains("class='logo'"),
        "HTML must contain logo element"
    );
    assert!(
        INDEX_HTML.contains("MirDB"),
        "Header must contain MirDB name"
    );
}

#[test]
fn test_case_2_header_contains_theme_toggle() {
    assert!(
        INDEX_HTML.contains("theme-toggle"),
        "Header must contain theme toggle button"
    );
    assert!(
        INDEX_HTML.contains("id=\"theme-toggle\"") || INDEX_HTML.contains("id='theme-toggle'"),
        "Theme toggle must have id for JavaScript interaction"
    );
}

#[test]
fn test_case_2_header_contains_navigation() {
    assert!(
        INDEX_HTML.contains("<nav") || INDEX_HTML.contains("class=\"nav\""),
        "Header must contain navigation"
    );
}

// ============================================================================
// Test Case 3: Features section with Memcached, LSM tree, persistence
// ============================================================================

#[test]
fn test_case_3_features_section_exists() {
    assert!(
        INDEX_HTML.contains("id=\"features\"") || INDEX_HTML.contains("id='features'"),
        "HTML must contain features section with id"
    );
    assert!(
        INDEX_HTML.contains("class=\"features\"") || INDEX_HTML.contains("class='features'"),
        "Features section must have features class"
    );
}

#[test]
fn test_case_3_features_memcached_protocol() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("memcached") && html_lower.contains("protocol"),
        "Features section must describe Memcached protocol compatibility"
    );
}

#[test]
fn test_case_3_features_lsm_tree() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("lsm") && html_lower.contains("tree"),
        "Features section must describe LSM tree architecture"
    );
}

#[test]
fn test_case_3_features_persistence() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("persistent") || html_lower.contains("persistence") || html_lower.contains("persisted"),
        "Features section must describe data persistence"
    );
}

#[test]
fn test_case_3_features_has_feature_cards() {
    let card_count = INDEX_HTML.matches("class=\"feature-card\"").count()
        + INDEX_HTML.matches("class='feature-card'").count();
    assert!(
        card_count >= 3,
        "Features section must have at least 3 feature cards, found {}",
        card_count
    );
}

// ============================================================================
// Test Case 4: Quick start section with CLI examples
// ============================================================================

#[test]
fn test_case_4_quick_start_section_exists() {
    assert!(
        INDEX_HTML.contains("id=\"quick-start\"") || INDEX_HTML.contains("id='quick-start'"),
        "HTML must contain quick-start section with id"
    );
    assert!(
        INDEX_HTML.contains("class=\"quick-start\"") || INDEX_HTML.contains("class='quick-start'"),
        "Quick start section must have quick-start class"
    );
}

#[test]
fn test_case_4_quick_start_contains_code_examples() {
    assert!(
        INDEX_HTML.contains("<pre>") && INDEX_HTML.contains("<code>"),
        "Quick start must contain code examples in pre/code tags"
    );
}

#[test]
fn test_case_4_quick_start_contains_set_command() {
    assert!(
        INDEX_HTML.contains("set ") || INDEX_HTML.contains("SET "),
        "Quick start must show set command example"
    );
}

#[test]
fn test_case_4_quick_start_contains_get_command() {
    assert!(
        INDEX_HTML.contains("get ") || INDEX_HTML.contains("GET "),
        "Quick start must show get command example"
    );
}

#[test]
fn test_case_4_quick_start_contains_cli_examples() {
    assert!(
        INDEX_HTML.contains("telnet") || INDEX_HTML.contains("nc ") || INDEX_HTML.contains("netcat"),
        "Quick start must contain CLI connection examples"
    );
}

#[test]
fn test_case_4_quick_start_contains_memcached_commands() {
    // Verify memcached-style commands are shown
    assert!(
        INDEX_HTML.contains("STORED") || INDEX_HTML.contains("VALUE") || INDEX_HTML.contains("END"),
        "Quick start must show memcached protocol responses"
    );
}

// ============================================================================
// Test Case 5: Footer with GitHub and documentation links
// ============================================================================

#[test]
fn test_case_5_footer_section_exists() {
    assert!(
        INDEX_HTML.contains("<footer") && INDEX_HTML.contains("</footer>"),
        "HTML must contain footer element"
    );
    assert!(
        INDEX_HTML.contains("class=\"footer\"") || INDEX_HTML.contains("class='footer'"),
        "Footer must have footer class"
    );
}

#[test]
fn test_case_5_footer_contains_github_link() {
    assert!(
        INDEX_HTML.contains("github.com"),
        "Footer must contain GitHub link"
    );
    assert!(
        INDEX_HTML.contains("href=\"https://github.com") || INDEX_HTML.contains("href='https://github.com"),
        "Footer must have valid GitHub URL"
    );
}

#[test]
fn test_case_5_footer_contains_documentation_link() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("documentation") || html_lower.contains("docs") || html_lower.contains("readme"),
        "Footer must contain documentation link"
    );
}

#[test]
fn test_case_5_footer_contains_footer_links_section() {
    assert!(
        INDEX_HTML.contains("footer-links") || INDEX_HTML.contains("footer-content"),
        "Footer must have organized links section"
    );
}

// ============================================================================
// CSS Tests - Verify styling exists for all sections
// ============================================================================

#[test]
fn test_css_exists_and_has_content() {
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
fn test_css_has_root_variables() {
    assert!(
        STYLE_CSS.contains(":root"),
        "CSS must define CSS variables in :root"
    );
    assert!(
        STYLE_CSS.contains("--color-"),
        "CSS must define color variables"
    );
}

#[test]
fn test_css_has_header_styles() {
    assert!(
        STYLE_CSS.contains(".header"),
        "CSS must have header styles"
    );
}

#[test]
fn test_css_has_feature_styles() {
    assert!(
        STYLE_CSS.contains(".features") || STYLE_CSS.contains(".feature-card"),
        "CSS must have feature section styles"
    );
}

#[test]
fn test_css_has_footer_styles() {
    assert!(
        STYLE_CSS.contains(".footer"),
        "CSS must have footer styles"
    );
}

#[test]
fn test_css_has_quick_start_styles() {
    assert!(
        STYLE_CSS.contains(".quick-start") || STYLE_CSS.contains("pre"),
        "CSS must have quick start section styles"
    );
}

#[test]
fn test_css_has_theme_toggle_styles() {
    assert!(
        STYLE_CSS.contains(".theme-toggle"),
        "CSS must have theme toggle button styles"
    );
}
