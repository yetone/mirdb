//! Homepage rendering and functionality tests.
//!
//! Owner: Scenario 1 - Homepage Core Rendering
//! Co-owners: Multiple scenarios for specific tests
//!
//! Test areas:
//! - All sections render correctly (Scenario 1)
//! - Status indicator updates (Scenario 2)
//! - Metrics refresh (Scenario 4)
//! - Documentation links work (Scenario 6)
//! - Theme toggle works (Scenario 7)
//! - Performance metrics (Scenario 11)

use std::fs;
use std::path::Path;

/// Helper function to load the homepage HTML content
fn load_homepage_html() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/index.html");
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read index.html at {:?}: {}", path, e))
}

/// Helper function to check if HTML contains a specific element by ID
fn has_element_with_id(html: &str, id: &str) -> bool {
    html.contains(&format!("id=\"{}\"", id))
}

/// Helper function to check if HTML contains specific text content
fn contains_text(html: &str, text: &str) -> bool {
    html.contains(text)
}

// =============================================
// Test Case 1: HTTP 200 response simulation
// Verifies homepage HTML file exists and is valid
// =============================================
#[test]
fn test_homepage_html_exists_and_valid() {
    let html = load_homepage_html();

    // Verify it's a valid HTML document
    assert!(html.contains("<!DOCTYPE html>"), "Should have DOCTYPE declaration");
    assert!(html.contains("<html"), "Should have html tag");
    assert!(html.contains("<head>"), "Should have head section");
    assert!(html.contains("<body>"), "Should have body section");
    assert!(html.contains("</html>"), "Should have closing html tag");
}

// =============================================
// Test Case 2: Header elements verification
// Verifies header contains MirDB branding, name, and version
// =============================================
#[test]
fn test_header_contains_logo_and_branding() {
    let html = load_homepage_html();

    // Check header section exists
    assert!(has_element_with_id(&html, "header"), "Should have header element");

    // Check logo section
    assert!(has_element_with_id(&html, "logo"), "Should have logo element");
    assert!(contains_text(&html, "MirDB"), "Should contain MirDB branding");

    // Check version display
    assert!(has_element_with_id(&html, "version"), "Should have version element");
    assert!(contains_text(&html, "v0.1.0") || contains_text(&html, "v0."),
            "Should display version number");
}

#[test]
fn test_header_has_product_name() {
    let html = load_homepage_html();

    // Check for product name in logo
    assert!(html.contains("class=\"logo-text\">MirDB</span>") ||
            html.contains(">MirDB<"),
            "Should have MirDB product name displayed");
}

// =============================================
// Test Case 3: Navigation links verification
// Verifies navigation includes Overview, API Docs, and GitHub
// =============================================
#[test]
fn test_navigation_links() {
    let html = load_homepage_html();

    // Check navigation section exists
    assert!(has_element_with_id(&html, "nav"), "Should have navigation element");

    // Check Overview link
    assert!(has_element_with_id(&html, "nav-overview"), "Should have Overview nav link");
    assert!(contains_text(&html, "Overview"), "Should have Overview link text");

    // Check API Docs link
    assert!(has_element_with_id(&html, "nav-api-docs"), "Should have API Docs nav link");
    assert!(contains_text(&html, "API Docs"), "Should have API Docs link text");

    // Check GitHub link
    assert!(has_element_with_id(&html, "nav-github"), "Should have GitHub nav link");
    assert!(contains_text(&html, "GitHub"), "Should have GitHub link text");
}

#[test]
fn test_navigation_links_have_valid_hrefs() {
    let html = load_homepage_html();

    // Check that external links have proper URLs
    assert!(contains_text(&html, "href=\"https://github.com"),
            "Should have GitHub URL");
    assert!(contains_text(&html, "memcached") && contains_text(&html, "protocol"),
            "Should reference Memcached protocol");
}

// =============================================
// Test Case 4: Hero section verification
// Verifies hero section displays tagline and CTA
// =============================================
#[test]
fn test_hero_section_exists() {
    let html = load_homepage_html();

    // Check hero section exists
    assert!(has_element_with_id(&html, "hero"), "Should have hero section");
}

#[test]
fn test_hero_tagline() {
    let html = load_homepage_html();

    // Check for product tagline
    assert!(has_element_with_id(&html, "tagline"), "Should have tagline element");
    assert!(contains_text(&html, "A Persistent Key-Value Store with Memcached Protocol"),
            "Should display the correct tagline");
}

#[test]
fn test_hero_cta_button() {
    let html = load_homepage_html();

    // Check for CTA button
    assert!(has_element_with_id(&html, "cta-button"), "Should have CTA button");
    assert!(contains_text(&html, "Get Started in Seconds"),
            "Should have 'Get Started in Seconds' CTA text");
}

#[test]
fn test_hero_status_indicator() {
    let html = load_homepage_html();

    // Check for status indicator
    assert!(has_element_with_id(&html, "status-indicator"),
            "Should have status indicator");
    assert!(contains_text(&html, "status-dot"),
            "Should have status dot indicator");
}

// =============================================
// Test Case 5: Dashboard metrics panel verification
// Verifies dashboard shows all required metrics
// =============================================
#[test]
fn test_dashboard_section_exists() {
    let html = load_homepage_html();

    // Check dashboard section exists
    assert!(has_element_with_id(&html, "dashboard"), "Should have dashboard section");
}

#[test]
fn test_dashboard_uptime_metric() {
    let html = load_homepage_html();

    // Check uptime metric
    assert!(has_element_with_id(&html, "metric-uptime"),
            "Should have uptime metric card");
    assert!(contains_text(&html, "Uptime"),
            "Should have Uptime label");
}

#[test]
fn test_dashboard_memory_metric() {
    let html = load_homepage_html();

    // Check memory metric
    assert!(has_element_with_id(&html, "metric-memory"),
            "Should have memory metric card");
    assert!(contains_text(&html, "Memory Usage") || contains_text(&html, "Memory"),
            "Should have Memory Usage label");
}

#[test]
fn test_dashboard_keys_metric() {
    let html = load_homepage_html();

    // Check keys metric
    assert!(has_element_with_id(&html, "metric-keys"),
            "Should have keys metric card");
    assert!(contains_text(&html, "Total Keys") || contains_text(&html, "Keys"),
            "Should have Keys label");
}

#[test]
fn test_dashboard_ops_metric() {
    let html = load_homepage_html();

    // Check ops/sec metric
    assert!(has_element_with_id(&html, "metric-ops"),
            "Should have ops metric card");
    assert!(contains_text(&html, "Ops/sec") || contains_text(&html, "ops"),
            "Should have Ops/sec label");
}

#[test]
fn test_dashboard_storage_metric() {
    let html = load_homepage_html();

    // Check storage metric
    assert!(has_element_with_id(&html, "metric-storage"),
            "Should have storage metric card");
    assert!(contains_text(&html, "Storage") || contains_text(&html, "storage"),
            "Should have Storage label");
}

#[test]
fn test_dashboard_has_all_five_metrics() {
    let html = load_homepage_html();

    // Verify all 5 required metrics are present
    let metrics = ["uptime", "memory", "keys", "ops", "storage"];
    for metric in &metrics {
        assert!(
            html.contains(&format!("data-metric=\"{}\"", metric)) ||
            html.contains(&format!("id=\"metric-{}\"", metric)),
            "Dashboard should have {} metric", metric
        );
    }
}

// =============================================
// Test Case 6: Footer content verification
// Verifies footer contains required links and license
// =============================================
#[test]
fn test_footer_section_exists() {
    let html = load_homepage_html();

    // Check footer section exists
    assert!(has_element_with_id(&html, "footer"), "Should have footer section");
}

#[test]
fn test_footer_memcached_protocol_link() {
    let html = load_homepage_html();

    // Check for Memcached protocol link in footer
    assert!(has_element_with_id(&html, "footer-protocol"),
            "Should have protocol link in footer");
    assert!(contains_text(&html, "Memcached Protocol"),
            "Should have Memcached Protocol link text");
}

#[test]
fn test_footer_github_link() {
    let html = load_homepage_html();

    // Check for GitHub link in footer
    assert!(has_element_with_id(&html, "footer-github"),
            "Should have GitHub link in footer");
}

#[test]
fn test_footer_license_info() {
    let html = load_homepage_html();

    // Check for license information
    assert!(has_element_with_id(&html, "footer-license"),
            "Should have license section in footer");
    assert!(contains_text(&html, "MIT License") ||
            contains_text(&html, "License"),
            "Should display license information");
}

// =============================================
// Additional structural tests
// =============================================
#[test]
fn test_html_has_proper_meta_tags() {
    let html = load_homepage_html();

    // Check for essential meta tags
    assert!(contains_text(&html, "charset=\"UTF-8\"") ||
            contains_text(&html, "charset=UTF-8"),
            "Should have UTF-8 charset meta tag");
    assert!(contains_text(&html, "viewport"),
            "Should have viewport meta tag for responsiveness");
}

#[test]
fn test_html_loads_css_stylesheet() {
    let html = load_homepage_html();

    // Check for CSS link
    assert!(contains_text(&html, "main.css"),
            "Should link to main.css stylesheet");
    assert!(contains_text(&html, "rel=\"stylesheet\""),
            "Should have proper stylesheet link");
}

#[test]
fn test_html_has_title() {
    let html = load_homepage_html();

    // Check for page title
    assert!(contains_text(&html, "<title>"),
            "Should have title tag");
    assert!(contains_text(&html, "MirDB"),
            "Title should contain MirDB");
}

#[test]
fn test_content_section_exists() {
    let html = load_homepage_html();

    // Check main content areas exist
    assert!(has_element_with_id(&html, "content"),
            "Should have main content section");
    assert!(has_element_with_id(&html, "features"),
            "Should have features section");
    assert!(has_element_with_id(&html, "quickstart"),
            "Should have quickstart section");
}

// =============================================
// CSS file tests
// =============================================
#[test]
fn test_css_file_exists_and_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read main.css at {:?}: {}", path, e));

    // Verify CSS file has content
    assert!(!css.is_empty(), "CSS file should not be empty");

    // Verify essential CSS classes are defined
    assert!(css.contains(".header"), "Should have header styles");
    assert!(css.contains(".hero"), "Should have hero styles");
    assert!(css.contains(".dashboard"), "Should have dashboard styles");
    assert!(css.contains(".footer"), "Should have footer styles");
}

#[test]
fn test_css_has_required_styles() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path).unwrap();

    // Check for metric card styles
    assert!(css.contains(".metric-card"), "Should have metric card styles");
    assert!(css.contains(".metric-value"), "Should have metric value styles");

    // Check for navigation styles
    assert!(css.contains(".nav"), "Should have navigation styles");
    assert!(css.contains(".nav-link"), "Should have nav link styles");

    // Check for CTA button styles
    assert!(css.contains(".cta-button"), "Should have CTA button styles");
}

#[test]
fn test_css_has_responsive_styles() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path).unwrap();

    // Check for media queries (responsive design)
    assert!(css.contains("@media"), "Should have media queries for responsiveness");
}

// =============================================
// Scenario 2: System Status Display Tests
// =============================================

/// Test Case 2-1: Status indicator has ARIA label for accessibility
/// Verifies the status indicator has proper ARIA attributes for screen readers
#[test]
fn test_status_indicator_has_aria_label() {
    let html = load_homepage_html();

    // Check for ARIA label on status indicator
    assert!(
        html.contains("aria-label=") && html.contains("status-indicator"),
        "Status indicator should have aria-label attribute"
    );

    // Check that ARIA label describes the server status
    assert!(
        html.contains("Server status:") ||
        html.contains("aria-label=\"Server status"),
        "ARIA label should describe the current server state"
    );
}

/// Test Case 2-2: Status indicator has role attribute
/// Verifies the status indicator has proper role for accessibility
#[test]
fn test_status_indicator_has_role() {
    let html = load_homepage_html();

    // Check for role="status" on status indicator
    assert!(
        html.contains("role=\"status\""),
        "Status indicator should have role='status' attribute"
    );
}

/// Test Case 2-3: Status indicator has aria-live attribute
/// Verifies the status indicator announces updates to screen readers
#[test]
fn test_status_indicator_has_aria_live() {
    let html = load_homepage_html();

    // Check for aria-live="polite" on status indicator
    assert!(
        html.contains("aria-live=\"polite\""),
        "Status indicator should have aria-live='polite' attribute"
    );
}

/// Test Case 2-4: Running status indicator displays correctly
/// Verifies running state shows green indicator with 'Running' label
#[test]
fn test_running_status_indicator_display() {
    let html = load_homepage_html();

    // Check for running class on status dot
    assert!(
        html.contains("status-dot running"),
        "Status dot should have 'running' class by default"
    );

    // Check for Running text in status indicator
    assert!(
        html.contains(">Running<") ||
        html.contains("class=\"status-text\">Running</span>"),
        "Status text should display 'Running'"
    );
}

/// Test Case 2-5: CSS has running status styles (green indicator)
/// Verifies CSS defines green color for running status
#[test]
fn test_css_has_running_status_styles() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path).unwrap();

    // Check for status dot running styles
    assert!(
        css.contains(".status-dot.running"),
        "CSS should have .status-dot.running styles"
    );

    // Check that running status uses success/green color
    assert!(
        css.contains("--color-success") || css.contains("#22c55e") || css.contains("green"),
        "Running status should use green/success color"
    );
}

/// Test Case 2-6: CSS has stopped status styles (red indicator)
/// Verifies CSS defines red color for stopped status
#[test]
fn test_css_has_stopped_status_styles() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path).unwrap();

    // Check for status dot stopped styles
    assert!(
        css.contains(".status-dot.stopped"),
        "CSS should have .status-dot.stopped styles"
    );

    // Check that stopped status uses danger/red color
    assert!(
        css.contains("--color-danger") || css.contains("#ef4444") || css.contains("red"),
        "Stopped status should use red/danger color"
    );
}

/// Test Case 2-7: CSS has error status styles
/// Verifies CSS defines warning color for error status
#[test]
fn test_css_has_error_status_styles() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path).unwrap();

    // Check for status dot error styles
    assert!(
        css.contains(".status-dot.error"),
        "CSS should have .status-dot.error styles"
    );
}

/// Test Case 2-8: Status dot has hidden aria for decorative element
/// Verifies the status dot visual indicator is hidden from screen readers
#[test]
fn test_status_dot_aria_hidden() {
    let html = load_homepage_html();

    // Check for aria-hidden on status dot
    assert!(
        html.contains("status-dot") && html.contains("aria-hidden=\"true\""),
        "Status dot should have aria-hidden='true' since it's decorative"
    );
}

/// Test Case 2-9: JavaScript file exists and is loaded
/// Verifies main.js is loaded for status functionality
#[test]
fn test_main_js_file_exists() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/main.js");
    let js = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read main.js at {:?}: {}", path, e));

    // Verify JS file has content
    assert!(!js.is_empty(), "main.js file should not be empty");

    // Verify it contains status-related functionality
    assert!(
        js.contains("status") || js.contains("Status"),
        "main.js should contain status-related code"
    );
}

/// Test Case 2-10: JavaScript has status update function
/// Verifies main.js can update the status display
#[test]
fn test_main_js_has_status_functions() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/main.js");
    let js = fs::read_to_string(&path).unwrap();

    // Check for status update functionality
    assert!(
        js.contains("updateStatusDisplay") || js.contains("updateStatus"),
        "main.js should have function to update status display"
    );

    // Check for fetch status functionality
    assert!(
        js.contains("fetchStatus") || js.contains("/api/status"),
        "main.js should have function to fetch status from API"
    );
}

/// Test Case 2-11: JavaScript exports status functions for testing
/// Verifies status functions are accessible for external testing
#[test]
fn test_main_js_exports_status_module() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/main.js");
    let js = fs::read_to_string(&path).unwrap();

    // Check that status functions are exported
    assert!(
        js.contains("window.MirDB") || js.contains("MirDB.status"),
        "main.js should export status module for testing"
    );
}

/// Test Case 2-12: HTML loads main.js
/// Verifies main.js script is included in HTML
#[test]
fn test_html_loads_main_js() {
    let html = load_homepage_html();

    // Check for main.js script tag
    assert!(
        html.contains("main.js"),
        "HTML should load main.js script"
    );

    // Check script source path
    assert!(
        html.contains("src=\"/static/js/main.js\"") ||
        html.contains("src='/static/js/main.js'"),
        "HTML should load main.js from correct path"
    );
}
