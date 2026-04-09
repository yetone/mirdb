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

// =============================================
// Scenario 4: Metrics Auto-Refresh Tests
// =============================================

/// Test Case 4-1: Metrics.js file exists
/// Verifies metrics.js file exists for auto-refresh functionality
#[test]
fn test_metrics_js_file_exists() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read metrics.js at {:?}: {}", path, e));

    assert!(!js.is_empty(), "metrics.js file should not be empty");
}

/// Test Case 4-2: Metrics.js has API endpoint configured
/// Verifies metrics.js references /api/metrics endpoint
#[test]
fn test_metrics_js_has_api_endpoint() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("/api/metrics"),
        "metrics.js should reference /api/metrics endpoint"
    );
}

/// Test Case 4-3: Metrics.js has 30-second refresh interval
/// Verifies setInterval or setTimeout is configured for 30000ms
#[test]
fn test_metrics_js_has_30_second_interval() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    // Check for 30000ms interval configuration
    assert!(
        js.contains("30000"),
        "metrics.js should configure 30000ms (30 seconds) refresh interval"
    );

    // Check for setInterval usage
    assert!(
        js.contains("setInterval"),
        "metrics.js should use setInterval for periodic refresh"
    );
}

/// Test Case 4-4: Metrics.js has refresh function
/// Verifies metrics.js has a function to refresh metrics
#[test]
fn test_metrics_js_has_refresh_function() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("refreshMetrics") || js.contains("refresh"),
        "metrics.js should have a refresh function"
    );
}

/// Test Case 4-5: Metrics.js uses fetch API
/// Verifies metrics.js uses fetch API (not full page reload)
#[test]
fn test_metrics_js_uses_fetch_api() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("fetch("),
        "metrics.js should use fetch API for AJAX requests"
    );

    // Ensure no full page reload
    assert!(
        !js.contains("location.reload"),
        "metrics.js should NOT use location.reload"
    );
}

/// Test Case 4-6: Metrics.js preserves scroll position
/// Verifies metrics.js tracks and preserves scroll position during refresh
#[test]
fn test_metrics_js_preserves_scroll_position() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("scrollX") && js.contains("scrollY"),
        "metrics.js should track scroll position"
    );

    assert!(
        js.contains("scrollTo"),
        "metrics.js should have ability to restore scroll position"
    );
}

/// Test Case 4-7: Metrics.js preserves focused element
/// Verifies metrics.js tracks and preserves active element during refresh
#[test]
fn test_metrics_js_preserves_focus() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("activeElement"),
        "metrics.js should track active element"
    );

    assert!(
        js.contains("focus()"),
        "metrics.js should restore focus if needed"
    );
}

/// Test Case 4-8: Metrics.js has uptime formatting
/// Verifies metrics.js can format uptime seconds to human-readable format
#[test]
fn test_metrics_js_formats_uptime() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("formatUptime"),
        "metrics.js should have formatUptime function"
    );
}

/// Test Case 4-9: HTML loads metrics.js script
/// Verifies index.html includes metrics.js script tag
#[test]
fn test_html_loads_metrics_js() {
    let html = load_homepage_html();

    assert!(
        html.contains("metrics.js"),
        "HTML should load metrics.js script"
    );

    assert!(
        html.contains("src=\"/static/js/metrics.js\"") ||
        html.contains("src='/static/js/metrics.js'"),
        "HTML should load metrics.js from correct path"
    );
}

/// Test Case 4-10: Metrics.js is loaded after main.js
/// Verifies metrics.js script comes after main.js in HTML
#[test]
fn test_metrics_js_loaded_after_main_js() {
    let html = load_homepage_html();

    let main_js_pos = html.find("main.js").unwrap_or(0);
    let metrics_js_pos = html.find("metrics.js").unwrap_or(0);

    assert!(
        metrics_js_pos > main_js_pos,
        "metrics.js should be loaded after main.js"
    );
}

/// Test Case 4-11: HTML has data-metric attributes for all metrics
/// Verifies dashboard elements have data-metric attributes for JS targeting
#[test]
fn test_html_has_data_metric_attributes() {
    let html = load_homepage_html();

    let metrics = ["uptime", "memory", "keys", "ops", "storage"];
    for metric in &metrics {
        assert!(
            html.contains(&format!("data-metric=\"{}\"", metric)),
            "HTML should have data-metric=\"{}\" attribute", metric
        );
    }
}

/// Test Case 4-12: Metrics.js exports to MirDB namespace
/// Verifies metrics functions are accessible via window.MirDB.metrics
#[test]
fn test_metrics_js_exports_to_namespace() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("window.MirDB.metrics"),
        "metrics.js should export to window.MirDB.metrics namespace"
    );
}

/// Test Case 4-13: Metrics.js can stop refresh
/// Verifies metrics.js has ability to stop the refresh interval
#[test]
fn test_metrics_js_can_stop_refresh() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("stopMetricsRefresh") && js.contains("clearInterval"),
        "metrics.js should have ability to stop refresh interval"
    );
}

/// Test Case 4-14: Metrics.js handles errors gracefully
/// Verifies metrics.js has error handling for API failures
#[test]
fn test_metrics_js_handles_errors() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("catch") && js.contains("error"),
        "metrics.js should handle fetch errors"
    );

    assert!(
        js.contains("mirdb:metrics-error"),
        "metrics.js should dispatch error event on failure"
    );
}

/// Test Case 4-15: Metrics.js dispatches update event
/// Verifies metrics.js dispatches custom event when metrics update
#[test]
fn test_metrics_js_dispatches_update_event() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let js = fs::read_to_string(&path).unwrap();

    assert!(
        js.contains("mirdb:metrics-updated") && js.contains("CustomEvent"),
        "metrics.js should dispatch mirdb:metrics-updated custom event"
    );
}

// =============================================
// Scenario 6: Documentation Links Tests
// =============================================

/// Test Case 6-1: README link element exists with href pointing to README
/// Verifies the documentation section has a README link with proper href
#[test]
fn test_readme_link_element_exists() {
    let html = load_homepage_html();

    // Check for README link with proper ID
    assert!(
        has_element_with_id(&html, "doc-link-readme"),
        "Should have README link with id='doc-link-readme'"
    );

    // Check for README link text
    assert!(
        contains_text(&html, ">README<") || contains_text(&html, ">README</a>"),
        "Should have README link text"
    );
}

/// Test Case 6-2: README link points to correct URL
/// Verifies README link href points to the project README on GitHub
#[test]
fn test_readme_link_has_correct_href() {
    let html = load_homepage_html();

    // Check that README link points to the GitHub README
    assert!(
        html.contains("href=\"https://github.com/yetone/mirdb/blob/master/README.md\""),
        "README link should point to GitHub README"
    );
}

/// Test Case 6-3: Memcached protocol link element exists
/// Verifies the documentation section has a Memcached protocol link
#[test]
fn test_memcached_protocol_link_element_exists() {
    let html = load_homepage_html();

    // Check for Memcached protocol link with proper ID
    assert!(
        has_element_with_id(&html, "doc-link-protocol"),
        "Should have Memcached protocol link with id='doc-link-protocol'"
    );

    // Check for Memcached Protocol link text
    assert!(
        contains_text(&html, "Memcached Protocol"),
        "Should have 'Memcached Protocol' link text"
    );
}

/// Test Case 6-4: Memcached protocol link points to correct URL
/// Verifies Memcached protocol link href points to the official protocol wiki
#[test]
fn test_memcached_protocol_link_has_correct_href() {
    let html = load_homepage_html();

    // Check that Memcached protocol link points to the official wiki
    assert!(
        html.contains("href=\"https://github.com/memcached/memcached/wiki/Protocols\""),
        "Memcached protocol link should point to official protocol wiki"
    );
}

/// Test Case 6-5: README link is navigable (has target="_blank")
/// Verifies README link opens in new tab for external navigation
#[test]
fn test_readme_link_opens_in_new_tab() {
    let html = load_homepage_html();

    // Check that the README link area has target="_blank"
    assert!(
        html.contains("doc-link-readme") && html.contains("target=\"_blank\""),
        "README link should open in new tab (target='_blank')"
    );

    // Check for rel="noopener" for security
    assert!(
        html.contains("rel=\"noopener\""),
        "External links should have rel='noopener' for security"
    );
}

/// Test Case 6-6: Memcached protocol link is navigable (has target="_blank")
/// Verifies Memcached protocol link opens in new tab for external navigation
#[test]
fn test_memcached_protocol_link_opens_in_new_tab() {
    let html = load_homepage_html();

    // Check that the protocol link area has target="_blank"
    assert!(
        html.contains("doc-link-protocol") && html.contains("target=\"_blank\""),
        "Memcached protocol link should open in new tab (target='_blank')"
    );
}

/// Test Case 6-7: GitHub repository link exists in navigation
/// Verifies navigation has GitHub link pointing to repository
#[test]
fn test_github_nav_link_exists() {
    let html = load_homepage_html();

    // Check for GitHub nav link
    assert!(
        has_element_with_id(&html, "nav-github"),
        "Should have GitHub link in navigation with id='nav-github'"
    );

    // Check for GitHub link text
    assert!(
        contains_text(&html, ">GitHub<") || contains_text(&html, ">GitHub</a>"),
        "Navigation should have GitHub link text"
    );
}

/// Test Case 6-8: GitHub repository link points to correct repository
/// Verifies GitHub link points to yetone/mirdb repository
#[test]
fn test_github_nav_link_has_correct_href() {
    let html = load_homepage_html();

    // Check that GitHub link points to the correct repository
    assert!(
        html.contains("href=\"https://github.com/yetone/mirdb\""),
        "GitHub link should point to https://github.com/yetone/mirdb"
    );
}

/// Test Case 6-9: Documentation section exists
/// Verifies the documentation section container exists
#[test]
fn test_documentation_section_exists() {
    let html = load_homepage_html();

    // Check for documentation section
    assert!(
        has_element_with_id(&html, "documentation"),
        "Should have documentation section with id='documentation'"
    );

    // Check for documentation section title
    assert!(
        contains_text(&html, ">Documentation<"),
        "Documentation section should have 'Documentation' title"
    );
}

/// Test Case 6-10: Documentation links have doc-link class for styling
/// Verifies documentation links have proper CSS class
#[test]
fn test_documentation_links_have_doc_link_class() {
    let html = load_homepage_html();

    // Check that README link has doc-link class
    assert!(
        html.contains("class=\"doc-link\"") || html.contains("class=\"doc-link "),
        "Documentation links should have 'doc-link' class for styling"
    );
}

/// Test Case 6-11: Footer also has Memcached protocol link
/// Verifies protocol documentation is accessible from footer
#[test]
fn test_footer_has_protocol_link() {
    let html = load_homepage_html();

    // Check for protocol link in footer
    assert!(
        has_element_with_id(&html, "footer-protocol"),
        "Footer should have Memcached protocol link"
    );

    // Check footer protocol link has correct URL
    assert!(
        html.contains("footer-protocol") && html.contains("memcached/memcached/wiki/Protocols"),
        "Footer protocol link should point to official wiki"
    );
}

/// Test Case 6-12: Footer has GitHub repository link
/// Verifies GitHub link is accessible from footer
#[test]
fn test_footer_has_github_link() {
    let html = load_homepage_html();

    // Check for GitHub link in footer
    assert!(
        has_element_with_id(&html, "footer-github"),
        "Footer should have GitHub link"
    );

    // Check footer GitHub link has correct URL
    assert!(
        html.contains("footer-github") && html.contains("github.com/yetone/mirdb"),
        "Footer GitHub link should point to repository"
    );
}

/// Test Case 6-13: Documentation section has description text
/// Verifies documentation section includes helpful description
#[test]
fn test_documentation_section_has_description() {
    let html = load_homepage_html();

    // Check for documentation description
    assert!(
        html.contains("doc-description") || html.contains("Learn more about MirDB"),
        "Documentation section should have a description"
    );
}

/// Test Case 6-14: All external links have rel="noopener" for security
/// Verifies external links follow security best practices
#[test]
fn test_external_links_have_noopener() {
    let html = load_homepage_html();

    // Check that target="_blank" links have rel="noopener"
    // Count occurrences of target="_blank" and rel="noopener"
    let target_blank_count = html.matches("target=\"_blank\"").count();
    let noopener_count = html.matches("rel=\"noopener\"").count();

    assert!(
        noopener_count >= target_blank_count,
        "All target='_blank' links should have rel='noopener' ({} vs {})",
        target_blank_count,
        noopener_count
    );
}

/// Test Case 6-15: CSS has documentation link styles
/// Verifies CSS includes styles for documentation links
#[test]
fn test_css_has_documentation_link_styles() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&path).unwrap();

    // Check for documentation section styles
    assert!(
        css.contains(".documentation"),
        "CSS should have .documentation styles"
    );

    // Check for doc-links styles
    assert!(
        css.contains(".doc-links"),
        "CSS should have .doc-links styles"
    );

    // Check for doc-link or doc-links hover styles
    assert!(
        css.contains(".doc-link:hover") || css.contains(".doc-links li a:hover"),
        "CSS should have documentation link hover styles"
    );
}

// =============================================
// Scenario 11: Page Load Performance Tests
// =============================================

/// Performance constant: Maximum total page weight in bytes (500KB)
const MAX_PAGE_WEIGHT_BYTES: usize = 500 * 1024;

/// Performance constant: Maximum number of HTTP requests on initial load
const MAX_HTTP_REQUESTS: usize = 20;

/// Test Case 11-1: DOMContentLoaded time validation (per NFR-2)
/// Verifies page structure supports sub-2-second load by checking for
/// - No inline blocking scripts
/// - Deferred/async script loading
/// - Minimal DOM complexity
#[test]
fn test_page_load_performance_structure() {
    let html = load_homepage_html();

    // Check that critical CSS is not massive (affects DOMContentLoaded)
    let css_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css_content = fs::read_to_string(&css_path).unwrap();

    // CSS should be under 50KB for fast parsing (affects DOMContentLoaded)
    let css_size = css_content.len();
    assert!(
        css_size < 50 * 1024,
        "CSS file should be under 50KB for fast DOMContentLoaded, got {} bytes",
        css_size
    );

    // HTML should not have massive inline styles (affects DOMContentLoaded)
    let inline_style_count = html.matches("<style").count();
    assert!(
        inline_style_count < 3,
        "Should have minimal inline styles for fast DOMContentLoaded, found {}",
        inline_style_count
    );

    // HTML should have a reasonable DOM depth/size
    let html_size = html.len();
    assert!(
        html_size < 50 * 1024,
        "HTML should be under 50KB for fast DOMContentLoaded, got {} bytes",
        html_size
    );
}

/// Test Case 11-2: Time to Interactive (TTI) validation
/// Verifies page structure supports sub-3-second TTI by checking for
/// - Scripts loaded with defer or at end of body
/// - No render-blocking resources
/// - Efficient JavaScript
#[test]
fn test_time_to_interactive_structure() {
    let html = load_homepage_html();

    // Scripts should be at the end of body (before </body>) for faster TTI
    let body_close_pos = html.rfind("</body>").unwrap_or(0);
    let last_script_pos = html.rfind("<script").unwrap_or(0);

    // Last script should be near the end of body (within 500 chars)
    assert!(
        body_close_pos > last_script_pos,
        "Scripts should be placed before closing body tag for faster TTI"
    );
    assert!(
        body_close_pos - last_script_pos < 500,
        "Scripts should be near the end of body for faster TTI"
    );

    // Check JavaScript files are lightweight
    let main_js_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/main.js");
    let main_js = fs::read_to_string(&main_js_path).unwrap();
    assert!(
        main_js.len() < 30 * 1024,
        "main.js should be under 30KB for fast TTI, got {} bytes",
        main_js.len()
    );

    let metrics_js_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let metrics_js = fs::read_to_string(&metrics_js_path).unwrap();
    assert!(
        metrics_js.len() < 30 * 1024,
        "metrics.js should be under 30KB for fast TTI, got {} bytes",
        metrics_js.len()
    );

    // No inline blocking scripts in head
    let head_section = html.split("</head>").next().unwrap_or("");
    let head_script_count = head_section.matches("<script").count();
    assert!(
        head_script_count == 0,
        "Should have no blocking scripts in <head> for fast TTI, found {}",
        head_script_count
    );
}

/// Test Case 11-3: Total page weight validation
/// Verifies total transferred size is under 500KB
#[test]
fn test_total_page_weight() {
    let static_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("static");

    // Calculate total size of all static assets
    let mut total_size: usize = 0;

    // HTML file
    let html_path = static_dir.join("index.html");
    let html_size = fs::read_to_string(&html_path).unwrap().len();
    total_size += html_size;

    // CSS file
    let css_path = static_dir.join("css/main.css");
    let css_size = fs::read_to_string(&css_path).unwrap().len();
    total_size += css_size;

    // JavaScript files
    let js_dir = static_dir.join("js");
    if js_dir.exists() {
        for entry in fs::read_dir(&js_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().map_or(false, |ext| ext == "js") {
                let js_size = fs::read_to_string(entry.path()).unwrap().len();
                total_size += js_size;
            }
        }
    }

    // Total should be under 500KB (MAX_PAGE_WEIGHT_BYTES)
    assert!(
        total_size < MAX_PAGE_WEIGHT_BYTES,
        "Total page weight should be under 500KB ({} bytes), got {} bytes ({:.1} KB)",
        MAX_PAGE_WEIGHT_BYTES,
        total_size,
        total_size as f64 / 1024.0
    );

    // Log the actual size for verification
    println!(
        "Total page weight: {} bytes ({:.1} KB) - under {} KB limit",
        total_size,
        total_size as f64 / 1024.0,
        MAX_PAGE_WEIGHT_BYTES / 1024
    );
}

/// Test Case 11-4: HTTP request count validation
/// Verifies minimal number of HTTP requests (< 20)
#[test]
fn test_http_request_count() {
    let html = load_homepage_html();

    let mut request_count = 0;

    // Count external stylesheets (CSS)
    let stylesheet_count = html.matches("rel=\"stylesheet\"").count();
    request_count += stylesheet_count;

    // Count script tags (JS)
    let script_count = html.matches("<script src=").count();
    request_count += script_count;

    // Count link preloads
    let preload_count = html.matches("rel=\"preload\"").count();
    request_count += preload_count;

    // Count favicon (if present)
    let favicon_count = html.matches("rel=\"icon\"").count();
    request_count += favicon_count;

    // Count images
    let img_count = html.matches("<img").count();
    request_count += img_count;

    // Add 1 for the HTML document itself
    request_count += 1;

    // Total should be under MAX_HTTP_REQUESTS
    assert!(
        request_count < MAX_HTTP_REQUESTS,
        "Should have fewer than {} HTTP requests, found {} (CSS: {}, JS: {}, images: {}, preloads: {}, favicon: {}, HTML: 1)",
        MAX_HTTP_REQUESTS,
        request_count,
        stylesheet_count,
        script_count,
        img_count,
        preload_count,
        favicon_count
    );

    // Log the actual count for verification
    println!(
        "Total HTTP requests: {} - under {} limit (CSS: {}, JS: {}, images: {})",
        request_count, MAX_HTTP_REQUESTS, stylesheet_count, script_count, img_count
    );
}

/// Test Case 11-5: Slow 3G usability validation
/// Verifies page is usable within 5 seconds on slow connection by checking:
/// - Critical content is in HTML (not loaded via JS)
/// - Assets are small enough for slow connections
/// - No large blocking resources
#[test]
fn test_slow_3g_usability() {
    let html = load_homepage_html();

    // Slow 3G bandwidth: ~50 KB/s
    // 5 second budget = ~250KB total
    const SLOW_3G_BUDGET_BYTES: usize = 250 * 1024;

    // Calculate critical path size (HTML + CSS needed for first render)
    let html_size = html.len();
    let css_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css_size = fs::read_to_string(&css_path).unwrap().len();

    let critical_size = html_size + css_size;

    assert!(
        critical_size < SLOW_3G_BUDGET_BYTES,
        "Critical path (HTML + CSS) should be under {} bytes for slow 3G, got {} bytes",
        SLOW_3G_BUDGET_BYTES,
        critical_size
    );

    // Critical content should be visible without JavaScript
    // Check that key sections are in HTML (not dynamically loaded)
    assert!(
        html.contains("MirDB"),
        "Product name should be in HTML for slow 3G usability"
    );
    assert!(
        html.contains("A Persistent Key-Value Store"),
        "Tagline should be in HTML for slow 3G usability"
    );
    assert!(
        html.contains("System Metrics") || html.contains("dashboard"),
        "Dashboard section should be in HTML for slow 3G usability"
    );
    assert!(
        html.contains("Quick Start") || html.contains("quickstart"),
        "Quick start section should be in HTML for slow 3G usability"
    );
    assert!(
        html.contains("Features") || html.contains("features"),
        "Features section should be in HTML for slow 3G usability"
    );

    // Scripts should not block rendering
    assert!(
        !html.contains("<script src=") || html.rfind("<script").unwrap_or(0) > html.find("<main").unwrap_or(0),
        "Scripts should not block main content rendering"
    );

    println!(
        "Critical path size: {} bytes ({:.1} KB) - under {} KB slow 3G budget",
        critical_size,
        critical_size as f64 / 1024.0,
        SLOW_3G_BUDGET_BYTES / 1024
    );
}

/// Additional performance test: CSS efficiency
/// Verifies CSS is optimized for performance
#[test]
fn test_css_performance_efficiency() {
    let css_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/css/main.css");
    let css = fs::read_to_string(&css_path).unwrap();

    // No @import statements (cause additional requests)
    assert!(
        !css.contains("@import"),
        "CSS should not use @import (causes additional HTTP requests)"
    );

    // Uses CSS variables for consistency (sign of well-organized CSS)
    assert!(
        css.contains(":root") && css.contains("--"),
        "CSS should use CSS variables for maintainability"
    );

    // Has media queries (responsive design)
    assert!(
        css.contains("@media"),
        "CSS should have media queries for responsive design"
    );

    // No excessive selectors (reasonable complexity)
    let selector_count = css.matches('{').count();
    assert!(
        selector_count < 200,
        "CSS should have reasonable number of selectors for performance, found {}",
        selector_count
    );
}

/// Additional performance test: JavaScript efficiency
/// Verifies JavaScript is optimized for performance
#[test]
fn test_js_performance_efficiency() {
    let main_js_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/main.js");
    let main_js = fs::read_to_string(&main_js_path).unwrap();

    let metrics_js_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/js/metrics.js");
    let metrics_js = fs::read_to_string(&metrics_js_path).unwrap();

    // No synchronous XMLHttpRequest (blocks rendering)
    assert!(
        !main_js.contains("XMLHttpRequest") || main_js.contains("async"),
        "main.js should not use synchronous XMLHttpRequest"
    );
    assert!(
        !metrics_js.contains("XMLHttpRequest") || metrics_js.contains("async"),
        "metrics.js should not use synchronous XMLHttpRequest"
    );

    // Uses modern async patterns (fetch, async/await)
    assert!(
        main_js.contains("fetch") || main_js.contains("async"),
        "main.js should use modern async patterns"
    );
    assert!(
        metrics_js.contains("fetch"),
        "metrics.js should use fetch API"
    );

    // Uses 'use strict' for better performance
    assert!(
        main_js.contains("'use strict'") || main_js.contains("\"use strict\""),
        "main.js should use strict mode"
    );
    assert!(
        metrics_js.contains("'use strict'") || metrics_js.contains("\"use strict\""),
        "metrics.js should use strict mode"
    );

    // No document.write (blocks parsing)
    assert!(
        !main_js.contains("document.write"),
        "main.js should not use document.write"
    );
    assert!(
        !metrics_js.contains("document.write"),
        "metrics.js should not use document.write"
    );
}

/// Additional performance test: HTML structure efficiency
/// Verifies HTML is optimized for fast rendering
#[test]
fn test_html_performance_structure() {
    let html = load_homepage_html();

    // Has proper doctype for standards mode (faster rendering)
    assert!(
        html.starts_with("<!DOCTYPE html>"),
        "HTML should have DOCTYPE for standards mode"
    );

    // Has viewport meta tag (needed for responsive rendering)
    assert!(
        html.contains("name=\"viewport\""),
        "HTML should have viewport meta tag"
    );

    // Has charset meta tag early (helps parser)
    let head_section = html.split("</head>").next().unwrap_or("");
    assert!(
        head_section.contains("charset=\"UTF-8\"") || head_section.contains("charset=UTF-8"),
        "HTML should have charset meta tag in head"
    );

    // CSS is in head (not body)
    let css_link_pos = html.find("rel=\"stylesheet\"").unwrap_or(0);
    let head_close_pos = html.find("</head>").unwrap_or(0);
    assert!(
        css_link_pos < head_close_pos,
        "CSS should be linked in <head> for optimal rendering"
    );

    // No excessive nesting (keep DOM depth reasonable)
    let max_nesting = count_max_nesting(&html);
    assert!(
        max_nesting < 15,
        "HTML should have reasonable nesting depth for performance, found {}",
        max_nesting
    );
}

/// Helper function to estimate maximum nesting depth in HTML
fn count_max_nesting(html: &str) -> usize {
    let mut current_depth = 0;
    let mut max_depth = 0;

    // Simple heuristic: count common block-level elements
    let block_elements = ["<div", "<section", "<main", "<article", "<header", "<footer", "<nav", "<ul", "<ol", "<li"];
    let close_elements = ["</div>", "</section>", "</main>", "</article>", "</header>", "</footer>", "</nav>", "</ul>", "</ol>", "</li>"];

    for line in html.lines() {
        for open_tag in &block_elements {
            current_depth += line.matches(open_tag).count();
        }
        if current_depth > max_depth {
            max_depth = current_depth;
        }
        for close_tag in &close_elements {
            let closes = line.matches(close_tag).count();
            current_depth = current_depth.saturating_sub(closes);
        }
    }

    max_depth
}
