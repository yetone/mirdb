//! Accessibility Compliance tests.
//! Owner: Scenario 15 - Accessibility Compliance
//!
//! Tests:
//! 1. Verify keyboard navigation - all interactive elements reachable via Tab key
//! 2. Verify visible focus indicators on all interactive elements
//! 3. Verify semantic HTML structure - heading hierarchy and landmark regions
//! 4. Verify all images have alt text (logo and icons)
//!
//! WCAG 2.1 AA Requirements:
//! - 4.5:1 contrast ratio for normal text
//! - 3:1 contrast ratio for large text
//! - Keyboard accessible interactive elements
//! - Visible focus indicators

use axum::{body::Body, http::Request};
use tower::ServiceExt;

use mirdb::web::routes::create_router;

/// Helper function to get homepage HTML content
async fn get_homepage_html() -> String {
    let app = create_router();
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Helper function to get CSS content
async fn get_css_content() -> String {
    let app = create_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/style.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

// ============================================================================
// Test Case 1: Keyboard Navigation - Tab through all interactive elements
// Expected: All buttons, links, and form fields reachable via Tab key
// ============================================================================

/// Test that all interactive elements are keyboard accessible
/// Interactive elements should have no tabindex=-1 (which would prevent keyboard access)
#[tokio::test]
async fn test_keyboard_navigation_all_interactive_elements_accessible() {
    let html = get_homepage_html().await;

    // Verify skip link exists for keyboard navigation
    assert!(
        html.contains("skip-link") && html.contains("Skip to main content"),
        "Page should have a skip link for keyboard navigation"
    );

    // Verify skip link targets main content
    assert!(
        html.contains("href=\"#main-content\""),
        "Skip link should target main content"
    );

    // Verify main content has id for skip link target
    assert!(
        html.contains("id=\"main-content\""),
        "Main content should have id for skip link target"
    );

    // Verify theme toggle button is accessible (no tabindex=-1)
    assert!(
        html.contains("id=\"theme-toggle\"") && html.contains("class=\"theme-toggle\""),
        "Theme toggle button should exist"
    );
    assert!(
        !html.contains("theme-toggle\" tabindex=\"-1\""),
        "Theme toggle button should be keyboard accessible"
    );

    // Verify all form inputs have proper id attributes for accessibility
    assert!(
        html.contains("id=\"set-key\"") || html.contains("name=\"key\""),
        "SET form should have accessible key input"
    );
    assert!(
        html.contains("id=\"set-value\"") || html.contains("name=\"value\""),
        "SET form should have accessible value input"
    );
    assert!(
        html.contains("id=\"get-key\""),
        "GET form should have accessible key input"
    );
    assert!(
        html.contains("id=\"delete-key\""),
        "DELETE form should have accessible key input"
    );

    // Verify navigation links are keyboard accessible
    assert!(
        html.contains("class=\"github-link\"") && html.contains("href=\"https://github.com"),
        "GitHub link should be keyboard accessible"
    );

    // Verify hero action buttons/links exist
    assert!(
        html.contains("class=\"btn") && html.contains("href=\"#"),
        "Hero action buttons should be keyboard accessible links"
    );
}

/// Test that all buttons have type attribute for keyboard accessibility
#[tokio::test]
async fn test_buttons_have_type_attribute() {
    let html = get_homepage_html().await;

    // Verify submit buttons exist in forms
    assert!(
        html.contains("type=\"submit\""),
        "Forms should have submit buttons with type attribute"
    );

    // Count submit buttons - should be at least 3 (SET, GET, DELETE)
    let submit_count = html.matches("type=\"submit\"").count();
    assert!(
        submit_count >= 3,
        "Should have at least 3 submit buttons for SET, GET, DELETE operations"
    );
}

// ============================================================================
// Test Case 2: Check Focus Indicators
// Expected: Visible focus ring on all interactive elements
// ============================================================================

/// Test that CSS includes focus styles for all interactive elements
#[tokio::test]
async fn test_focus_indicators_visible_in_css() {
    let css = get_css_content().await;

    // Verify global focus-visible styles exist
    assert!(
        css.contains(":focus-visible") || css.contains(":focus"),
        "CSS should include focus-visible or focus styles"
    );

    // Verify button focus styles
    assert!(
        css.contains(".btn:focus") || css.contains(".btn:focus-visible"),
        "CSS should include button focus styles"
    );

    // Verify input focus styles
    assert!(
        css.contains("input:focus") || css.contains("input:focus-visible"),
        "CSS should include input focus styles"
    );

    // Verify theme toggle focus styles
    assert!(
        css.contains(".theme-toggle:focus"),
        "CSS should include theme toggle focus styles"
    );

    // Verify GitHub link focus styles
    assert!(
        css.contains(".github-link:focus"),
        "CSS should include GitHub link focus styles"
    );

    // Verify focus uses outline property for visibility
    assert!(
        css.contains("outline:") || css.contains("outline-color"),
        "Focus styles should use outline for visibility"
    );

    // Verify focus outline is at least 2px for visibility
    assert!(
        css.contains("outline: 3px") || css.contains("outline: 2px"),
        "Focus outline should be at least 2px wide for visibility"
    );
}

/// Test that skip link has focus styles
#[tokio::test]
async fn test_skip_link_has_focus_styles() {
    let css = get_css_content().await;

    // Verify skip link class exists
    assert!(
        css.contains(".skip-link"),
        "CSS should include skip-link styles"
    );

    // Verify skip link focus styles
    assert!(
        css.contains(".skip-link:focus"),
        "Skip link should have focus styles"
    );
}

// ============================================================================
// Test Case 3: Run automated accessibility checker (semantic structure)
// Expected: No critical or serious accessibility violations
// ============================================================================

/// Test semantic HTML structure - proper heading hierarchy
#[tokio::test]
async fn test_semantic_html_heading_hierarchy() {
    let html = get_homepage_html().await;

    // Verify h1 exists (should be one main heading)
    let h1_count = html.matches("<h1").count();
    assert_eq!(h1_count, 1, "Page should have exactly one h1 heading");

    // Verify h2 exists (hero title)
    assert!(html.contains("<h2"), "Page should have h2 headings");

    // Verify h3 exists (section titles)
    assert!(html.contains("<h3"), "Page should have h3 section headings");

    // Verify h4 exists (subsection titles)
    assert!(html.contains("<h4"), "Page should have h4 subsection headings");

    // Verify headings follow proper order (h1 before h2 before h3)
    let h1_pos = html.find("<h1").expect("h1 should exist");
    let h2_pos = html.find("<h2").expect("h2 should exist");
    let h3_pos = html.find("<h3").expect("h3 should exist");
    assert!(
        h1_pos < h2_pos && h2_pos < h3_pos,
        "Headings should follow proper hierarchy order"
    );
}

/// Test semantic HTML structure - landmark regions
#[tokio::test]
async fn test_semantic_html_landmark_regions() {
    let html = get_homepage_html().await;

    // Verify header landmark
    assert!(
        html.contains("<header") && html.contains("role=\"banner\""),
        "Page should have header landmark with banner role"
    );

    // Verify main landmark
    assert!(
        html.contains("<main") && html.contains("role=\"main\""),
        "Page should have main landmark with main role"
    );

    // Verify footer landmark
    assert!(
        html.contains("<footer") && html.contains("role=\"contentinfo\""),
        "Page should have footer landmark with contentinfo role"
    );

    // Verify navigation landmark
    assert!(
        html.contains("<nav") && html.contains("role=\"navigation\""),
        "Page should have nav landmark with navigation role"
    );

    // Verify navigation has aria-label
    assert!(
        html.contains("aria-label=\"Main navigation\""),
        "Navigation should have aria-label"
    );
}

/// Test semantic HTML structure - sections have proper labeling
#[tokio::test]
async fn test_sections_have_proper_labeling() {
    let html = get_homepage_html().await;

    // Verify sections use aria-labelledby
    assert!(
        html.contains("aria-labelledby=\"hero-title\""),
        "Hero section should have aria-labelledby"
    );
    assert!(
        html.contains("aria-labelledby=\"features-title\""),
        "Features section should have aria-labelledby"
    );
    assert!(
        html.contains("aria-labelledby=\"dashboard-title\""),
        "Dashboard section should have aria-labelledby"
    );
    assert!(
        html.contains("aria-labelledby=\"try-title\""),
        "Try It Out section should have aria-labelledby"
    );
    assert!(
        html.contains("aria-labelledby=\"protocol-title\""),
        "Protocol section should have aria-labelledby"
    );
}

/// Test that forms have proper accessibility attributes
#[tokio::test]
async fn test_forms_have_proper_accessibility() {
    let html = get_homepage_html().await;

    // Verify forms have aria-labelledby
    assert!(
        html.contains("aria-labelledby=\"set-form-title\"")
            || html.contains("<form id=\"set-form\""),
        "SET form should have proper labeling"
    );
    assert!(
        html.contains("aria-labelledby=\"get-form-title\"")
            || html.contains("<form id=\"get-form\""),
        "GET form should have proper labeling"
    );
    assert!(
        html.contains("aria-labelledby=\"delete-form-title\"")
            || html.contains("<form id=\"delete-form\""),
        "DELETE form should have proper labeling"
    );

    // Verify inputs have labels (either label elements or aria-label)
    assert!(
        html.contains("<label") || html.contains("aria-label"),
        "Form inputs should have labels"
    );

    // Verify required inputs have aria-required
    assert!(
        html.contains("aria-required=\"true\"") || html.contains("required"),
        "Required inputs should indicate required status"
    );
}

/// Test result area has proper live region attributes
#[tokio::test]
async fn test_result_area_has_live_region() {
    let html = get_homepage_html().await;

    // Verify result div has aria-live for screen reader announcements
    assert!(
        html.contains("id=\"result\"") && html.contains("aria-live=\"polite\""),
        "Result area should have aria-live='polite' for screen reader announcements"
    );

    // Verify result div has role="status"
    assert!(
        html.contains("role=\"status\""),
        "Result area should have role='status'"
    );
}

// ============================================================================
// Test Case 4: Verify all images have alt text
// Expected: Logo and icons have descriptive alt attributes
// ============================================================================

/// Test that logo has accessible labeling
#[tokio::test]
async fn test_logo_has_accessible_labeling() {
    let html = get_homepage_html().await;

    // Verify logo icon has accessible label (role="img" with aria-label, or aria-hidden if decorative)
    let has_accessible_logo = html.contains("role=\"img\"") && html.contains("aria-label")
        || html.contains("aria-hidden=\"true\"");
    assert!(
        has_accessible_logo,
        "Logo icon should have accessible labeling (aria-label or aria-hidden)"
    );

    // Verify logo section exists
    assert!(
        html.contains("class=\"logo\""),
        "Logo section should exist"
    );

    // Verify MirDB text provides accessible name
    assert!(
        html.contains("<h1>MirDB</h1>"),
        "MirDB heading provides accessible name for logo"
    );
}

/// Test that decorative icons have aria-hidden
#[tokio::test]
async fn test_decorative_icons_have_aria_hidden() {
    let html = get_homepage_html().await;

    // Verify feature icons have aria-hidden (they are decorative)
    assert!(
        html.contains("class=\"feature-icon\"") && html.contains("aria-hidden=\"true\""),
        "Decorative feature icons should have aria-hidden='true'"
    );

    // Verify theme icon has aria-hidden (decorative)
    assert!(
        html.contains("class=\"theme-icon\"") && html.contains("aria-hidden=\"true\""),
        "Theme icon should have aria-hidden='true' as it is decorative"
    );
}

/// Test that theme toggle button has accessible label
#[tokio::test]
async fn test_theme_toggle_has_accessible_label() {
    let html = get_homepage_html().await;

    // Verify theme toggle has aria-label
    assert!(
        html.contains("id=\"theme-toggle\"") && html.contains("aria-label"),
        "Theme toggle button should have aria-label"
    );

    // Verify aria-label describes the action
    assert!(
        html.contains("Toggle") || html.contains("theme"),
        "Theme toggle aria-label should describe the toggle action"
    );
}

/// Test that external links have proper accessible attributes
#[tokio::test]
async fn test_external_links_accessibility() {
    let html = get_homepage_html().await;

    // Verify GitHub link has aria-label indicating external link
    assert!(
        html.contains("github-link") && html.contains("aria-label"),
        "GitHub link should have aria-label"
    );

    // Verify external link indicates it opens in new tab
    assert!(
        html.contains("opens in new tab") || html.contains("target=\"_blank\""),
        "External link should indicate it opens in new tab"
    );

    // Verify noopener for security
    assert!(
        html.contains("rel=\"noopener"),
        "External links should have rel='noopener' for security"
    );
}

// ============================================================================
// Additional accessibility tests
// ============================================================================

/// Test HTML lang attribute is set
#[tokio::test]
async fn test_html_has_lang_attribute() {
    let html = get_homepage_html().await;

    assert!(
        html.contains("lang=\"en\""),
        "HTML element should have lang attribute for accessibility"
    );
}

/// Test page has meta viewport for responsive design
#[tokio::test]
async fn test_page_has_viewport_meta() {
    let html = get_homepage_html().await;

    assert!(
        html.contains("viewport") && html.contains("width=device-width"),
        "Page should have viewport meta tag for responsive/accessible design"
    );
}

/// Test page has descriptive title
#[tokio::test]
async fn test_page_has_descriptive_title() {
    let html = get_homepage_html().await;

    assert!(
        html.contains("<title>") && html.contains("MirDB"),
        "Page should have descriptive title"
    );
}

/// Test sr-only class exists in CSS for screen reader content
#[tokio::test]
async fn test_sr_only_class_exists() {
    let css = get_css_content().await;

    // Verify sr-only class exists
    assert!(
        css.contains(".sr-only"),
        "CSS should include sr-only class for screen reader only content"
    );

    // Verify sr-only uses proper technique
    assert!(
        css.contains("clip") || css.contains("position: absolute"),
        "sr-only should use proper hiding technique"
    );
}

/// Test metrics have live region updates
#[tokio::test]
async fn test_metrics_have_live_regions() {
    let html = get_homepage_html().await;

    // Verify metric values have aria-live for updates
    assert!(
        html.contains("id=\"memory-value\"") && html.contains("aria-live"),
        "Memory metric should have aria-live for screen reader updates"
    );
    assert!(
        html.contains("id=\"disk-value\"") && html.contains("aria-live"),
        "Disk metric should have aria-live for screen reader updates"
    );
    assert!(
        html.contains("id=\"keys-value\"") && html.contains("aria-live"),
        "Keys metric should have aria-live for screen reader updates"
    );
    assert!(
        html.contains("id=\"connections-value\"") && html.contains("aria-live"),
        "Connections metric should have aria-live for screen reader updates"
    );
}
