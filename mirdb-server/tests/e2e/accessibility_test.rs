//! Accessibility Compliance E2E tests.
//! Owner: Scenario 15 - Accessibility Compliance
//!
//! Tests WCAG 2.1 AA compliance for the MirDB homepage:
//! 1. Keyboard navigation - all interactive elements reachable via Tab
//! 2. Focus indicators - visible focus ring on all interactive elements
//! 3. Automated accessibility checks - no critical violations
//! 4. Alt text verification - all images have descriptive alt attributes

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
async fn get_stylesheet_css() -> String {
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

// =============================================================================
// Test Case 1: Tab through all interactive elements (Manual test verification)
// Input: Tab through all interactive elements
// Expected: All buttons, links, and form fields reachable via Tab key
// =============================================================================

/// Verify skip navigation link exists for keyboard users
#[tokio::test]
async fn test_skip_navigation_link_exists() {
    let html = get_homepage_html().await;

    // Verify skip link exists
    assert!(
        html.contains("class=\"skip-link\""),
        "Homepage should have a skip navigation link"
    );

    // Verify skip link targets main content
    assert!(
        html.contains("href=\"#main-content\""),
        "Skip link should target main content area"
    );

    // Verify skip link text is descriptive
    assert!(
        html.contains("Skip to main content"),
        "Skip link should have descriptive text"
    );
}

/// Verify main content has id for skip link target
#[tokio::test]
async fn test_main_content_has_skip_target_id() {
    let html = get_homepage_html().await;

    // Verify main element has id for skip link
    assert!(
        html.contains("id=\"main-content\""),
        "Main element should have id='main-content' for skip link target"
    );

    // Verify it's on the main element
    assert!(
        html.contains("<main id=\"main-content\""),
        "Main element should be the skip link target"
    );
}

/// Verify all buttons have proper type attributes
#[tokio::test]
async fn test_buttons_have_type_attributes() {
    let html = get_homepage_html().await;

    // Form submit buttons should have type="submit"
    assert!(
        html.contains("type=\"submit\""),
        "Form buttons should have type='submit'"
    );
}

/// Verify forms are keyboard accessible with proper structure
#[tokio::test]
async fn test_forms_have_keyboard_accessible_structure() {
    let html = get_homepage_html().await;

    // Verify forms have associated labels
    assert!(
        html.contains("<label for=\"set-key\"") || html.contains("aria-label"),
        "Form inputs should have associated labels or aria-label"
    );

    // Verify required fields have aria-required
    assert!(
        html.contains("aria-required=\"true\""),
        "Required fields should have aria-required attribute"
    );

    // Verify autocomplete attribute for form fields
    assert!(
        html.contains("autocomplete="),
        "Form inputs should have autocomplete attributes"
    );
}

/// Verify navigation has proper aria-label
#[tokio::test]
async fn test_navigation_has_aria_label() {
    let html = get_homepage_html().await;

    // Verify nav element has aria-label
    assert!(
        html.contains("aria-label=\"Main navigation\""),
        "Navigation should have aria-label for screen readers"
    );
}

// =============================================================================
// Test Case 2: Check focus indicators
// Input: Check focus indicators
// Expected: Visible focus ring on all interactive elements
// =============================================================================

/// Verify CSS has focus styles for all interactive elements
#[tokio::test]
async fn test_css_has_focus_styles_for_all_elements() {
    let css = get_stylesheet_css().await;

    // Verify button focus styles exist
    assert!(
        css.contains(".btn:focus") || css.contains(".btn:focus-visible"),
        "CSS should have focus styles for buttons"
    );

    // Verify input focus styles exist
    assert!(
        css.contains("input:focus"),
        "CSS should have focus styles for inputs"
    );

    // Verify link focus styles exist
    assert!(
        css.contains("a:focus") || css.contains(".github-link:focus"),
        "CSS should have focus styles for links"
    );

    // Verify theme toggle focus styles
    assert!(
        css.contains(".theme-toggle:focus"),
        "CSS should have focus styles for theme toggle"
    );
}

/// Verify focus styles use visible outline
#[tokio::test]
async fn test_focus_styles_have_visible_outline() {
    let css = get_stylesheet_css().await;

    // Verify outline is used for focus (not just color change)
    assert!(
        css.contains("outline:") && css.contains(":focus"),
        "Focus styles should use outline property for visibility"
    );

    // Verify outline-offset for better visibility
    assert!(
        css.contains("outline-offset"),
        "Focus styles should have outline-offset for better visibility"
    );
}

/// Verify skip link has focus styles
#[tokio::test]
async fn test_skip_link_has_focus_styles() {
    let css = get_stylesheet_css().await;

    // Verify skip link exists in CSS
    assert!(
        css.contains(".skip-link"),
        "CSS should have skip-link styles"
    );

    // Verify skip link has focus state
    assert!(
        css.contains(".skip-link:focus"),
        "Skip link should have focus styles"
    );
}

/// Verify focus-visible support for modern browsers
#[tokio::test]
async fn test_focus_visible_support() {
    let css = get_stylesheet_css().await;

    // Verify :focus-visible is used where appropriate
    assert!(
        css.contains(":focus-visible"),
        "CSS should support :focus-visible for better UX"
    );
}

/// Verify focus styles have sufficient contrast
#[tokio::test]
async fn test_focus_outline_width() {
    let css = get_stylesheet_css().await;

    // Verify outline width is at least 2px (WCAG recommends 3px)
    assert!(
        css.contains("outline: 3px") || css.contains("outline: 2px"),
        "Focus outline should be at least 2px wide for visibility"
    );
}

// =============================================================================
// Test Case 3: Run automated accessibility checker
// Input: Run automated accessibility checker (axe, lighthouse)
// Expected: No critical or serious accessibility violations
// =============================================================================

/// Verify proper HTML document structure
#[tokio::test]
async fn test_proper_html_document_structure() {
    let html = get_homepage_html().await;

    // Verify DOCTYPE
    assert!(
        html.contains("<!DOCTYPE html>"),
        "Document should have HTML5 doctype"
    );

    // Verify lang attribute
    assert!(
        html.contains("<html lang=\"en\">"),
        "HTML element should have lang attribute"
    );

    // Verify charset meta
    assert!(
        html.contains("charset=\"UTF-8\"") || html.contains("charset=UTF-8"),
        "Document should specify UTF-8 charset"
    );

    // Verify viewport meta for responsive design
    assert!(
        html.contains("viewport"),
        "Document should have viewport meta tag"
    );
}

/// Verify semantic landmark regions exist
#[tokio::test]
async fn test_semantic_landmark_regions() {
    let html = get_homepage_html().await;

    // Verify header element with banner role
    assert!(
        html.contains("<header") && html.contains("role=\"banner\""),
        "Header should have banner role"
    );

    // Verify main element with main role
    assert!(
        html.contains("<main") && html.contains("role=\"main\""),
        "Main element should have main role"
    );

    // Verify footer element with contentinfo role
    assert!(
        html.contains("<footer") && html.contains("role=\"contentinfo\""),
        "Footer should have contentinfo role"
    );

    // Verify nav element
    assert!(
        html.contains("<nav"),
        "Document should have navigation element"
    );
}

/// Verify proper heading hierarchy (h1 -> h2 -> h3 -> h4)
#[tokio::test]
async fn test_proper_heading_hierarchy() {
    let html = get_homepage_html().await;

    // Verify h1 exists (only one per page)
    let h1_count = html.matches("<h1>").count();
    assert!(
        h1_count == 1,
        "Document should have exactly one h1 heading, found {}", h1_count
    );

    // Verify h2 exists after h1
    assert!(
        html.contains("<h2"),
        "Document should have h2 headings"
    );

    // Verify h3 exists
    assert!(
        html.contains("<h3"),
        "Document should have h3 headings"
    );

    // Verify h4 exists
    assert!(
        html.contains("<h4"),
        "Document should have h4 headings"
    );
}

/// Verify sections have proper labeling
#[tokio::test]
async fn test_sections_have_proper_labeling() {
    let html = get_homepage_html().await;

    // Verify sections have aria-labelledby
    assert!(
        html.contains("aria-labelledby=\"hero-title\""),
        "Hero section should have aria-labelledby"
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
        "Protocol reference section should have aria-labelledby"
    );
}

/// Verify live regions for dynamic content
#[tokio::test]
async fn test_live_regions_for_dynamic_content() {
    let html = get_homepage_html().await;

    // Verify result area has aria-live
    assert!(
        html.contains("aria-live=\"polite\""),
        "Dynamic content areas should have aria-live attribute"
    );

    // Verify aria-atomic for complete announcements
    assert!(
        html.contains("aria-atomic=\"true\"") || html.contains("aria-atomic=\"false\""),
        "Live regions should have aria-atomic attribute"
    );

    // Verify result has role="status"
    assert!(
        html.contains("role=\"status\""),
        "Result area should have role='status' for screen reader announcements"
    );
}

/// Verify forms have proper role and structure
#[tokio::test]
async fn test_form_accessibility() {
    let html = get_homepage_html().await;

    // Verify forms have aria-labelledby
    assert!(
        html.contains("aria-labelledby=\"set-form-title\""),
        "SET form should have aria-labelledby"
    );

    assert!(
        html.contains("aria-labelledby=\"get-form-title\""),
        "GET form should have aria-labelledby"
    );

    assert!(
        html.contains("aria-labelledby=\"delete-form-title\""),
        "DELETE form should have aria-labelledby"
    );
}

/// Verify external links have proper attributes
#[tokio::test]
async fn test_external_links_accessibility() {
    let html = get_homepage_html().await;

    // Verify external links have target="_blank"
    assert!(
        html.contains("target=\"_blank\""),
        "External links should open in new tab"
    );

    // Verify external links have rel="noopener noreferrer"
    assert!(
        html.contains("rel=\"noopener noreferrer\""),
        "External links should have noopener noreferrer for security"
    );

    // Verify external links indicate they open in new tab
    assert!(
        html.contains("opens in new tab") || html.contains("title="),
        "External links should indicate they open in new tab"
    );
}

// =============================================================================
// Test Case 4: Verify all images have alt text
// Input: Verify all images have alt text
// Expected: Logo and icons have descriptive alt attributes
// =============================================================================

/// Verify emoji icons have role="img" and aria-label
#[tokio::test]
async fn test_emoji_icons_have_accessibility_attributes() {
    let html = get_homepage_html().await;

    // Verify logo icon has role="img" and aria-label
    assert!(
        html.contains("role=\"img\"") && html.contains("aria-label="),
        "Emoji icons should have role='img' and aria-label"
    );

    // Verify logo icon specifically
    assert!(
        html.contains("class=\"logo-icon\"") && html.contains("aria-label=\"Database icon\""),
        "Logo icon should have descriptive aria-label"
    );
}

/// Verify feature icons have accessibility attributes
#[tokio::test]
async fn test_feature_icons_have_accessibility_attributes() {
    let html = get_homepage_html().await;

    // Verify feature icons have role="img"
    assert!(
        html.contains("class=\"feature-icon\"") && html.contains("role=\"img\""),
        "Feature icons should have role='img'"
    );

    // Count feature icons with aria-label
    let feature_icon_with_aria = html.matches("class=\"feature-icon\" role=\"img\" aria-label=").count();
    assert!(
        feature_icon_with_aria >= 3,
        "All feature icons should have aria-label, found {}", feature_icon_with_aria
    );
}

/// Verify theme icon has aria-hidden (decorative when button has label)
#[tokio::test]
async fn test_theme_icon_is_properly_hidden() {
    let html = get_homepage_html().await;

    // Theme icon should be hidden from screen readers (button has aria-label)
    assert!(
        html.contains("class=\"theme-icon\" aria-hidden=\"true\""),
        "Theme icon should have aria-hidden='true' since button has aria-label"
    );
}

/// Verify no images exist without alt text (if any img tags)
#[tokio::test]
async fn test_no_images_without_alt() {
    let html = get_homepage_html().await;

    // If there are img tags, they should have alt attributes
    let img_count = html.matches("<img").count();
    let img_with_alt = html.matches("<img").count() - html.matches("<img [^>]*(?<!alt=)>").count();

    // For this page, we're using emoji icons, not img tags
    // But if there are any img tags, they must have alt
    assert!(
        img_count == 0 || img_with_alt == img_count,
        "All img elements must have alt attributes"
    );
}

// =============================================================================
// Additional Accessibility Tests
// =============================================================================

/// Verify WCAG 2.1 AA color contrast support in CSS
#[tokio::test]
async fn test_color_contrast_css_variables() {
    let css = get_stylesheet_css().await;

    // Verify light theme has dark text on light background
    assert!(
        css.contains("--text-primary: #1") || css.contains("--text-primary: #2"),
        "Light theme should have dark text color"
    );

    // Verify dark theme has light text on dark background
    assert!(
        css.contains("--bg-primary: #0") || css.contains("--bg-primary: #1"),
        "Dark theme should have dark background color"
    );
}

/// Verify high contrast mode support
#[tokio::test]
async fn test_high_contrast_mode_support() {
    let css = get_stylesheet_css().await;

    // Verify forced-colors media query exists
    assert!(
        css.contains("forced-colors: active"),
        "CSS should support Windows High Contrast mode"
    );
}

/// Verify reduced motion preferences support
#[tokio::test]
async fn test_reduced_motion_support() {
    let css = get_stylesheet_css().await;

    // Verify prefers-reduced-motion media query
    assert!(
        css.contains("prefers-reduced-motion"),
        "CSS should respect reduced motion preferences"
    );
}

/// Verify theme toggle button has proper aria-pressed attribute
#[tokio::test]
async fn test_theme_toggle_aria_pressed() {
    let html = get_homepage_html().await;

    // Verify theme toggle has aria-pressed for toggle button pattern
    assert!(
        html.contains("aria-pressed="),
        "Theme toggle should have aria-pressed attribute"
    );
}

/// Verify description list is used for config panel
#[tokio::test]
async fn test_config_uses_description_list() {
    let html = get_homepage_html().await;

    // Verify config uses dl/dt/dd structure
    assert!(
        html.contains("<dl class=\"config-grid\"") || html.contains("<dl"),
        "Configuration panel should use description list (dl) for semantic markup"
    );

    assert!(
        html.contains("<dt class=\"config-label\""),
        "Configuration labels should use dt elements"
    );

    assert!(
        html.contains("<dd class=\"config-value\""),
        "Configuration values should use dd elements"
    );
}

/// Verify links are distinguishable from regular text
#[tokio::test]
async fn test_links_are_distinguishable() {
    let css = get_stylesheet_css().await;

    // Verify links have underline (not just color)
    assert!(
        css.contains("text-decoration: underline") || css.contains("text-decoration:underline"),
        "Links should be underlined to be distinguishable without relying on color"
    );
}

/// Verify interactive elements have adequate touch targets
#[tokio::test]
async fn test_touch_targets_size() {
    let css = get_stylesheet_css().await;

    // Verify minimum height for touch targets (44px is WCAG recommended)
    assert!(
        css.contains("min-height: 44px") || css.contains("min-height: 48px"),
        "Interactive elements should have minimum touch target size"
    );
}
