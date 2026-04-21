//! Responsive Design tests.
//! Owner: Scenario 14 - Responsive Design
//!
//! Tests:
//! 1. Desktop viewport (1920x1080): Full layout visible, metric cards in horizontal row
//! 2. Mobile viewport (375x667): Layout stacks vertically, all content accessible
//! 3. Try It Out section on mobile: Forms usable, buttons tappable, results visible
//! 4. Browser compatibility: Manual test (Chrome, Firefox, Safari, Edge)
//!
//! REQ-8: Homepage must be responsive and display correctly on desktop and mobile browsers

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
async fn get_stylesheet() -> String {
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

/// Test Case 1: View homepage at 1920x1080 viewport
/// Expected: Full layout visible, metric cards in horizontal row
#[tokio::test]
async fn test_desktop_viewport_layout() {
    let css = get_stylesheet().await;
    let html = get_homepage_html().await;

    // Verify CSS has desktop breakpoint (1200px+)
    assert!(
        css.contains("@media (min-width: 1200px)"),
        "CSS should have desktop breakpoint (1200px+)"
    );

    // Verify desktop layout shows 4 metric cards in a row
    assert!(
        css.contains("grid-template-columns: repeat(4, 1fr)"),
        "Desktop layout should show 4 metric cards in horizontal row"
    );

    // Verify metrics grid exists in HTML
    assert!(
        html.contains("class=\"metrics-grid\""),
        "HTML should have metrics-grid class for metric cards"
    );

    // Verify all 4 metric cards exist
    assert!(
        html.contains("id=\"memory-card\""),
        "Memory metric card should exist"
    );
    assert!(
        html.contains("id=\"disk-card\""),
        "Disk metric card should exist"
    );
    assert!(
        html.contains("id=\"keys-card\""),
        "Keys metric card should exist"
    );
    assert!(
        html.contains("id=\"connections-card\""),
        "Connections metric card should exist"
    );

    // Verify operation forms show 3 in a row on desktop
    assert!(
        css.contains("operation-forms") && css.contains("repeat(3, 1fr)"),
        "Operation forms should be in 3-column layout on desktop"
    );

    // Verify full header layout
    assert!(
        html.contains("class=\"header-content\""),
        "Header content wrapper should exist"
    );
    assert!(
        html.contains("class=\"logo\"") && html.contains("class=\"nav\""),
        "Logo and nav should exist for desktop layout"
    );
}

/// Test Case 2: View homepage at 375x667 mobile viewport
/// Expected: Layout stacks vertically, all content accessible via scroll
#[tokio::test]
async fn test_mobile_viewport_layout_stacks_vertically() {
    let css = get_stylesheet().await;
    let html = get_homepage_html().await;

    // Verify CSS has mobile breakpoint (max-width: 479px)
    assert!(
        css.contains("@media (max-width: 479px)"),
        "CSS should have mobile breakpoint (479px)"
    );

    // Verify mobile breakpoint (480-767px) exists
    assert!(
        css.contains("@media (min-width: 480px) and (max-width: 767px)"),
        "CSS should have mobile landscape breakpoint"
    );

    // Verify metrics grid stacks vertically on small mobile
    assert!(
        css.contains("@media (max-width: 479px)") && css.contains("metrics-grid"),
        "Mobile CSS should style metrics-grid"
    );

    // Verify the CSS contains single-column grid for mobile
    let mobile_section_start = css.find("@media (max-width: 479px)").unwrap_or(0);
    let mobile_section = &css[mobile_section_start..];
    assert!(
        mobile_section.contains("grid-template-columns: 1fr"),
        "Mobile layout should use single-column grid for vertical stacking"
    );

    // Verify hero actions stack vertically on mobile
    assert!(
        css.contains("hero-actions") && css.contains("flex-direction: column"),
        "Hero actions should stack vertically on mobile"
    );

    // Verify all main sections exist for scrollable content
    assert!(
        html.contains("class=\"hero\""),
        "Hero section should exist for scroll access"
    );
    assert!(
        html.contains("class=\"features\""),
        "Features section should exist for scroll access"
    );
    assert!(
        html.contains("id=\"dashboard\""),
        "Dashboard section should exist for scroll access"
    );
    assert!(
        html.contains("id=\"try-it-out\""),
        "Try It Out section should exist for scroll access"
    );
    assert!(
        html.contains("id=\"protocol\""),
        "Protocol Reference section should exist for scroll access"
    );

    // Verify viewport meta tag exists for proper mobile rendering
    assert!(
        html.contains("viewport") && html.contains("width=device-width"),
        "HTML should have viewport meta tag for mobile"
    );
}

/// Test Case 3: Use Try It Out section on mobile
/// Expected: Forms usable, buttons tappable, results visible
#[tokio::test]
async fn test_try_it_out_section_mobile_usability() {
    let css = get_stylesheet().await;
    let html = get_homepage_html().await;

    // Verify Try It Out section exists
    assert!(
        html.contains("id=\"try-it-out\""),
        "Try It Out section should exist"
    );

    // Verify all three operation forms exist
    assert!(
        html.contains("id=\"set-form\""),
        "SET form should exist"
    );
    assert!(
        html.contains("id=\"get-form\""),
        "GET form should exist"
    );
    assert!(
        html.contains("id=\"delete-form\""),
        "DELETE form should exist"
    );

    // Verify forms have proper input elements
    assert!(
        html.contains("<input type=\"text\""),
        "Forms should have text input fields"
    );

    // Verify buttons exist for form submission
    assert!(
        html.contains("type=\"submit\"") && html.contains("class=\"btn"),
        "Forms should have submit buttons"
    );

    // Verify result area exists for displaying operation results
    assert!(
        html.contains("id=\"result\""),
        "Result display area should exist"
    );

    // Verify CSS has touch-friendly tap targets (min-height: 44-48px)
    assert!(
        css.contains("min-height: 44px") || css.contains("min-height: 48px"),
        "Mobile CSS should have touch-friendly tap targets (44-48px minimum)"
    );

    // Verify input font-size is 16px to prevent iOS zoom
    assert!(
        css.contains("font-size: 16px"),
        "Mobile inputs should have 16px font-size to prevent iOS zoom"
    );

    // Verify operation forms stack vertically on mobile
    let mobile_section_start = css.find("@media (max-width: 479px)").unwrap_or(0);
    let mobile_section = &css[mobile_section_start..];
    assert!(
        mobile_section.contains("operation-forms"),
        "Mobile CSS should style operation-forms"
    );

    // Verify result area is scrollable and has proper styling
    assert!(
        css.contains("result") && css.contains("overflow"),
        "Result area should handle overflow for long content"
    );

    // Verify inputs have aria-labels for accessibility
    assert!(
        html.contains("aria-label=\"Key\"") || html.contains("aria-label="),
        "Inputs should have aria-labels for mobile screen reader users"
    );

    // Verify buttons are full-width on mobile
    assert!(
        css.contains("operation-form .btn") && css.contains("width: 100%"),
        "Form buttons should be full-width on mobile for easy tapping"
    );
}

/// Additional test: Verify tablet viewport breakpoints exist
#[tokio::test]
async fn test_tablet_viewport_breakpoints() {
    let css = get_stylesheet().await;

    // Verify tablet breakpoint (768px - 1023px)
    assert!(
        css.contains("@media (min-width: 768px) and (max-width: 1023px)"),
        "CSS should have tablet breakpoint (768px - 1023px)"
    );

    // Verify tablet shows 2-column layout for metrics
    let tablet_start = css.find("@media (min-width: 768px) and (max-width: 1023px)").unwrap_or(0);
    let tablet_section = &css[tablet_start..];
    assert!(
        tablet_section.contains("repeat(2, 1fr)"),
        "Tablet layout should use 2-column grid"
    );
}

/// Additional test: Verify responsive CSS uses proper techniques
#[tokio::test]
async fn test_responsive_css_techniques() {
    let css = get_stylesheet().await;

    // Verify CSS uses CSS Grid for responsive layouts
    assert!(
        css.contains("display: grid") || css.contains("grid-template-columns"),
        "CSS should use CSS Grid for responsive layouts"
    );

    // Verify CSS uses auto-fit/auto-fill with minmax for responsive grids
    assert!(
        css.contains("auto-fit") && css.contains("minmax"),
        "CSS should use auto-fit with minmax for responsive grids"
    );

    // Verify CSS uses flexbox for flexible layouts
    assert!(
        css.contains("display: flex"),
        "CSS should use flexbox for flexible layouts"
    );

    // Verify CSS uses flex-wrap for responsive wrapping
    assert!(
        css.contains("flex-wrap: wrap") || css.contains("flex-wrap"),
        "CSS should use flex-wrap for responsive wrapping"
    );

    // Verify CSS has box-sizing: border-box for consistent sizing
    assert!(
        css.contains("box-sizing: border-box"),
        "CSS should use box-sizing: border-box"
    );
}

/// Additional test: Verify HTML structure supports responsive design
#[tokio::test]
async fn test_html_responsive_structure() {
    let html = get_homepage_html().await;

    // Verify HTML5 doctype
    assert!(
        html.contains("<!DOCTYPE html>"),
        "Should have HTML5 doctype"
    );

    // Verify viewport meta tag with proper attributes
    assert!(
        html.contains("width=device-width") && html.contains("initial-scale=1"),
        "Viewport meta should have device-width and initial-scale=1"
    );

    // Verify charset is set
    assert!(
        html.contains("charset=\"UTF-8\""),
        "Should have UTF-8 charset for proper rendering"
    );

    // Verify CSS is loaded
    assert!(
        html.contains("<link rel=\"stylesheet\""),
        "CSS stylesheet should be linked"
    );

    // Verify semantic HTML elements exist
    assert!(
        html.contains("<header") && html.contains("<main") && html.contains("<footer"),
        "Should have semantic HTML structure"
    );

    // Verify sections have proper IDs for navigation
    assert!(
        html.contains("id=\"dashboard\"") && html.contains("id=\"try-it-out\""),
        "Sections should have IDs for smooth scroll navigation"
    );
}

/// Additional test: Verify overflow handling for mobile
#[tokio::test]
async fn test_mobile_overflow_handling() {
    let css = get_stylesheet().await;

    // Verify CSS prevents horizontal overflow on mobile
    assert!(
        css.contains("overflow-x: hidden"),
        "Mobile CSS should prevent horizontal overflow"
    );

    // Verify body/html don't cause horizontal scroll
    let mobile_media_start = css.find("@media (max-width: 767px)").unwrap_or(0);
    let mobile_section = &css[mobile_media_start..];
    assert!(
        mobile_section.contains("overflow-x: hidden"),
        "Mobile CSS should prevent horizontal scroll on html/body"
    );

    // Verify word-break is handled for long content
    assert!(
        css.contains("word-break"),
        "CSS should handle word-break for long content"
    );
}

/// Additional test: Verify configuration panel is responsive
#[tokio::test]
async fn test_config_panel_responsive() {
    let css = get_stylesheet().await;
    let html = get_homepage_html().await;

    // Verify config panel exists
    assert!(
        html.contains("class=\"config-panel\""),
        "Config panel should exist"
    );

    // Verify config grid exists
    assert!(
        html.contains("class=\"config-grid\""),
        "Config grid should exist"
    );

    // Verify config items exist
    assert!(
        html.contains("class=\"config-item\""),
        "Config items should exist"
    );

    // Verify config grid is responsive
    assert!(
        css.contains("config-grid") && css.contains("grid-template-columns"),
        "Config grid should be responsive"
    );

    // Verify mobile config layout stacks to single column
    let mobile_section_start = css.find("@media (max-width: 479px)").unwrap_or(0);
    let mobile_section = &css[mobile_section_start..];
    assert!(
        mobile_section.contains("config-grid"),
        "Mobile CSS should style config-grid"
    );
}

/// Additional test: Verify feature cards are responsive
#[tokio::test]
async fn test_feature_cards_responsive() {
    let css = get_stylesheet().await;
    let html = get_homepage_html().await;

    // Verify feature grid exists
    assert!(
        html.contains("class=\"feature-grid\""),
        "Feature grid should exist"
    );

    // Verify feature cards exist
    assert!(
        html.contains("class=\"feature-card\""),
        "Feature cards should exist"
    );

    // Verify feature grid uses responsive auto-fit
    assert!(
        css.contains("feature-grid") && css.contains("auto-fit"),
        "Feature grid should use auto-fit for responsive layout"
    );

    // Verify feature grid has appropriate minmax
    assert!(
        css.contains("minmax(280px"),
        "Feature cards should have minimum width for readability"
    );
}

/// Additional test: Verify print styles exist
#[tokio::test]
async fn test_print_media_query() {
    let css = get_stylesheet().await;

    // Verify print media query exists
    assert!(
        css.contains("@media print"),
        "CSS should have print media query"
    );

    // Verify non-essential elements are hidden in print
    assert!(
        css.contains("@media print") && css.contains("display: none"),
        "Print CSS should hide non-essential elements"
    );
}
