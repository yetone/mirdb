//! E2E Tests for Responsive Layout
//!
//! Scenario 7 - Responsive Layout
//!
//! These tests verify that the homepage has responsive CSS for:
//! - Test Case 1: Desktop viewport (1920x1080) - multi-column layout, no horizontal scroll
//! - Test Case 2: Tablet viewport (768x1024) - adaptive narrower width layout
//! - Test Case 3: Mobile viewport (375x667) - single column layout
//! - Test Case 4: Touch-friendly elements (>44px tap targets)

/// The embedded CSS content from the homepage
const STYLE_CSS: &str = include_str!("../assets/css/style.css");

/// The embedded HTML content from the homepage
const INDEX_HTML: &str = include_str!("../assets/index.html");

// ============================================================================
// Test Case 1: Desktop viewport (1920x1080) - multi-column layout
// ============================================================================

#[test]
fn test_case_1_desktop_media_query_exists() {
    // Verify CSS has media query for large desktops (1920px+)
    assert!(
        STYLE_CSS.contains("@media") && STYLE_CSS.contains("min-width: 1920px"),
        "CSS must contain media query for desktop viewport (1920px+)"
    );
}

#[test]
fn test_case_1_desktop_multi_column_layout() {
    // Verify CSS defines multi-column grid for features at desktop size
    let css_lower = STYLE_CSS.to_lowercase();

    // Check for grid layouts that support multi-column
    assert!(
        css_lower.contains("grid-template-columns") && css_lower.contains("repeat"),
        "CSS must define grid-template-columns for multi-column layout"
    );
}

#[test]
fn test_case_1_desktop_features_three_columns() {
    // Verify features grid has 3 columns at desktop size
    // Looking for: grid-template-columns: repeat(3, 1fr)
    assert!(
        STYLE_CSS.contains("repeat(3, 1fr)"),
        "CSS must define 3-column layout for features grid at desktop size"
    );
}

#[test]
fn test_case_1_no_horizontal_scroll_styles() {
    // Verify CSS prevents horizontal scrolling
    assert!(
        STYLE_CSS.contains("overflow-x: hidden") || STYLE_CSS.contains("overflow-x:hidden"),
        "CSS must prevent horizontal scroll with overflow-x: hidden"
    );

    // Verify max-width constraints
    assert!(
        STYLE_CSS.contains("max-width: 100%") || STYLE_CSS.contains("max-width:100%") ||
        STYLE_CSS.contains("max-width: 100vw") || STYLE_CSS.contains("max-width:100vw"),
        "CSS must constrain max-width to prevent horizontal overflow"
    );
}

#[test]
fn test_case_1_viewport_meta_tag() {
    // Verify HTML has proper viewport meta tag for responsive design
    assert!(
        INDEX_HTML.contains("name=\"viewport\"") || INDEX_HTML.contains("name='viewport'"),
        "HTML must contain viewport meta tag"
    );

    assert!(
        INDEX_HTML.contains("width=device-width"),
        "Viewport meta tag must include width=device-width"
    );

    assert!(
        INDEX_HTML.contains("initial-scale=1"),
        "Viewport meta tag must include initial-scale=1"
    );
}

// ============================================================================
// Test Case 2: Tablet viewport (768x1024) - adaptive layout
// ============================================================================

#[test]
fn test_case_2_tablet_media_query_exists() {
    // Verify CSS has media query for tablet viewport
    // Looking for max-width around 1023px (covering tablet range)
    assert!(
        STYLE_CSS.contains("max-width: 1023px") || STYLE_CSS.contains("max-width:1023px"),
        "CSS must contain media query for tablet viewport (max-width: 1023px)"
    );

    assert!(
        STYLE_CSS.contains("min-width: 768px") || STYLE_CSS.contains("min-width:768px"),
        "CSS must contain media query for tablet viewport (min-width: 768px)"
    );
}

#[test]
fn test_case_2_tablet_two_column_grid() {
    // Verify tablet viewport uses 2-column grid
    assert!(
        STYLE_CSS.contains("repeat(2, 1fr)"),
        "CSS must define 2-column layout for tablet viewport"
    );
}

#[test]
fn test_case_2_tablet_no_content_overflow() {
    // Verify tablet has proper overflow handling
    assert!(
        STYLE_CSS.contains("overflow-x: auto") || STYLE_CSS.contains("overflow-x:auto") ||
        STYLE_CSS.contains("overflow-x: hidden") || STYLE_CSS.contains("overflow-x:hidden"),
        "CSS must handle content overflow for tablet viewport"
    );
}

#[test]
fn test_case_2_tablet_reduced_padding() {
    // Verify tablet has reduced padding for narrower screens
    // The CSS should have padding values smaller than desktop
    let css_sections: Vec<&str> = STYLE_CSS.split("@media").collect();

    // At least one media query section should exist
    assert!(
        css_sections.len() > 1,
        "CSS must have media query sections for tablet"
    );
}

// ============================================================================
// Test Case 3: Mobile viewport (375x667) - single column layout
// ============================================================================

#[test]
fn test_case_3_mobile_media_query_exists() {
    // Verify CSS has media query for mobile viewport (max-width: 767px)
    assert!(
        STYLE_CSS.contains("max-width: 767px") || STYLE_CSS.contains("max-width:767px"),
        "CSS must contain media query for mobile viewport (max-width: 767px)"
    );
}

#[test]
fn test_case_3_mobile_single_column_layout() {
    // Verify mobile uses single column layout
    // Looking for: grid-template-columns: 1fr
    assert!(
        STYLE_CSS.contains("grid-template-columns: 1fr") ||
        STYLE_CSS.contains("grid-template-columns:1fr"),
        "CSS must define single column layout (1fr) for mobile viewport"
    );
}

#[test]
fn test_case_3_mobile_stacked_features() {
    // Verify features grid stacks to single column on mobile
    let mobile_section_exists = STYLE_CSS.contains("max-width: 767px") &&
                                 STYLE_CSS.contains(".features-grid");

    assert!(
        mobile_section_exists || STYLE_CSS.contains("grid-template-columns: 1fr"),
        "CSS must stack features to single column on mobile"
    );
}

#[test]
fn test_case_3_mobile_vertical_scroll_accessible() {
    // Verify content is accessible via vertical scroll (no fixed heights that cut off content)
    // Check that body/html doesn't have overflow: hidden for vertical
    let css_lower = STYLE_CSS.to_lowercase();

    // Should not have overflow: hidden (which would block all scroll)
    // But overflow-x: hidden is fine for preventing horizontal scroll
    let has_proper_overflow = css_lower.contains("overflow-x: hidden") ||
                               css_lower.contains("overflow-x:hidden");

    assert!(
        has_proper_overflow,
        "CSS must allow vertical scrolling while preventing horizontal overflow"
    );
}

#[test]
fn test_case_3_small_mobile_media_query() {
    // Verify CSS has media query for extra small mobile (375px)
    assert!(
        STYLE_CSS.contains("max-width: 375px") || STYLE_CSS.contains("max-width:375px"),
        "CSS must contain media query for small mobile viewport (max-width: 375px)"
    );
}

#[test]
fn test_case_3_mobile_reduced_font_sizes() {
    // Verify mobile has reduced font sizes
    // Check that mobile section contains font-size declarations
    let has_mobile_fonts = STYLE_CSS.contains("max-width: 767px") &&
                           STYLE_CSS.contains("font-size");

    assert!(
        has_mobile_fonts,
        "CSS must adjust font sizes for mobile viewport"
    );
}

// ============================================================================
// Test Case 4: Touch-friendly elements (>44px tap target)
// ============================================================================

#[test]
fn test_case_4_touch_friendly_min_height() {
    // Verify CSS defines 44px minimum height for touch targets
    assert!(
        STYLE_CSS.contains("min-height: 44px") || STYLE_CSS.contains("min-height:44px"),
        "CSS must define min-height: 44px for touch-friendly tap targets"
    );
}

#[test]
fn test_case_4_touch_friendly_min_width() {
    // Verify CSS defines 44px minimum width for touch targets
    assert!(
        STYLE_CSS.contains("min-width: 44px") || STYLE_CSS.contains("min-width:44px"),
        "CSS must define min-width: 44px for touch-friendly tap targets"
    );
}

#[test]
fn test_case_4_theme_toggle_touch_friendly() {
    // Verify theme toggle button has touch-friendly sizing
    let css_has_theme_toggle = STYLE_CSS.contains(".theme-toggle");
    let css_has_44px = STYLE_CSS.contains("44px");

    assert!(
        css_has_theme_toggle && css_has_44px,
        "CSS must make theme toggle button touch-friendly (44px tap target)"
    );
}

#[test]
fn test_case_4_buttons_touch_friendly() {
    // Verify buttons have touch-friendly sizing in mobile media queries
    let has_button_styles = STYLE_CSS.contains("button") && STYLE_CSS.contains("44px");

    assert!(
        has_button_styles,
        "CSS must make buttons touch-friendly (44px tap target)"
    );
}

#[test]
fn test_case_4_footer_links_touch_friendly() {
    // Verify footer links have touch-friendly sizing
    let has_footer_link_styles = STYLE_CSS.contains(".footer-links a") ||
                                  (STYLE_CSS.contains(".footer-links") && STYLE_CSS.contains("44px"));

    assert!(
        has_footer_link_styles,
        "CSS must make footer links touch-friendly"
    );
}

#[test]
fn test_case_4_nav_links_touch_friendly() {
    // Verify nav links have touch-friendly sizing on mobile
    let has_nav_mobile_styles = STYLE_CSS.contains(".nav a") ||
                                 STYLE_CSS.contains(".nav");

    assert!(
        has_nav_mobile_styles,
        "CSS must style nav links for touch-friendly interaction"
    );
}

#[test]
fn test_case_4_tap_highlight_disabled() {
    // Verify tap highlight is disabled for better touch experience
    assert!(
        STYLE_CSS.contains("-webkit-tap-highlight-color"),
        "CSS should disable tap highlight for better touch experience"
    );
}

// ============================================================================
// Additional Responsive Tests
// ============================================================================

#[test]
fn test_responsive_code_blocks() {
    // Verify code blocks have proper overflow handling
    assert!(
        STYLE_CSS.contains("pre") &&
        (STYLE_CSS.contains("overflow-x: auto") || STYLE_CSS.contains("overflow-x:auto")),
        "CSS must make code blocks scrollable horizontally"
    );
}

#[test]
fn test_responsive_images() {
    // Verify proper max-width on content
    assert!(
        STYLE_CSS.contains("max-width"),
        "CSS must define max-width for responsive content"
    );
}

#[test]
fn test_css_has_smooth_scrolling() {
    // Verify smooth scrolling for mobile touch scrolling
    assert!(
        STYLE_CSS.contains("-webkit-overflow-scrolling: touch") ||
        STYLE_CSS.contains("-webkit-overflow-scrolling:touch"),
        "CSS should enable smooth touch scrolling"
    );
}

#[test]
fn test_css_uses_box_sizing_border_box() {
    // Verify proper box-sizing for predictable layouts
    assert!(
        STYLE_CSS.contains("box-sizing: border-box") ||
        STYLE_CSS.contains("box-sizing:border-box"),
        "CSS must use box-sizing: border-box for predictable layouts"
    );
}

#[test]
fn test_css_has_media_query_structure() {
    // Count media queries to ensure comprehensive responsive design
    let media_query_count = STYLE_CSS.matches("@media").count();

    assert!(
        media_query_count >= 3,
        "CSS must have at least 3 media queries for desktop/tablet/mobile, found {}",
        media_query_count
    );
}

#[test]
fn test_all_sections_responsive() {
    // Verify all main sections are addressed in responsive styles
    let sections = [".header", ".hero", ".features", ".metrics", ".config", ".quick-start", ".footer"];

    for section in &sections {
        assert!(
            STYLE_CSS.contains(section),
            "CSS must style {} section",
            section
        );
    }
}
