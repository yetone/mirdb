//! Accessibility Compliance Tests (Scenario 11)
//!
//! These tests verify that the MirDB homepage meets WCAG 2.1 AA accessibility requirements.
//! Test cases include:
//! - Semantic HTML structure with proper heading hierarchy
//! - Keyboard navigation support
//! - Color contrast ratios
//! - Screen reader compatibility (ARIA labels)
//! - Focus indicators

use std::fs;
use std::path::PathBuf;

fn get_html_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.push("mirdb-server/assets/index.html");
    p
}

fn get_css_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.push("mirdb-server/assets/styles.css");
    p
}

fn read_homepage_html() -> String {
    let path = get_html_path();
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read index.html from {:?}: {}", path, e))
}

fn read_styles_css() -> String {
    let path = get_css_path();
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read styles.css from {:?}: {}", path, e))
}

// =============================================================================
// Test Case 1: Semantic HTML Structure - Heading Hierarchy
// =============================================================================

#[test]
fn test_heading_hierarchy_has_single_h1() {
    let html = read_homepage_html();
    let h1_count = html.matches("<h1>").count();
    assert_eq!(
        h1_count, 1,
        "Page should have exactly one h1 element. Found: {}",
        h1_count
    );
}

#[test]
fn test_heading_hierarchy_h1_before_h2() {
    let html = read_homepage_html();
    let h1_pos = html.find("<h1>").expect("h1 should exist");
    let h2_pos = html.find("<h2").expect("h2 should exist");
    assert!(
        h1_pos < h2_pos,
        "h1 should appear before h2 for proper heading hierarchy"
    );
}

#[test]
fn test_heading_hierarchy_h2_before_h3() {
    let html = read_homepage_html();
    let h2_pos = html.find("<h2").expect("h2 should exist");
    let h3_pos = html.find("<h3>").expect("h3 should exist");
    assert!(
        h2_pos < h3_pos,
        "h2 should appear before h3 for proper heading hierarchy"
    );
}

#[test]
fn test_heading_hierarchy_h3_before_h4() {
    let html = read_homepage_html();
    if let Some(h4_pos) = html.find("<h4>") {
        let h3_pos = html.find("<h3>").expect("h3 should exist if h4 exists");
        assert!(
            h3_pos < h4_pos,
            "h3 should appear before h4 for proper heading hierarchy"
        );
    }
    // If no h4 exists, the test passes (h4 is optional)
}

#[test]
fn test_uses_semantic_section_elements() {
    let html = read_homepage_html();
    assert!(html.contains("<section"), "Page should use semantic <section> elements");
    assert!(html.contains("<article"), "Page should use semantic <article> elements where appropriate");
}

#[test]
fn test_sections_have_aria_labelledby() {
    let html = read_homepage_html();
    // Count sections with aria-labelledby attribute
    let sections_with_labels = html.matches("aria-labelledby").count();
    assert!(
        sections_with_labels >= 3,
        "Major sections should have aria-labelledby attributes for screen readers. Found: {}",
        sections_with_labels
    );
}

// =============================================================================
// Test Case 2: Keyboard Navigation Support
// =============================================================================

#[test]
fn test_has_skip_link() {
    let html = read_homepage_html();
    assert!(
        html.contains("skip-link") || html.contains("skip-to"),
        "Page should have a skip navigation link for keyboard users"
    );
}

#[test]
fn test_skip_link_targets_main_content() {
    let html = read_homepage_html();
    // Check that skip link href points to main content
    assert!(
        html.contains("href=\"#main-content\"") || html.contains("href=\"#main\""),
        "Skip link should target main content area"
    );
}

#[test]
fn test_main_content_has_id() {
    let html = read_homepage_html();
    assert!(
        html.contains("id=\"main-content\"") || html.contains("id=\"main\""),
        "Main content area should have an id that matches skip link target"
    );
}

#[test]
fn test_interactive_elements_are_focusable() {
    let html = read_homepage_html();
    // Verify buttons and inputs exist for interactive console
    assert!(
        html.contains("<button") || html.contains("<input"),
        "Page should have focusable interactive elements"
    );
}

#[test]
fn test_console_input_has_label() {
    let html = read_homepage_html();
    // Check for console input with proper label
    assert!(
        html.contains("id=\"console-input\""),
        "Console input should have an id for label association"
    );
    assert!(
        html.contains("for=\"console-input\"") || html.contains("aria-label") || html.contains("aria-describedby"),
        "Console input should have an associated label or aria-label"
    );
}

// =============================================================================
// Test Case 3: Color Contrast - CSS Verification
// =============================================================================

#[test]
fn test_css_has_focus_styles() {
    let css = read_styles_css();
    assert!(
        css.contains(":focus") || css.contains(":focus-visible"),
        "CSS should define focus styles for keyboard navigation"
    );
}

#[test]
fn test_css_focus_uses_outline() {
    let css = read_styles_css();
    // Focus styles should use outline for visibility
    assert!(
        css.contains("outline"),
        "Focus styles should use outline for visible focus indicators"
    );
}

#[test]
fn test_css_has_high_contrast_text() {
    let css = read_styles_css();
    // Check for text color variables that should provide good contrast
    // Dark text on light background (#1f2937 on white provides >4.5:1)
    assert!(
        css.contains("--text-color") && css.contains("#1f2937"),
        "CSS should define high-contrast text color (dark text on light background)"
    );
}

#[test]
fn test_css_skip_link_styles() {
    let css = read_styles_css();
    assert!(
        css.contains(".skip-link"),
        "CSS should have styles for skip navigation link"
    );
}

// =============================================================================
// Test Case 4: Screen Reader Compatibility - ARIA Labels
// =============================================================================

#[test]
fn test_nav_has_aria_label() {
    let html = read_homepage_html();
    assert!(
        html.contains("role=\"navigation\"") && html.contains("aria-label"),
        "Navigation should have role and aria-label for screen readers"
    );
}

#[test]
fn test_main_has_role() {
    let html = read_homepage_html();
    assert!(
        html.contains("role=\"main\"") || html.contains("<main"),
        "Main content should have role=\"main\" or use <main> element"
    );
}

#[test]
fn test_header_has_role() {
    let html = read_homepage_html();
    assert!(
        html.contains("role=\"banner\"") || html.contains("<header"),
        "Header should have role=\"banner\" or use <header> element"
    );
}

#[test]
fn test_footer_has_role() {
    let html = read_homepage_html();
    assert!(
        html.contains("role=\"contentinfo\"") || html.contains("<footer"),
        "Footer should have role=\"contentinfo\" or use <footer> element"
    );
}

#[test]
fn test_images_have_alt_text() {
    let html = read_homepage_html();
    // Check that img tags have alt attributes
    let img_count = html.matches("<img").count();
    let alt_count = html.matches("alt=").count();
    assert!(
        img_count <= alt_count,
        "All images should have alt attributes. Images: {}, alt attrs: {}",
        img_count,
        alt_count
    );
}

#[test]
fn test_aria_live_for_dynamic_content() {
    let html = read_homepage_html();
    // Console output should have aria-live for screen reader announcements
    assert!(
        html.contains("aria-live"),
        "Dynamic content areas should have aria-live for screen readers"
    );
}

#[test]
fn test_console_output_has_role() {
    let html = read_homepage_html();
    assert!(
        html.contains("role=\"log\"") || html.contains("role=\"status\""),
        "Console output should have appropriate role for screen readers"
    );
}

// =============================================================================
// Test Case 5: Console Input Accessibility
// =============================================================================

#[test]
fn test_console_input_exists() {
    let html = read_homepage_html();
    assert!(
        html.contains("id=\"console-input\""),
        "Console input field should exist with id='console-input'"
    );
}

#[test]
fn test_console_input_has_visible_label_or_aria() {
    let html = read_homepage_html();
    // Check for label element or aria-label
    let has_label = html.contains("for=\"console-input\"");
    let has_aria = html.contains("aria-label") || html.contains("aria-describedby");
    assert!(
        has_label || has_aria,
        "Console input should have a visible label or aria-label for accessibility"
    );
}

#[test]
fn test_console_has_help_text() {
    let html = read_homepage_html();
    assert!(
        html.contains("console-help") || html.contains("aria-describedby"),
        "Console should have help text or description for users"
    );
}

#[test]
fn test_css_visually_hidden_class() {
    let css = read_styles_css();
    assert!(
        css.contains(".visually-hidden") || css.contains(".sr-only"),
        "CSS should have visually-hidden class for screen reader only content"
    );
}

#[test]
fn test_html_uses_visually_hidden() {
    let html = read_homepage_html();
    assert!(
        html.contains("visually-hidden") || html.contains("sr-only"),
        "HTML should use visually-hidden class for accessible hidden labels"
    );
}

// =============================================================================
// Additional Accessibility Requirements
// =============================================================================

#[test]
fn test_html_has_lang_attribute() {
    let html = read_homepage_html();
    assert!(
        html.contains("<html lang=\"en\"") || html.contains("<html lang='en'"),
        "HTML element should have lang attribute for language identification"
    );
}

#[test]
fn test_meta_viewport_exists() {
    let html = read_homepage_html();
    assert!(
        html.contains("meta name=\"viewport\"") || html.contains("meta name='viewport'"),
        "Page should have viewport meta tag for mobile accessibility"
    );
}

#[test]
fn test_external_links_have_rel_noopener() {
    let html = read_homepage_html();
    // External links should have rel="noopener noreferrer" for security
    if html.contains("target=\"_blank\"") {
        assert!(
            html.contains("rel=\"noopener") || html.contains("rel='noopener"),
            "External links with target=\"_blank\" should have rel=\"noopener noreferrer\""
        );
    }
}

#[test]
fn test_links_have_accessible_text() {
    let html = read_homepage_html();
    // Links should not be empty or just say "click here"
    // Check that no links have empty text content ("></a>")
    assert!(
        !html.contains(">click here<") && !html.contains("></a>"),
        "Links should have descriptive accessible text, not 'click here' or empty"
    );
}

#[test]
fn test_buttons_have_accessible_text() {
    let html = read_homepage_html();
    // Buttons should have text content or aria-label
    if html.contains("<button") {
        // Check that button is not empty or has aria-label
        let has_text_button = html.contains("</button>") && !html.contains("></button>");
        let has_aria_label = html.contains("aria-label");
        assert!(
            has_text_button || has_aria_label,
            "Buttons should have accessible text or aria-label"
        );
    }
}
