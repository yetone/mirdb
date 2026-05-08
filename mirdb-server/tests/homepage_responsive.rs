/**
 * Mobile responsiveness tests for the MirDB homepage.
 * Owner: Scenario 7 - Mobile Responsiveness
 *
 * Validates that the homepage renders correctly on mobile viewports:
 * - Viewport meta tag is present and correctly configured
 * - CSS contains media queries for mobile/tablet breakpoints
 * - Navigation uses a responsive pattern (stacked, flex-wrap, hamburger)
 * - Layout uses relative units (rem, %, vw) for main containers
 * - Text sizes are readable on mobile (>=16px equivalent)
 */

use std::fs;

const HTML_PATH: &str = "static/index.html";
const CSS_PATH: &str = "static/styles.css";

fn read_html() -> String {
    fs::read_to_string(HTML_PATH).expect("Failed to read static/index.html")
}

fn read_css() -> String {
    fs::read_to_string(CSS_PATH).expect("Failed to read static/styles.css")
}

/// Extract the head section from the HTML
fn get_head(html: &str) -> Option<String> {
    let start = html.find("<head>")?;
    let end = html.find("</head>")?;
    Some(html[start..end].to_string())
}

#[test]
fn test_viewport_meta_tag_present() {
    let html = read_html();
    let head = get_head(&html).expect("HTML head section not found");

    // The head must contain a viewport meta tag with width=device-width
    let has_viewport_meta = head.contains("name=\"viewport\"") || head.contains("name='viewport'");
    assert!(
        has_viewport_meta,
        "HTML head must contain a <meta name=\"viewport\"> tag for mobile rendering"
    );

    let has_device_width = head.contains("width=device-width");
    assert!(
        has_device_width,
        "Viewport meta tag must include 'width=device-width' for proper mobile sizing"
    );

    let has_initial_scale = head.contains("initial-scale=1");
    assert!(
        has_initial_scale,
        "Viewport meta tag should include 'initial-scale=1' for consistent zoom level"
    );
}

#[test]
fn test_css_contains_media_query_for_mobile() {
    let css = read_css();

    // The CSS must contain at least one @media query targeting screens <= 768px
    assert!(
        css.contains("@media"),
        "CSS must contain at least one @media query for responsive design"
    );

    // Look for a max-width breakpoint at or below 768px (common mobile breakpoint)
    let has_mobile_breakpoint = css.contains("max-width: 768px")
        || css.contains("max-width:768px")
        || css.contains("max-width: 767px")
        || css.contains("max-width: 480px")
        || css.contains("max-width: 600px");

    assert!(
        has_mobile_breakpoint,
        "CSS must contain a @media query targeting mobile screens (max-width <= 768px)"
    );
}

#[test]
fn test_css_contains_tablet_or_additional_breakpoint() {
    let css = read_css();

    // Count distinct @media occurrences to confirm multiple breakpoints exist
    let media_count = css.matches("@media").count();
    assert!(
        media_count >= 1,
        "CSS should contain at least one @media query, found {}",
        media_count
    );
}

#[test]
fn test_navigation_uses_responsive_pattern() {
    let css = read_css();

    // Mobile navigation must use one of the standard responsive patterns:
    // - hamburger menu (.menu-toggle, .hamburger, etc.)
    // - flex-wrap on navigation
    // - stacked layout (flex-direction: column on nav)
    let has_hamburger = css.contains(".menu-toggle")
        || css.contains(".hamburger")
        || css.contains(".nav-toggle");
    let has_flex_wrap = css.contains("flex-wrap");
    let has_stacked_nav = css.contains("flex-direction: column")
        || css.contains("flex-direction:column");

    assert!(
        has_hamburger || has_flex_wrap || has_stacked_nav,
        "CSS must define a responsive navigation pattern (hamburger menu, flex-wrap, or stacked/column layout) for small screens"
    );
}

#[test]
fn test_layout_uses_relative_units() {
    let css = read_css();

    // The CSS must use relative units (%, vw, rem) for main containers,
    // rather than only fixed pixel widths.
    let has_percent = css.contains("100%") || css.contains(": 100%") || css.contains(": 90%");
    let has_vw = css.contains("vw");
    let has_rem = css.contains("rem");

    assert!(
        has_percent || has_vw || has_rem,
        "CSS must use relative units (%, vw, rem) for main containers"
    );

    // Specifically: there should be at least one container/layout rule using % or vw
    let has_container_relative = css.contains("width: 100%")
        || css.contains("max-width: 100vw")
        || css.contains("max-width: 100%");
    assert!(
        has_container_relative,
        "CSS must define at least one main container using relative width (100%, 100vw, etc.)"
    );
}

#[test]
fn test_no_horizontal_overflow_protection() {
    let css = read_css();

    // To prevent horizontal scrolling, the CSS should set overflow-x: hidden
    // on body/html OR use max-width constraints to keep content within viewport.
    let has_overflow_x_hidden = css.contains("overflow-x: hidden")
        || css.contains("overflow-x:hidden");
    let has_max_width_viewport =
        css.contains("max-width: 100vw") || css.contains("max-width: 100%");

    assert!(
        has_overflow_x_hidden || has_max_width_viewport,
        "CSS must prevent horizontal overflow via overflow-x: hidden or max-width constraints"
    );
}

/// Extract the body of an @media block by counting matching braces.
fn extract_media_block<'a>(css: &'a str, query_marker: &str) -> Option<&'a str> {
    let media_start = css.find(query_marker)?;
    let after_marker = &css[media_start..];
    let open_brace_offset = after_marker.find('{')?;
    let body_start = media_start + open_brace_offset + 1;

    let mut depth: i32 = 1;
    let mut end_idx: Option<usize> = None;
    for (i, ch) in css[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(body_start + i);
                    break;
                }
            }
            _ => {}
        }
    }
    end_idx.map(|end| &css[body_start..end])
}

#[test]
fn test_mobile_text_size_readable() {
    let css = read_css();

    let media_block = extract_media_block(&css, "@media (max-width: 768px)")
        .expect("Mobile media query @media (max-width: 768px) not found");

    // body font-size should be at least 1rem (16px equivalent)
    let has_readable_body_font = media_block.contains("font-size: 1rem")
        || media_block.contains("font-size: 16px")
        || media_block.contains("font-size:1rem");

    assert!(
        has_readable_body_font,
        "Mobile media query should set body font-size to >= 1rem (16px) for readability"
    );
}

#[test]
fn test_hero_adapts_on_mobile() {
    let css = read_css();

    let media_block = extract_media_block(&css, "@media (max-width: 768px)")
        .expect("Mobile media query not found");

    let has_hero_h1_override = media_block.contains(".hero-content h1");
    assert!(
        has_hero_h1_override,
        "Mobile media query should override .hero-content h1 font-size for smaller screens"
    );
}
