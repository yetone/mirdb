/**
 * Content validation tests for the MirDB homepage HTML.
 * Owner: Scenario 2 - Hero Section Content
 *
 * Parses the homepage HTML and validates structural requirements.
 * Tests hero section, features, navigation, footer content.
 *
 * Expected test categories:
 * - Hero section headline and subheadline
 * - Features count and descriptions
 * - Navigation link presence
 * - Footer copyright
 * - Semantic HTML structure
 * - Accessibility attributes
 */

use std::fs;

fn read_homepage_html() -> String {
    fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/static/index.html"))
        .expect("Failed to read index.html")
}

fn extract_footer(html: &str) -> &str {
    let start = html.find("<footer").expect("Footer element not found");
    let end = html[start..].find("</footer>").expect("Footer closing tag not found")
        + start
        + 9; // length of "</footer>"
    &html[start..end]
}

// ============================================================================
// Footer Section Tests (Scenario 6)
// ============================================================================

#[test]
fn test_footer_element_exists() {
    let html = read_homepage_html();
    assert!(
        html.contains("<footer"),
        "Page should contain a <footer> element"
    );
}

#[test]
fn test_footer_contains_copyright() {
    let html = read_homepage_html();
    let footer = extract_footer(&html);
    let lower = footer.to_lowercase();

    assert!(
        lower.contains("copyright") || lower.contains("&copy;") || lower.contains("©"),
        "Footer should contain copyright text"
    );

    assert!(
        footer.contains("2026"),
        "Footer should contain the current year"
    );

    assert!(
        lower.contains("mirdb"),
        "Footer should contain the organization or product name (MirDB)"
    );
}

#[test]
fn test_footer_contains_links() {
    let html = read_homepage_html();
    let footer = extract_footer(&html);
    let lower = footer.to_lowercase();

    assert!(
        lower.contains("github"),
        "Footer should contain a link to the GitHub repository"
    );

    assert!(
        lower.contains("license"),
        "Footer should contain a link to the license file"
    );

    // Verify there are actual anchor tags in the footer
    assert!(
        footer.contains("<a "),
        "Footer should contain anchor link elements"
    );
}
