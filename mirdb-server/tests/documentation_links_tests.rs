//! Documentation Links Tests
//! Owner: Scenario 10 - Documentation Links
//!
//! Tests for REQ-8: Homepage shall provide links to README, GitHub repository, and API documentation
//!
//! Test Cases:
//! 1. Footer contains GitHub link, license info, version number
//! 2. Links to README and API documentation are present
//! 3. Links to correct GitHub repository URL

use std::fs;
use std::path::Path;

const GITHUB_REPO_URL: &str = "https://github.com/yetone/mirdb";

/// Test Case 1: Footer contains GitHub link, license info, version number
#[test]
fn test_footer_contains_github_link() {
    let html_path = Path::new("src/web/index.html");
    assert!(html_path.exists(), "index.html should exist");

    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for GitHub link with correct URL
    assert!(
        content.contains(GITHUB_REPO_URL),
        "Footer should contain GitHub repository URL"
    );
    assert!(
        content.contains("data-testid=\"github-link\""),
        "Footer should have GitHub link with data-testid"
    );
}

#[test]
fn test_footer_contains_license_info() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for license information
    assert!(
        content.contains("MIT"),
        "Footer should contain MIT license reference"
    );
    assert!(
        content.contains("Licensed under"),
        "Footer should contain 'Licensed under' text"
    );
    assert!(
        content.contains("data-testid=\"license-link\""),
        "Footer should have license link with data-testid"
    );
}

#[test]
fn test_footer_contains_version_number() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for version number element
    assert!(
        content.contains("data-testid=\"version-number\""),
        "Footer should have version number element with data-testid"
    );
    assert!(
        content.contains("id=\"app-version\""),
        "Footer should have app-version element"
    );
    assert!(
        content.contains("Version"),
        "Footer should contain 'Version' text"
    );
}

/// Test Case 2: Links to README and API documentation are present
#[test]
fn test_readme_link_present() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for README link
    assert!(
        content.contains("data-testid=\"readme-link\""),
        "Footer should have README link with data-testid"
    );
    assert!(
        content.contains("#readme"),
        "README link should point to #readme anchor"
    );
    assert!(
        content.contains(">README<"),
        "README link should have 'README' text"
    );
}

#[test]
fn test_api_documentation_link_present() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for API documentation link
    assert!(
        content.contains("data-testid=\"api-docs-link\""),
        "Footer should have API docs link with data-testid"
    );
    assert!(
        content.contains("/api-docs"),
        "API docs link should point to /api-docs"
    );
    assert!(
        content.contains("API Documentation"),
        "API docs link should have 'API Documentation' text"
    );
}

#[test]
fn test_documentation_section_exists() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for documentation section
    assert!(
        content.contains("data-testid=\"footer-docs\""),
        "Footer should have documentation section with data-testid"
    );
    assert!(
        content.contains("class=\"footer-heading\""),
        "Footer should have documentation heading"
    );
    assert!(
        content.contains("Documentation"),
        "Footer heading should contain 'Documentation'"
    );
}

/// Test Case 3: Links to correct GitHub repository URL
#[test]
fn test_github_link_correct_url() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Verify exact GitHub URL
    let github_url = "https://github.com/yetone/mirdb";
    assert!(
        content.contains(&format!("href=\"{}\"", github_url)),
        "GitHub link should have exact URL: {}", github_url
    );
}

#[test]
fn test_readme_link_correct_url() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Verify README URL includes GitHub repo and #readme anchor
    let readme_url = "https://github.com/yetone/mirdb#readme";
    assert!(
        content.contains(&format!("href=\"{}\"", readme_url)),
        "README link should have exact URL: {}", readme_url
    );
}

#[test]
fn test_footer_structure() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check footer structure
    assert!(
        content.contains("<footer class=\"footer\""),
        "Should have footer element with footer class"
    );
    assert!(
        content.contains("class=\"footer-content\""),
        "Footer should have footer-content container"
    );
    assert!(
        content.contains("class=\"footer-nav\""),
        "Footer should have footer-nav for links"
    );
    assert!(
        content.contains("class=\"footer-info\""),
        "Footer should have footer-info section"
    );
}

#[test]
fn test_footer_accessibility() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for accessibility attributes
    assert!(
        content.contains("aria-label=\"Documentation links\""),
        "Footer nav should have aria-label for accessibility"
    );
}

#[test]
fn test_all_links_have_footer_link_class() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check that documentation links use footer-link class
    assert!(
        content.contains("class=\"footer-link\" data-testid=\"readme-link\""),
        "README link should have footer-link class"
    );
    assert!(
        content.contains("class=\"footer-link\" data-testid=\"github-link\""),
        "GitHub link should have footer-link class"
    );
    assert!(
        content.contains("class=\"footer-link\" data-testid=\"api-docs-link\""),
        "API docs link should have footer-link class"
    );
}

#[test]
fn test_css_footer_styles_exist() {
    let css_path = Path::new("src/web/styles/main.css");
    assert!(css_path.exists(), "main.css should exist");

    let content = fs::read_to_string(css_path).expect("Failed to read main.css");

    // Check for footer-specific styles
    assert!(
        content.contains(".footer"),
        "CSS should have .footer styles"
    );
    assert!(
        content.contains(".footer-link"),
        "CSS should have .footer-link styles"
    );
    assert!(
        content.contains(".footer-docs"),
        "CSS should have .footer-docs styles"
    );
    assert!(
        content.contains(".footer-heading"),
        "CSS should have .footer-heading styles"
    );
    assert!(
        content.contains(".footer-version"),
        "CSS should have .footer-version styles"
    );
}
