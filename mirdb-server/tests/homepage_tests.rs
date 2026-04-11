//! Homepage Landing and Branding Tests
//! Owner: Scenario 1 - Homepage Landing and Branding
//!
//! Tests:
//! - Homepage HTML contains MirDB branding
//! - Logo and tagline are present
//! - Navigation links are present
//! - Rust-themed CSS colors are applied

use std::fs;
use std::path::Path;

/// Test 1: Homepage HTML file exists and contains MirDB branding
#[test]
fn test_homepage_contains_mirdb_branding() {
    let html_path = Path::new("src/web/index.html");
    assert!(html_path.exists(), "index.html should exist");

    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for MirDB branding
    assert!(
        content.contains("MirDB"),
        "Homepage should contain 'MirDB' branding"
    );
    assert!(
        content.contains("logo"),
        "Homepage should contain logo element"
    );
    assert!(
        content.contains("tagline"),
        "Homepage should contain tagline"
    );
}

/// Test 2: Homepage contains navigation links
#[test]
fn test_homepage_has_navigation_links() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for navigation elements
    assert!(content.contains("nav"), "Homepage should contain nav element");
    assert!(
        content.contains("Home"),
        "Homepage should have Home nav link"
    );
    assert!(
        content.contains("Status"),
        "Homepage should have Status nav link"
    );
    assert!(
        content.contains("Keys"),
        "Homepage should have Keys nav link"
    );
    assert!(
        content.contains("Config"),
        "Homepage should have Config nav link"
    );
}

/// Test 3: CSS file contains Rust-themed color palette
#[test]
fn test_css_contains_rust_themed_colors() {
    let css_path = Path::new("src/web/styles/main.css");
    assert!(css_path.exists(), "main.css should exist");

    let content = fs::read_to_string(css_path).expect("Failed to read main.css");

    // Check for Rust orange color palette
    assert!(
        content.contains("#E65100") || content.contains("#e65100"),
        "CSS should contain primary Rust orange (#E65100)"
    );
    assert!(
        content.contains("#FF9800") || content.contains("#ff9800"),
        "CSS should contain secondary Rust orange (#FF9800)"
    );

    // Check for dark accent colors
    assert!(
        content.contains("#1A1A1A") || content.contains("#1a1a1a"),
        "CSS should contain dark primary color (#1A1A1A)"
    );
}

/// Test 4: Homepage HTML structure is valid
#[test]
fn test_homepage_html_structure() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for essential HTML structure
    assert!(
        content.contains("<!DOCTYPE html>"),
        "Should have DOCTYPE declaration"
    );
    assert!(
        content.contains("<html"),
        "Should have html opening tag"
    );
    assert!(
        content.contains("<head>"),
        "Should have head element"
    );
    assert!(
        content.contains("<body>"),
        "Should have body element"
    );
    assert!(
        content.contains("<header"),
        "Should have header element"
    );
    assert!(
        content.contains("<main"),
        "Should have main element"
    );
    assert!(
        content.contains("<footer"),
        "Should have footer element"
    );
}

/// Test 5: Homepage includes required CSS and JS links
#[test]
fn test_homepage_includes_assets() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for stylesheet links
    assert!(
        content.contains("main.css"),
        "Should link to main.css"
    );

    // Check for script includes
    assert!(
        content.contains("main.js"),
        "Should include main.js"
    );
    assert!(
        content.contains("api.js"),
        "Should include api.js"
    );
}

/// Test 6: Logo element has proper structure
#[test]
fn test_logo_structure() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    // Check for logo with SVG or image
    assert!(
        content.contains("class=\"logo\"") || content.contains("class='logo'"),
        "Should have logo class"
    );

    // Check for logo text
    assert!(
        content.contains("logo-text"),
        "Should have logo text element"
    );
}

/// Test 7: CSS has responsive media queries
#[test]
fn test_css_has_responsive_design() {
    let css_path = Path::new("src/web/styles/main.css");
    let content = fs::read_to_string(css_path).expect("Failed to read main.css");

    // Check for media queries
    assert!(
        content.contains("@media"),
        "CSS should have media queries for responsive design"
    );

    // Check for common breakpoints
    assert!(
        content.contains("768px") || content.contains("1024px"),
        "CSS should have standard breakpoint values"
    );
}

/// Test 8: Dark mode CSS file exists and has theme colors
#[test]
fn test_dark_mode_css_exists() {
    let css_path = Path::new("src/web/styles/dark-mode.css");
    assert!(css_path.exists(), "dark-mode.css should exist");

    let content = fs::read_to_string(css_path).expect("Failed to read dark-mode.css");

    // Check for dark mode media query or class
    assert!(
        content.contains("prefers-color-scheme") || content.contains("dark-mode"),
        "Should have dark mode support"
    );
}

/// Test 9: Homepage title includes MirDB
#[test]
fn test_homepage_title() {
    let html_path = Path::new("src/web/index.html");
    let content = fs::read_to_string(html_path).expect("Failed to read index.html");

    assert!(
        content.contains("<title>") && content.contains("MirDB"),
        "Page title should include MirDB"
    );
}

/// Test 10: CSS variables are defined
#[test]
fn test_css_custom_properties() {
    let css_path = Path::new("src/web/styles/main.css");
    let content = fs::read_to_string(css_path).expect("Failed to read main.css");

    // Check for CSS custom properties (variables)
    assert!(
        content.contains(":root"),
        "CSS should define root variables"
    );
    assert!(
        content.contains("--color-rust"),
        "CSS should have Rust color variables"
    );
}
