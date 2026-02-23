//! Integration tests for Homepage Static Content Rendering (Scenario 2)
//!
//! These tests verify that the homepage HTML contains all required content
//! as specified in the PRD requirements.

use std::fs;
use std::path::PathBuf;

fn get_html_path() -> PathBuf {
    // Get the path relative to CARGO_MANIFEST_DIR (tests-standalone directory)
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // Go up from tests-standalone to workspace root
    p.push("mirdb-server/assets/index.html");
    p
}

fn read_homepage_html() -> String {
    let path = get_html_path();
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read index.html from {:?}: {}", path, e))
}

#[test]
fn test_homepage_contains_logo_reference() {
    let html = read_homepage_html();
    assert!(
        html.contains("assets/logo.gif") || html.contains("/assets/logo.gif"),
        "Homepage should contain MirDB logo reference (assets/logo.gif)"
    );
}

#[test]
fn test_homepage_contains_project_title() {
    let html = read_homepage_html();
    assert!(
        html.contains("MirDB: A Persistent Key-Value Store with Memcached Protocol"),
        "Homepage should contain the project title 'MirDB: A Persistent Key-Value Store with Memcached Protocol'"
    );
}

#[test]
fn test_homepage_contains_features_nav_link() {
    let html = read_homepage_html();
    assert!(
        html.contains(">Features<") || html.contains(">Features</"),
        "Homepage should contain Features navigation link"
    );
}

#[test]
fn test_homepage_contains_documentation_nav_link() {
    let html = read_homepage_html();
    assert!(
        html.contains(">Documentation<") || html.contains(">Documentation</"),
        "Homepage should contain Documentation navigation link"
    );
}

#[test]
fn test_homepage_contains_github_nav_link() {
    let html = read_homepage_html();
    assert!(
        html.contains(">GitHub<") || html.contains(">GitHub</"),
        "Homepage should contain GitHub navigation link"
    );
}

#[test]
fn test_homepage_contains_memcached_protocol_feature() {
    let html = read_homepage_html();
    assert!(
        html.contains("Memcached Protocol") || html.contains("memcached protocol"),
        "Homepage features section should mention memcached protocol"
    );
}

#[test]
fn test_homepage_contains_skiplist_memtable_feature() {
    let html = read_homepage_html();
    assert!(
        html.contains("Skip-List") || html.contains("skip-list") || html.contains("skiplist"),
        "Homepage features section should mention skip-list memtable"
    );
}

#[test]
fn test_homepage_contains_compaction_feature() {
    let html = read_homepage_html();
    assert!(
        html.contains("Compaction") || html.contains("compaction"),
        "Homepage features section should mention compaction"
    );
}

#[test]
fn test_homepage_contains_persistence_feature() {
    let html = read_homepage_html();
    assert!(
        html.contains("Persistence") || html.contains("persistence") || html.contains("persistent") || html.contains("Persistent"),
        "Homepage features section should mention persistence"
    );
}

#[test]
fn test_homepage_contains_github_repository_link() {
    let html = read_homepage_html();
    assert!(
        html.contains("github.com") && html.contains("mirdb"),
        "Homepage should contain GitHub repository link"
    );
}

#[test]
fn test_homepage_has_valid_doctype() {
    let html = read_homepage_html();
    assert!(
        html.trim_start().starts_with("<!DOCTYPE html>") || html.trim_start().starts_with("<!doctype html>"),
        "Homepage should have valid HTML5 doctype"
    );
}

#[test]
fn test_homepage_has_lang_attribute() {
    let html = read_homepage_html();
    assert!(
        html.contains("<html lang="),
        "Homepage should have lang attribute on html element for accessibility"
    );
}

#[test]
fn test_homepage_has_h1_heading() {
    let html = read_homepage_html();
    assert!(
        html.contains("<h1>") && html.contains("</h1>"),
        "Homepage should have an h1 heading element"
    );
}

#[test]
fn test_homepage_has_main_element() {
    let html = read_homepage_html();
    assert!(
        html.contains("<main") && html.contains("</main>"),
        "Homepage should have semantic main element"
    );
}

#[test]
fn test_homepage_has_header_element() {
    let html = read_homepage_html();
    assert!(
        html.contains("<header") && html.contains("</header>"),
        "Homepage should have semantic header element"
    );
}

#[test]
fn test_homepage_has_footer_element() {
    let html = read_homepage_html();
    assert!(
        html.contains("<footer") && html.contains("</footer>"),
        "Homepage should have semantic footer element"
    );
}

#[test]
fn test_homepage_has_nav_element() {
    let html = read_homepage_html();
    assert!(
        html.contains("<nav") && html.contains("</nav>"),
        "Homepage should have semantic nav element"
    );
}

#[test]
fn test_homepage_has_features_section() {
    let html = read_homepage_html();
    assert!(
        html.contains("id=\"features\""),
        "Homepage should have features section with id='features'"
    );
}

#[test]
fn test_homepage_heading_hierarchy() {
    let html = read_homepage_html();
    let h1_pos = html.find("<h1>").expect("h1 should exist");
    let h2_pos = html.find("<h2").expect("h2 should exist");
    assert!(
        h1_pos < h2_pos,
        "h1 should appear before h2 for proper heading hierarchy"
    );
}

#[test]
fn test_homepage_has_sections_with_aria_labels() {
    let html = read_homepage_html();
    // Check that at least some sections have accessibility labels
    assert!(
        html.contains("aria-label") || html.contains("aria-labelledby"),
        "Homepage sections should have ARIA labels for accessibility"
    );
}
