//! Homepage UI Layout and Branding tests.
//! Owner: Scenario 9 - Homepage UI Layout and Branding
//!
//! Tests:
//! 1. Verify MirDB logo and product name in header (REQ-1)
//! 2. Verify hero section tagline with key phrases (REQ-1)
//! 3. Verify features section content (REQ-2)
//! 4. Verify GitHub link presence (REQ-9)

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

/// Test Case 1: Load homepage and inspect DOM
/// Expected: Contains MirDB logo image and 'MirDB' text in header
#[tokio::test]
async fn test_homepage_contains_mirdb_logo_and_name_in_header() {
    let html = get_homepage_html().await;

    // Verify header section exists
    assert!(
        html.contains("<header"),
        "Homepage should have a header section"
    );

    // Verify MirDB product name is in the header (h1 element)
    assert!(
        html.contains("<h1>MirDB</h1>"),
        "Header should contain MirDB product name in h1"
    );

    // Verify logo element exists (logo icon with logo class)
    assert!(
        html.contains("class=\"logo\"") || html.contains("class=\"logo-icon\""),
        "Header should contain logo element"
    );

    // Verify logo icon is present (file cabinet emoji or similar)
    assert!(
        html.contains("logo-icon") && html.contains("</span>"),
        "Header should contain logo icon element"
    );

    // Verify the logo section contains both logo icon and MirDB text
    assert!(
        html.contains("<div class=\"logo\">") && html.contains("MirDB"),
        "Logo div should contain MirDB branding"
    );
}

/// Test Case 2: Check hero section tagline
/// Expected: Text includes 'Persistent Key-Value Store' and 'Memcached Protocol'
#[tokio::test]
async fn test_hero_section_contains_key_phrases() {
    let html = get_homepage_html().await;

    // Verify hero section exists
    assert!(
        html.contains("class=\"hero\""),
        "Homepage should have a hero section"
    );

    // Verify 'Persistent Key-Value Store' phrase is present
    assert!(
        html.contains("Persistent Key-Value Store"),
        "Hero section should contain 'Persistent Key-Value Store'"
    );

    // Verify 'Memcached Protocol' phrase is present
    assert!(
        html.contains("Memcached Protocol"),
        "Hero section should contain 'Memcached Protocol'"
    );

    // Verify hero has navigation buttons (Quick Start, Documentation, Try It Out)
    assert!(
        html.contains("Quick Start"),
        "Hero section should have Quick Start button"
    );
    assert!(
        html.contains("Documentation"),
        "Hero section should have Documentation button"
    );
    assert!(
        html.contains("Try It Out"),
        "Hero section should have Try It Out button"
    );
}

/// Test Case 3: Verify features section content
/// Expected: Contains 'Disk Persistence', 'LSM-tree', 'Memcached Compatible' features
#[tokio::test]
async fn test_features_section_contains_core_features() {
    let html = get_homepage_html().await;

    // Verify features section exists
    assert!(
        html.contains("class=\"features\""),
        "Homepage should have a features section"
    );

    // Verify 'Disk Persistence' feature is present
    assert!(
        html.contains("Disk Persistence"),
        "Features section should contain 'Disk Persistence'"
    );

    // Verify 'LSM-tree' feature is present (part of 'LSM-tree Architecture')
    assert!(
        html.contains("LSM-tree"),
        "Features section should contain 'LSM-tree'"
    );

    // Verify 'Memcached Compatible' feature is present
    assert!(
        html.contains("Memcached Compatible"),
        "Features section should contain 'Memcached Compatible'"
    );

    // Verify feature cards exist
    assert!(
        html.contains("class=\"feature-card\""),
        "Features should be displayed in feature cards"
    );
}

/// Test Case 4: Check GitHub link presence
/// Expected: Link to GitHub repository exists and is clickable (REQ-9)
#[tokio::test]
async fn test_github_link_exists_and_clickable() {
    let html = get_homepage_html().await;

    // Verify GitHub link exists
    assert!(
        html.contains("github.com") || html.contains("GitHub"),
        "Homepage should have a GitHub reference"
    );

    // Verify the link is an anchor tag (clickable)
    assert!(
        html.contains("href=\"https://github.com"),
        "GitHub link should be a clickable anchor with href"
    );

    // Verify the link has appropriate attributes for external link
    assert!(
        html.contains("target=\"_blank\""),
        "GitHub link should open in new tab"
    );
    assert!(
        html.contains("rel=\"noopener"),
        "GitHub link should have noopener for security"
    );

    // Verify the GitHub link has a class for styling
    assert!(
        html.contains("class=\"github-link\""),
        "GitHub link should have github-link class"
    );
}

/// Additional test: Verify overall page structure and accessibility
#[tokio::test]
async fn test_homepage_structure_and_accessibility() {
    let html = get_homepage_html().await;

    // Verify proper HTML5 structure
    assert!(
        html.contains("<!DOCTYPE html>"),
        "Should have HTML5 doctype"
    );
    assert!(
        html.contains("lang=\"en\""),
        "Should have language attribute for accessibility"
    );

    // Verify semantic structure
    assert!(html.contains("<header"), "Should have header element");
    assert!(html.contains("<main"), "Should have main element");
    assert!(html.contains("<footer"), "Should have footer element");

    // Verify aria labels for accessibility
    assert!(
        html.contains("aria-label") || html.contains("aria-labelledby"),
        "Should have aria labels for screen readers"
    );

    // Verify meta viewport for responsive design
    assert!(
        html.contains("viewport"),
        "Should have viewport meta tag for responsive design"
    );
}

/// Additional test: Verify navigation elements in hero section
#[tokio::test]
async fn test_hero_navigation_elements() {
    let html = get_homepage_html().await;

    // Verify hero actions container exists
    assert!(
        html.contains("class=\"hero-actions\""),
        "Hero should have actions container"
    );

    // Verify CTA buttons are links (anchor tags with href)
    assert!(
        html.contains("<a href=\"#dashboard\"") || html.contains("href=\"#"),
        "Quick Start should be a link"
    );
    assert!(
        html.contains("<a href=\"#try-it-out\"") || html.contains("Try It Out"),
        "Try It Out should be navigable"
    );

    // Verify button styling classes
    assert!(
        html.contains("btn btn-primary") || html.contains("class=\"btn"),
        "Buttons should have button styling"
    );
}

// ============================================================================
// Scenario 12: Memcached Protocol Reference Section tests
// Owner: Scenario 12 - Memcached Protocol Reference Section
// REQ-6: Quick reference section for Memcached protocol commands
// ============================================================================

/// Test Case 1: Load homepage and find protocol reference section
/// Expected: Section visible with 'Memcached Protocol' or similar heading
#[tokio::test]
async fn test_protocol_reference_section_visible() {
    let html = get_homepage_html().await;

    // Verify protocol reference section exists with id="protocol"
    assert!(
        html.contains("id=\"protocol\""),
        "Homepage should have a protocol reference section with id='protocol'"
    );

    // Verify protocol reference section has the correct class
    assert!(
        html.contains("class=\"protocol-reference\""),
        "Protocol reference section should have 'protocol-reference' class"
    );

    // Verify 'Memcached Protocol' heading is present
    assert!(
        html.contains("Memcached Protocol Reference")
            || html.contains("Memcached Protocol"),
        "Protocol reference section should have 'Memcached Protocol' heading"
    );

    // Verify the section has proper heading structure
    assert!(
        html.contains("id=\"protocol-title\""),
        "Protocol section should have labeled title for accessibility"
    );

    // Verify aria-labelledby for accessibility
    assert!(
        html.contains("aria-labelledby=\"protocol-title\""),
        "Protocol section should have aria-labelledby for accessibility"
    );
}

/// Test Case 2: Check SET command documentation
/// Expected: Shows syntax: SET key flags exptime bytes [noreply]
#[tokio::test]
async fn test_set_command_documentation() {
    let html = get_homepage_html().await;

    // Verify SET command is documented
    assert!(
        html.contains("SET key flags exptime bytes"),
        "Protocol reference should document SET command syntax"
    );

    // Verify SET command shows optional noreply parameter
    assert!(
        html.contains("[noreply]") || html.contains("noreply"),
        "SET command should include noreply option"
    );

    // Verify SET command has description
    assert!(
        html.contains("Store a key-value pair")
            || html.contains("store")
            || html.contains("key-value"),
        "SET command should have a description"
    );

    // Verify command is displayed in code element
    assert!(
        html.contains("<code>SET"),
        "SET command syntax should be in code element for clarity"
    );
}

/// Test Case 3: Check GET command documentation
/// Expected: Shows syntax: GET key1 [key2 ...]
#[tokio::test]
async fn test_get_command_documentation() {
    let html = get_homepage_html().await;

    // Verify GET command is documented (accepts one or more keys)
    assert!(
        html.contains("GET key") || html.contains("GET key1"),
        "Protocol reference should document GET command syntax"
    );

    // Verify GET command shows ability to retrieve multiple keys
    assert!(
        html.contains("key2") || html.contains("...") || html.contains("more"),
        "GET command should show multiple key retrieval support"
    );

    // Verify GET command has description
    assert!(
        html.contains("Retrieve") || html.contains("retrieve") || html.contains("get"),
        "GET command should have a description"
    );

    // Verify command is displayed in code element
    assert!(
        html.contains("<code>GET"),
        "GET command syntax should be in code element for clarity"
    );
}

/// Additional test: Verify DELETE command documentation
/// Expected: Shows DELETE command syntax with key parameter
#[tokio::test]
async fn test_delete_command_documentation() {
    let html = get_homepage_html().await;

    // Verify DELETE command is documented
    assert!(
        html.contains("DELETE key"),
        "Protocol reference should document DELETE command syntax"
    );

    // Verify DELETE command has description
    assert!(
        html.contains("Remove") || html.contains("remove") || html.contains("delete"),
        "DELETE command should have a description"
    );

    // Verify command is displayed in code element
    assert!(
        html.contains("<code>DELETE"),
        "DELETE command syntax should be in code element for clarity"
    );
}

/// Additional test: Verify protocol commands are displayed in proper structure
#[tokio::test]
async fn test_protocol_commands_structure() {
    let html = get_homepage_html().await;

    // Verify commands are wrapped in command class divs
    assert!(
        html.contains("class=\"command\""),
        "Protocol commands should be in command-styled divs"
    );

    // Verify protocol-commands container exists
    assert!(
        html.contains("class=\"protocol-commands\""),
        "Protocol commands should be in a protocol-commands container"
    );

    // Count the number of commands documented (should have at least SET, GET, DELETE)
    let command_count = html.matches("<code>SET").count()
        + html.matches("<code>GET").count()
        + html.matches("<code>DELETE").count();
    assert!(
        command_count >= 3,
        "Should document at least SET, GET, and DELETE commands"
    );
}
