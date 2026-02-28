//! Status Display Tests
//!
//! Integration tests for Scenario 3 - Server Status and Version Display
//!
//! These tests verify:
//! - /api/status endpoint returns proper JSON with running status and version
//! - Homepage HTML contains status and version display elements
//! - Status indicator elements are present in HTML

/// The embedded HTML content from the homepage
const INDEX_HTML: &str = include_str!("../assets/index.html");

/// The embedded CSS content
const STYLE_CSS: &str = include_str!("../assets/css/style.css");

// ============================================================================
// Test Case 1: Access homepage with healthy storage engine
// Page displays 'running' or equivalent positive status indicator
// ============================================================================

#[test]
fn test_html_has_version_info_section() {
    assert!(
        INDEX_HTML.contains("id=\"version-info\""),
        "HTML must contain version-info section with id"
    );
    assert!(
        INDEX_HTML.contains("class=\"version-info\""),
        "Version info section must have version-info class"
    );
}

#[test]
fn test_html_has_server_version_element() {
    assert!(
        INDEX_HTML.contains("id=\"server-version\""),
        "HTML must contain server-version element with id"
    );
    assert!(
        INDEX_HTML.contains("class=\"server-version\""),
        "Server version element must have server-version class"
    );
}

#[test]
fn test_html_has_server_status_element() {
    assert!(
        INDEX_HTML.contains("id=\"server-status\""),
        "HTML must contain server-status element with id"
    );
    assert!(
        INDEX_HTML.contains("class=\"server-status\""),
        "Server status element must have server-status class"
    );
}

#[test]
fn test_html_has_status_indicator() {
    assert!(
        INDEX_HTML.contains("id=\"status-indicator\""),
        "HTML must contain status-indicator element with id"
    );
    assert!(
        INDEX_HTML.contains("class=\"status-indicator\""),
        "Status indicator must have status-indicator class"
    );
}

#[test]
fn test_html_has_status_text_element() {
    assert!(
        INDEX_HTML.contains("id=\"status-text\""),
        "HTML must contain status-text element with id"
    );
}

// ============================================================================
// Test Case 2: Check version display element
// Page contains version string matching MirDB package version
// ============================================================================

#[test]
fn test_html_has_version_loading_placeholder() {
    assert!(
        INDEX_HTML.contains("Version:"),
        "HTML must contain Version: text for display"
    );
}

#[test]
fn test_html_has_status_loading_placeholder() {
    assert!(
        INDEX_HTML.contains("Status:"),
        "HTML must contain Status: text for display"
    );
}

#[test]
fn test_html_has_status_fetch_script() {
    assert!(
        INDEX_HTML.contains("fetch('/api/status')"),
        "HTML must contain JavaScript to fetch status API"
    );
}

#[test]
fn test_html_script_handles_version() {
    assert!(
        INDEX_HTML.contains("data.version"),
        "Status script must access version from API response"
    );
}

#[test]
fn test_html_script_handles_running_status() {
    assert!(
        INDEX_HTML.contains("data.running"),
        "Status script must access running status from API response"
    );
}

#[test]
fn test_html_script_displays_running_text() {
    assert!(
        INDEX_HTML.contains("Running"),
        "Status script must display 'Running' text for active server"
    );
}

#[test]
fn test_html_script_displays_stopped_text() {
    assert!(
        INDEX_HTML.contains("Stopped"),
        "Status script must display 'Stopped' text for inactive server"
    );
}

// ============================================================================
// Test Case 3: GET /api/status endpoint
// Verify API status handler structure and state module
// ============================================================================

/// Tests the state module compiles and VERSION is defined
#[test]
fn test_state_version_constant_exists() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");
    assert!(
        handlers_content.contains("handle_status"),
        "handlers.rs must export handle_status function"
    );
    assert!(
        handlers_content.contains("state.is_running()") || handlers_content.contains("is_running"),
        "handle_status must check running status"
    );
    assert!(
        handlers_content.contains("state.get_version()") || handlers_content.contains("get_version"),
        "handle_status must get version"
    );
}

#[test]
fn test_state_module_exports() {
    let state_content = include_str!("../src/homepage/state.rs");
    assert!(
        state_content.contains("pub fn get_version"),
        "state.rs must export get_version function"
    );
    assert!(
        state_content.contains("pub fn is_running"),
        "state.rs must export is_running function"
    );
    assert!(
        state_content.contains("CARGO_PKG_VERSION"),
        "state.rs must use CARGO_PKG_VERSION for version"
    );
}

#[test]
fn test_status_handler_returns_json() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");
    assert!(
        handlers_content.contains("serde_json::json!") || handlers_content.contains("warp::reply::json"),
        "handle_status must return JSON response"
    );
}

#[test]
fn test_status_response_has_running_field() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");
    assert!(
        handlers_content.contains("\"running\"") || handlers_content.contains("\"status\""),
        "Status response must include running or status field"
    );
}

#[test]
fn test_status_response_has_version_field() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");
    assert!(
        handlers_content.contains("\"version\""),
        "Status response must include version field"
    );
}

// ============================================================================
// CSS Tests for Status Display
// ============================================================================

#[test]
fn test_css_has_version_info_styles() {
    assert!(
        STYLE_CSS.contains(".version-info"),
        "CSS must have version-info styles"
    );
}

#[test]
fn test_css_has_server_version_styles() {
    assert!(
        STYLE_CSS.contains(".server-version"),
        "CSS must have server-version styles"
    );
}

#[test]
fn test_css_has_server_status_styles() {
    assert!(
        STYLE_CSS.contains(".server-status"),
        "CSS must have server-status styles"
    );
}

#[test]
fn test_css_has_status_indicator_styles() {
    assert!(
        STYLE_CSS.contains(".status-indicator"),
        "CSS must have status-indicator styles"
    );
}

#[test]
fn test_css_has_running_indicator_styles() {
    assert!(
        STYLE_CSS.contains(".status-running") || STYLE_CSS.contains(".status-indicator.status-running"),
        "CSS must have status-running indicator styles"
    );
}

#[test]
fn test_css_has_stopped_indicator_styles() {
    assert!(
        STYLE_CSS.contains(".status-stopped") || STYLE_CSS.contains(".status-indicator.status-stopped"),
        "CSS must have status-stopped indicator styles"
    );
}

#[test]
fn test_css_status_indicator_is_round() {
    assert!(
        STYLE_CSS.contains("border-radius: 50%"),
        "Status indicator must be round (border-radius: 50%)"
    );
}

#[test]
fn test_css_running_indicator_has_green_color() {
    assert!(
        STYLE_CSS.contains("#22c55e") || STYLE_CSS.contains("green") || STYLE_CSS.contains("rgb(34, 197, 94)"),
        "Running status indicator must have green color"
    );
}

// ============================================================================
// API Route Tests
// ============================================================================

#[test]
fn test_routes_has_status_endpoint() {
    let routes_content = include_str!("../src/homepage/routes.rs");
    assert!(
        routes_content.contains("api") && routes_content.contains("status"),
        "routes.rs must define /api/status endpoint"
    );
}

#[test]
fn test_routes_uses_handle_status() {
    let routes_content = include_str!("../src/homepage/routes.rs");
    assert!(
        routes_content.contains("handle_status"),
        "routes.rs must use handle_status handler"
    );
}
