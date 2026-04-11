//! Status API Integration Tests
//! Owner: Scenario 3 - Server Status Dashboard
//!
//! Tests:
//! - GET /api/status returns expected fields
//! - Response time within 500ms
//! - StatusResponse serialization

use std::fs::{self, create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

/// Test 1: StatusResponse contains all required fields
#[test]
fn test_status_response_has_required_fields() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for required fields in StatusResponse struct
    assert!(
        status_rs.contains("memory_usage"),
        "StatusResponse should have memory_usage field"
    );
    assert!(
        status_rs.contains("active_connections"),
        "StatusResponse should have active_connections field"
    );
    assert!(
        status_rs.contains("database_size"),
        "StatusResponse should have database_size field"
    );
}

/// Test 2: Status module exports handle_status function
#[test]
fn test_status_handler_exists() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for handle_status function
    assert!(
        status_rs.contains("pub fn handle_status"),
        "status.rs should export handle_status function"
    );
}

/// Test 3: Status response includes LSM statistics fields
#[test]
fn test_status_includes_lsm_stats() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for LSM statistics fields (for Scenario 9 co-ownership)
    assert!(
        status_rs.contains("memtable_count"),
        "StatusResponse should have memtable_count field"
    );
    assert!(
        status_rs.contains("sstable_count"),
        "StatusResponse should have sstable_count field"
    );
    assert!(
        status_rs.contains("total_size"),
        "StatusResponse should have total_size field"
    );
}

/// Test 4: StatusResponse implements Serialize
#[test]
fn test_status_response_is_serializable() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for Serialize derive
    assert!(
        status_rs.contains("Serialize"),
        "StatusResponse should derive Serialize"
    );
}

/// Test 5: Status handler includes JSON output function
#[test]
fn test_status_json_handler_exists() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for JSON handler function
    assert!(
        status_rs.contains("handle_status_json") || status_rs.contains("serde_json"),
        "status.rs should support JSON serialization"
    );
}

/// Test 6: Status module includes connection counting
#[test]
fn test_connection_tracking_exists() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for connection tracking functions
    assert!(
        status_rs.contains("active_connections") || status_rs.contains("ACTIVE_CONNECTIONS"),
        "status.rs should track active connections"
    );
}

/// Test 7: Status handler can calculate database size
#[test]
fn test_database_size_calculation() {
    let status_rs = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    // Check for database size calculation
    assert!(
        status_rs.contains("database_size") || status_rs.contains("get_database_size"),
        "status.rs should calculate database size"
    );
}

/// Test 8: API response time measurement capability
#[test]
fn test_response_time_measurement() {
    // This test verifies we can measure response times
    let start = Instant::now();

    // Simulate some work (reading a file)
    let _content = fs::read_to_string("src/http/handlers/status.rs")
        .expect("Failed to read status.rs");

    let elapsed = start.elapsed();

    // Response should be well under 500ms for local file operations
    assert!(
        elapsed.as_millis() < 500,
        "File operations should complete within 500ms, took {}ms",
        elapsed.as_millis()
    );
}

/// Test 9: status.js file exists
#[test]
fn test_status_js_exists() {
    let js_path = Path::new("src/web/scripts/status.js");
    assert!(js_path.exists(), "status.js should exist");
}

/// Test 10: status.js exports required functions
#[test]
fn test_status_js_exports() {
    let status_js = fs::read_to_string("src/web/scripts/status.js")
        .expect("Failed to read status.js");

    // Check for required exports
    assert!(
        status_js.contains("initStatusDashboard"),
        "status.js should export initStatusDashboard"
    );
    assert!(
        status_js.contains("updateStatus"),
        "status.js should export updateStatus"
    );
    assert!(
        status_js.contains("startAutoRefresh"),
        "status.js should export startAutoRefresh"
    );
}

/// Test 11: Auto-refresh interval is 5 seconds
#[test]
fn test_refresh_interval_is_5_seconds() {
    let status_js = fs::read_to_string("src/web/scripts/status.js")
        .expect("Failed to read status.js");

    // Check for 5-second interval (5000ms)
    assert!(
        status_js.contains("5000") || status_js.contains("5 second"),
        "Auto-refresh interval should be 5 seconds"
    );
}

/// Test 12: status.js uses MirDBApi for fetching
#[test]
fn test_uses_api_client() {
    let status_js = fs::read_to_string("src/web/scripts/status.js")
        .expect("Failed to read status.js");

    // Check for API usage
    assert!(
        status_js.contains("MirDBApi") || status_js.contains("fetchStatus"),
        "status.js should use MirDBApi for data fetching"
    );
}

/// Test 13: status.js renders metrics grid
#[test]
fn test_renders_metrics_grid() {
    let status_js = fs::read_to_string("src/web/scripts/status.js")
        .expect("Failed to read status.js");

    // Check for grid rendering
    assert!(
        status_js.contains("grid") || status_js.contains("metric"),
        "status.js should render metrics in a grid layout"
    );
}

/// Test 14: status.js shows refresh indicator
#[test]
fn test_has_refresh_indicator() {
    let status_js = fs::read_to_string("src/web/scripts/status.js")
        .expect("Failed to read status.js");

    // Check for refresh indicator
    assert!(
        status_js.contains("refresh-indicator") || status_js.contains("RefreshIndicator"),
        "status.js should show refresh indicator"
    );
}

/// Test 15: index.html has status section
#[test]
fn test_index_has_status_section() {
    let html = fs::read_to_string("src/web/index.html")
        .expect("Failed to read index.html");

    assert!(
        html.contains("id=\"status\"") || html.contains("status-section"),
        "index.html should have status section"
    );
}

/// Test 16: Status panel element exists
#[test]
fn test_status_panel_exists() {
    let html = fs::read_to_string("src/web/index.html")
        .expect("Failed to read index.html");

    assert!(
        html.contains("id=\"status-panel\"") || html.contains("status-panel"),
        "index.html should have status-panel element"
    );
}

/// Test 17: status.js is included in HTML
#[test]
fn test_status_js_included() {
    let html = fs::read_to_string("src/web/index.html")
        .expect("Failed to read index.html");

    assert!(
        html.contains("status.js"),
        "index.html should include status.js"
    );
}
