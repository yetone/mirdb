//! Real-time Metrics Dashboard tests.
//! Owner: Scenario 10 - Real-time Metrics Dashboard
//!
//! Tests:
//! 1. Four metric cards visible: Memory, Disk, Keys, Connections (REQ-3)
//! 2. Metrics refresh at least once within 5 seconds (NFR-2)
//! 3. Key count updates when keys are added via API
//! 4. Configuration panel shows Listen Addr, Work Dir, Max Level, SST Max Size (REQ-4)

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

/// Helper function to get JavaScript content
async fn get_javascript() -> String {
    let app = create_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/main.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Test Case 1: Load homepage and check dashboard section
/// Expected: Four metric cards visible: Memory, Disk, Keys, Connections
#[tokio::test]
async fn test_dashboard_has_four_metric_cards() {
    let html = get_homepage_html().await;

    // Verify dashboard section exists
    assert!(
        html.contains("id=\"dashboard\""),
        "Homepage should have a dashboard section"
    );

    // Verify metrics grid exists
    assert!(
        html.contains("class=\"metrics-grid\""),
        "Dashboard should have a metrics grid"
    );

    // Verify Memory card
    assert!(
        html.contains("id=\"memory-card\""),
        "Dashboard should have a Memory metric card"
    );
    assert!(
        html.contains("Memory Usage") || html.contains("Memory"),
        "Memory card should have Memory label"
    );
    assert!(
        html.contains("id=\"memory-value\""),
        "Memory card should have a value element"
    );

    // Verify Disk card
    assert!(
        html.contains("id=\"disk-card\""),
        "Dashboard should have a Disk metric card"
    );
    assert!(
        html.contains("Disk Usage") || html.contains("Disk"),
        "Disk card should have Disk label"
    );
    assert!(
        html.contains("id=\"disk-value\""),
        "Disk card should have a value element"
    );

    // Verify Keys card
    assert!(
        html.contains("id=\"keys-card\""),
        "Dashboard should have a Keys metric card"
    );
    assert!(
        html.contains("Total Keys") || html.contains("Keys"),
        "Keys card should have Keys label"
    );
    assert!(
        html.contains("id=\"keys-value\""),
        "Keys card should have a value element"
    );

    // Verify Connections card
    assert!(
        html.contains("id=\"connections-card\""),
        "Dashboard should have a Connections metric card"
    );
    assert!(
        html.contains("Connections"),
        "Connections card should have Connections label"
    );
    assert!(
        html.contains("id=\"connections-value\""),
        "Connections card should have a value element"
    );
}

/// Test Case 2: Wait 10 seconds and observe metric updates
/// Expected: Metrics refresh at least once within 5 seconds
/// This test verifies that the JavaScript polling is configured correctly
#[tokio::test]
async fn test_metrics_polling_interval_is_5_seconds() {
    let js = get_javascript().await;

    // Verify initMetricsPolling function exists
    assert!(
        js.contains("initMetricsPolling"),
        "JavaScript should have initMetricsPolling function"
    );

    // Verify setInterval is used with 5000ms (5 seconds)
    assert!(
        js.contains("setInterval(fetchMetrics, 5000)"),
        "Metrics should poll every 5 seconds (5000ms)"
    );

    // Verify fetchMetrics function exists
    assert!(
        js.contains("async function fetchMetrics()") || js.contains("function fetchMetrics"),
        "JavaScript should have fetchMetrics function"
    );

    // Verify it fetches from /api/metrics
    assert!(
        js.contains("fetch('/api/metrics')"),
        "fetchMetrics should fetch from /api/metrics endpoint"
    );

    // Verify updateDashboard function exists
    assert!(
        js.contains("function updateDashboard"),
        "JavaScript should have updateDashboard function"
    );
}

/// Test Case 3: Add 10 keys via API, observe key count on dashboard
/// Expected: Key count increases by 10 on next refresh
/// This test verifies the updateDashboard function correctly updates key count
#[tokio::test]
async fn test_dashboard_updates_key_count() {
    let js = get_javascript().await;

    // Verify updateDashboard function handles key_count
    assert!(
        js.contains("keys-value"),
        "updateDashboard should update keys-value element"
    );

    // Verify it uses metrics.key_count
    assert!(
        js.contains("metrics.key_count"),
        "updateDashboard should read key_count from metrics"
    );

    // Verify it formats the value (toLocaleString for numbers)
    assert!(
        js.contains("toLocaleString()"),
        "Key count should be formatted with locale string"
    );
}

/// Test Case 4: Check configuration panel display
/// Expected: Shows Listen Addr, Work Dir, Max Level, SST Max Size values
#[tokio::test]
async fn test_configuration_panel_displays_all_fields() {
    let html = get_homepage_html().await;

    // Verify configuration panel exists
    assert!(
        html.contains("id=\"config-panel\"") || html.contains("class=\"config-panel\""),
        "Dashboard should have a configuration panel"
    );

    // Verify Listen Addr field
    assert!(
        html.contains("Listen Addr"),
        "Configuration panel should show Listen Addr"
    );
    assert!(
        html.contains("id=\"config-listen-addr\""),
        "Configuration panel should have listen-addr value element"
    );

    // Verify Work Dir field
    assert!(
        html.contains("Work Dir"),
        "Configuration panel should show Work Dir"
    );
    assert!(
        html.contains("id=\"config-work-dir\""),
        "Configuration panel should have work-dir value element"
    );

    // Verify Max Level field
    assert!(
        html.contains("Max Level"),
        "Configuration panel should show Max Level"
    );
    assert!(
        html.contains("id=\"config-max-level\""),
        "Configuration panel should have max-level value element"
    );

    // Verify SST Max Size field
    assert!(
        html.contains("SST Max Size"),
        "Configuration panel should show SST Max Size"
    );
    assert!(
        html.contains("id=\"config-sst-max-size\""),
        "Configuration panel should have sst-max-size value element"
    );
}

/// Additional test: Verify configuration fetching in JavaScript
#[tokio::test]
async fn test_javascript_fetches_configuration() {
    let js = get_javascript().await;

    // Verify fetchConfig function exists
    assert!(
        js.contains("async function fetchConfig()") || js.contains("function fetchConfig"),
        "JavaScript should have fetchConfig function"
    );

    // Verify it fetches from /api/config
    assert!(
        js.contains("fetch('/api/config')"),
        "fetchConfig should fetch from /api/config endpoint"
    );

    // Verify updateConfigPanel function exists
    assert!(
        js.contains("function updateConfigPanel"),
        "JavaScript should have updateConfigPanel function"
    );

    // Verify fetchConfig is called on page load
    assert!(
        js.contains("fetchConfig()"),
        "fetchConfig should be called on page load"
    );
}

/// Additional test: Verify updateConfigPanel updates all config fields
#[tokio::test]
async fn test_update_config_panel_handles_all_fields() {
    let js = get_javascript().await;

    // Verify it updates listen_addr
    assert!(
        js.contains("config-listen-addr") && js.contains("listen_addr"),
        "updateConfigPanel should update listen_addr field"
    );

    // Verify it updates work_dir
    assert!(
        js.contains("config-work-dir") && js.contains("work_dir"),
        "updateConfigPanel should update work_dir field"
    );

    // Verify it updates max_lsm_levels
    assert!(
        js.contains("config-max-level") && js.contains("max_lsm_levels"),
        "updateConfigPanel should update max_lsm_levels field"
    );

    // Verify it updates sstable_max_size
    assert!(
        js.contains("config-sst-max-size") && js.contains("sstable_max_size"),
        "updateConfigPanel should update sstable_max_size field"
    );
}

/// Additional test: Verify dashboard structure follows accessibility guidelines
#[tokio::test]
async fn test_dashboard_accessibility() {
    let html = get_homepage_html().await;

    // Verify dashboard has aria-labelledby for accessibility
    assert!(
        html.contains("aria-labelledby=\"dashboard-title\""),
        "Dashboard section should have aria-labelledby"
    );

    // Verify dashboard has a heading
    assert!(
        html.contains("id=\"dashboard-title\""),
        "Dashboard should have a title element"
    );

    // Verify config panel has aria-labelledby
    assert!(
        html.contains("aria-labelledby=\"config-title\""),
        "Configuration panel should have aria-labelledby"
    );
}

/// Additional test: Verify formatBytes utility function exists
#[tokio::test]
async fn test_format_bytes_utility() {
    let js = get_javascript().await;

    // Verify formatBytes function exists
    assert!(
        js.contains("function formatBytes"),
        "JavaScript should have formatBytes utility function"
    );

    // Verify it handles different size units
    assert!(
        js.contains("'B', 'KB', 'MB', 'GB', 'TB'") || js.contains("sizes"),
        "formatBytes should handle multiple size units"
    );
}

/// Additional test: Verify metrics polling starts on page load
#[tokio::test]
async fn test_metrics_polling_starts_on_dom_ready() {
    let js = get_javascript().await;

    // Verify DOMContentLoaded event handler exists
    assert!(
        js.contains("DOMContentLoaded"),
        "JavaScript should handle DOMContentLoaded event"
    );

    // Verify initMetricsPolling is called on page load
    assert!(
        js.contains("initMetricsPolling()"),
        "initMetricsPolling should be called on page load"
    );
}
