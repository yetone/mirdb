//! Health check API endpoint tests.
//! Owner: Scenario 8 - Health Check Endpoint
//!
//! Tests for /api/health endpoint verifying:
//! 1. GET /api/health returns valid JSON with expected fields
//! 2. Response includes status, server_running, and compaction_status
//! 3. Compaction status reflects actual running state

/// Test Case 1: GET /api/health on healthy server
/// Verifies JSON response structure:
/// - status: 'healthy'
/// - server_running: true
/// - compaction_status: 'idle'
#[test]
fn test_health_endpoint_returns_healthy_status() {
    use mirdb::web::handlers::health::{get_health_json, HealthState};

    let state = HealthState::new();
    let json = get_health_json(&state);

    // Parse the JSON to verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify all required fields exist and have correct values
    assert_eq!(
        parsed["status"], "healthy",
        "status should be 'healthy'"
    );
    assert_eq!(
        parsed["server_running"], true,
        "server_running should be true"
    );
    assert_eq!(
        parsed["compaction_status"], "idle",
        "compaction_status should be 'idle' on healthy server"
    );
}

/// Test Case 2: GET /api/health during compaction
/// Verifies compaction_status is 'running' when compaction is active
#[test]
fn test_health_endpoint_shows_compaction_running() {
    use mirdb::web::handlers::health::{get_health_json, HealthState};

    let state = HealthState::new();
    state.set_compaction_running(true);

    let json = get_health_json(&state);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify compaction status is running
    assert_eq!(
        parsed["compaction_status"], "running",
        "compaction_status should be 'running' during compaction"
    );

    // Server should still be healthy during compaction
    assert_eq!(
        parsed["status"], "healthy",
        "status should still be 'healthy' during compaction"
    );
}

/// Test Case 3: Health check response time is fast (< 100ms)
/// Verifies the health check is a lightweight operation
#[test]
fn test_health_check_is_fast() {
    use mirdb::web::handlers::health::{is_health_check_fast, HealthState};
    use std::time::Instant;

    let state = HealthState::new();
    let start = Instant::now();

    // Simulate health check operations
    let _health = state.get_health();

    // Verify the operation completed within 100ms
    assert!(
        is_health_check_fast(start),
        "Health check should complete within 100ms"
    );
}

/// Test: Health state correctly tracks server running status
#[test]
fn test_health_state_server_status() {
    use mirdb::web::handlers::health::HealthState;

    let state = HealthState::new();

    // Initially running
    assert!(state.is_server_running(), "Server should be running by default");

    // Set to not running
    state.set_server_running(false);
    assert!(!state.is_server_running(), "Server should not be running after set");

    let health = state.get_health();
    assert_eq!(health.status, "unhealthy", "Status should be unhealthy when server is not running");
}

/// Test: Health state correctly tracks compaction status
#[test]
fn test_health_state_compaction_status() {
    use mirdb::web::handlers::health::HealthState;

    let state = HealthState::new();

    // Initially not compacting
    assert!(!state.is_compaction_running(), "Compaction should not be running by default");

    // Start compaction
    state.set_compaction_running(true);
    assert!(state.is_compaction_running(), "Compaction should be running after set");

    let health = state.get_health();
    assert_eq!(health.compaction_status, "running", "Compaction status should be 'running'");

    // Stop compaction
    state.set_compaction_running(false);
    let health = state.get_health();
    assert_eq!(health.compaction_status, "idle", "Compaction status should be 'idle' after stop");
}

/// Test: Shared health state works across threads
#[test]
fn test_shared_health_state() {
    use mirdb::web::handlers::health::create_health_state;

    let state = create_health_state();
    let state_clone = state.clone();

    // Modify through one reference
    state.set_compaction_running(true);

    // Read through another reference
    assert!(
        state_clone.is_compaction_running(),
        "Shared state should reflect changes"
    );
}

/// Test: Health response JSON serialization
#[test]
fn test_health_json_serialization() {
    use mirdb::web::handlers::health::{get_health_json, HealthState};

    let state = HealthState::new();
    let json = get_health_json(&state);

    // Verify JSON is well-formed
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(&json);
    assert!(parsed.is_ok(), "Health JSON should be valid");

    // Verify expected fields
    let value = parsed.unwrap();
    assert!(value.get("status").is_some(), "JSON should have status field");
    assert!(value.get("server_running").is_some(), "JSON should have server_running field");
    assert!(value.get("compaction_status").is_some(), "JSON should have compaction_status field");
}

/// Test: get_health_with_state function returns correct response
#[test]
fn test_get_health_with_state() {
    use mirdb::web::handlers::health::{get_health_with_state, HealthState};

    let state = HealthState::new();
    state.set_compaction_running(true);

    let health = get_health_with_state(&state);

    assert_eq!(health.status, "healthy");
    assert!(health.server_running);
    assert_eq!(health.compaction_status, "running");
}
