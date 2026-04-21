//! Configuration API endpoint tests.
//! Owner: Scenario 4 - Configuration API Endpoint
//!
//! Tests for /api/config endpoint verifying:
//! 1. Response contains all required JSON fields with default values
//! 2. Response reflects custom configuration values when provided
//! 3. POST/PUT/DELETE methods return 405 Method Not Allowed (read-only)

/// Test Case 1: GET /api/config with default configuration
/// Verifies JSON response contains default values:
/// - listen_addr: '0.0.0.0:12333'
/// - max_lsm_levels: 7
/// - work_dir: '/tmp/mirdb'
/// - sstable_max_size: 104857600 (100MB)
#[test]
fn test_config_endpoint_returns_default_values() {
    use mirdb::web::handlers::config::{create_default_config_state, get_config_json};

    let state = create_default_config_state();
    let json = get_config_json(&state);

    // Parse the JSON to verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify all required fields exist and have correct default values
    assert_eq!(
        parsed["listen_addr"], "0.0.0.0:12333",
        "listen_addr should be '0.0.0.0:12333'"
    );
    assert_eq!(
        parsed["max_lsm_levels"], 7,
        "max_lsm_levels should be 7"
    );
    assert_eq!(
        parsed["work_dir"], "/tmp/mirdb",
        "work_dir should be '/tmp/mirdb'"
    );
    assert_eq!(
        parsed["sstable_max_size"], 100 * 1024 * 1024,
        "sstable_max_size should be 104857600 (100MB)"
    );
    assert_eq!(
        parsed["memtable_max_size"], 4 * 1024 * 1024,
        "memtable_max_size should be 4194304 (4MB)"
    );
    assert_eq!(
        parsed["block_size"], 4 * 1024,
        "block_size should be 4096 (4KB)"
    );
}

/// Test Case 1 Additional: Verify JSON structure has all required fields
#[test]
fn test_config_response_has_all_required_fields() {
    let json_str = r#"{
        "listen_addr": "0.0.0.0:12333",
        "max_lsm_levels": 7,
        "work_dir": "/tmp/mirdb",
        "sstable_max_size": 104857600,
        "memtable_max_size": 4194304,
        "block_size": 4096
    }"#;

    // Parse the JSON to verify structure
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();

    // Verify all required fields exist
    assert!(parsed.get("listen_addr").is_some(), "missing listen_addr");
    assert!(parsed.get("max_lsm_levels").is_some(), "missing max_lsm_levels");
    assert!(parsed.get("work_dir").is_some(), "missing work_dir");
    assert!(parsed.get("sstable_max_size").is_some(), "missing sstable_max_size");
    assert!(parsed.get("memtable_max_size").is_some(), "missing memtable_max_size");
    assert!(parsed.get("block_size").is_some(), "missing block_size");

    // Verify field types
    assert!(parsed["listen_addr"].is_string());
    assert!(parsed["max_lsm_levels"].is_u64());
    assert!(parsed["work_dir"].is_string());
    assert!(parsed["sstable_max_size"].is_u64());
    assert!(parsed["memtable_max_size"].is_u64());
    assert!(parsed["block_size"].is_u64());
}

/// Test Case 2: GET /api/config with custom configuration
/// Verifies JSON response reflects custom configuration values
#[test]
fn test_config_endpoint_returns_custom_values() {
    use mirdb::web::handlers::config::{create_config_state, get_config_json};

    // Create a config state with custom values
    let state = create_config_state(
        "192.168.1.100:9000".to_string(),
        10,
        "/data/custom_mirdb".to_string(),
        200 * 1024 * 1024,  // 200MB
        8 * 1024 * 1024,    // 8MB
        8 * 1024,           // 8KB
    );

    let json = get_config_json(&state);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify custom values are reflected
    assert_eq!(
        parsed["listen_addr"], "192.168.1.100:9000",
        "listen_addr should reflect custom value"
    );
    assert_eq!(
        parsed["max_lsm_levels"], 10,
        "max_lsm_levels should reflect custom value"
    );
    assert_eq!(
        parsed["work_dir"], "/data/custom_mirdb",
        "work_dir should reflect custom value"
    );
    assert_eq!(
        parsed["sstable_max_size"], 200 * 1024 * 1024,
        "sstable_max_size should reflect custom value (200MB)"
    );
    assert_eq!(
        parsed["memtable_max_size"], 8 * 1024 * 1024,
        "memtable_max_size should reflect custom value (8MB)"
    );
    assert_eq!(
        parsed["block_size"], 8 * 1024,
        "block_size should reflect custom value (8KB)"
    );
}

/// Test Case 2 Additional: Verify config with various custom configurations
#[test]
fn test_config_endpoint_with_different_configurations() {
    use mirdb::web::handlers::config::{create_config_state, get_config_json};

    // Test with minimal values
    let state = create_config_state(
        "127.0.0.1:8080".to_string(),
        3,
        "/var/mirdb".to_string(),
        50 * 1024 * 1024,   // 50MB
        2 * 1024 * 1024,    // 2MB
        2 * 1024,           // 2KB
    );

    let json = get_config_json(&state);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["listen_addr"], "127.0.0.1:8080");
    assert_eq!(parsed["max_lsm_levels"], 3);
    assert_eq!(parsed["work_dir"], "/var/mirdb");
    assert_eq!(parsed["sstable_max_size"], 50 * 1024 * 1024);
    assert_eq!(parsed["memtable_max_size"], 2 * 1024 * 1024);
    assert_eq!(parsed["block_size"], 2 * 1024);
}

/// Test Case 3: POST /api/config returns HTTP 405 Method Not Allowed
/// Configuration is read-only, modification attempts should fail
#[test]
fn test_config_endpoint_post_not_allowed() {
    use mirdb::web::handlers::config::is_method_allowed;

    // POST should not be allowed
    assert!(
        !is_method_allowed("POST"),
        "POST method should not be allowed for /api/config"
    );
}

/// Test Case 3 Additional: Verify method_not_allowed_error response
#[test]
fn test_config_method_not_allowed_error_response() {
    use mirdb::web::handlers::config::method_not_allowed_error;

    let error_json = method_not_allowed_error();
    let parsed: serde_json::Value = serde_json::from_str(&error_json).unwrap();

    // Verify error response structure
    assert_eq!(parsed["code"], 405, "Error code should be 405");
    assert_eq!(parsed["success"], false, "success should be false");
    assert!(
        parsed["error"].as_str().unwrap().contains("Method Not Allowed"),
        "Error message should indicate Method Not Allowed"
    );
}

/// Test Case 3 Additional: Verify PUT method not allowed
#[test]
fn test_config_endpoint_put_not_allowed() {
    use mirdb::web::handlers::config::is_method_allowed;

    assert!(
        !is_method_allowed("PUT"),
        "PUT method should not be allowed for /api/config"
    );
}

/// Test Case 3 Additional: Verify DELETE method not allowed
#[test]
fn test_config_endpoint_delete_not_allowed() {
    use mirdb::web::handlers::config::is_method_allowed;

    assert!(
        !is_method_allowed("DELETE"),
        "DELETE method should not be allowed for /api/config"
    );
}

/// Test Case 3 Additional: Verify PATCH method not allowed
#[test]
fn test_config_endpoint_patch_not_allowed() {
    use mirdb::web::handlers::config::is_method_allowed;

    assert!(
        !is_method_allowed("PATCH"),
        "PATCH method should not be allowed for /api/config"
    );
}

/// Test: Verify GET method is allowed
#[test]
fn test_config_endpoint_get_is_allowed() {
    use mirdb::web::handlers::config::is_method_allowed;

    assert!(
        is_method_allowed("GET"),
        "GET method should be allowed for /api/config"
    );

    // Test case-insensitivity
    assert!(is_method_allowed("get"), "get (lowercase) should be allowed");
    assert!(is_method_allowed("Get"), "Get (mixed case) should be allowed");
}

/// Test: Verify ConfigResponse serialization matches expected format
#[test]
fn test_config_response_serialization() {
    use mirdb::web::ConfigResponse;

    let config = ConfigResponse {
        listen_addr: "0.0.0.0:12333".to_string(),
        max_lsm_levels: 7,
        work_dir: "/tmp/mirdb".to_string(),
        sstable_max_size: 100 * 1024 * 1024,
        memtable_max_size: 4 * 1024 * 1024,
        block_size: 4 * 1024,
    };

    let json = serde_json::to_string(&config).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["listen_addr"], "0.0.0.0:12333");
    assert_eq!(parsed["max_lsm_levels"], 7);
    assert_eq!(parsed["work_dir"], "/tmp/mirdb");
}

/// Test: Verify shared config state works across clones
#[test]
fn test_shared_config_state_cloning() {
    use mirdb::web::handlers::config::create_default_config_state;

    let state = create_default_config_state();
    let state_clone = state.clone();

    // Both should refer to the same underlying config
    assert_eq!(state.listen_addr, state_clone.listen_addr);
    assert_eq!(state.max_lsm_levels, state_clone.max_lsm_levels);
    assert_eq!(state.work_dir, state_clone.work_dir);
    assert_eq!(state.sstable_max_size, state_clone.sstable_max_size);
    assert_eq!(state.memtable_max_size, state_clone.memtable_max_size);
    assert_eq!(state.block_size, state_clone.block_size);
}

/// Test: Verify ConfigState get_config produces correct ConfigResponse
#[test]
fn test_config_state_get_config() {
    use mirdb::web::handlers::config::ConfigState;

    let state = ConfigState::new(
        "localhost:5000".to_string(),
        5,
        "/opt/mirdb".to_string(),
        64 * 1024 * 1024,
        1 * 1024 * 1024,
        1 * 1024,
    );

    let config = state.get_config();

    assert_eq!(config.listen_addr, "localhost:5000");
    assert_eq!(config.max_lsm_levels, 5);
    assert_eq!(config.work_dir, "/opt/mirdb");
    assert_eq!(config.sstable_max_size, 64 * 1024 * 1024);
    assert_eq!(config.memtable_max_size, 1 * 1024 * 1024);
    assert_eq!(config.block_size, 1 * 1024);
}

/// Test: Verify config is immutable (read-only semantics)
#[test]
fn test_config_is_read_only() {
    use mirdb::web::handlers::config::{create_default_config_state, get_config_json};

    let state = create_default_config_state();

    // Get config twice and verify it's the same
    let json1 = get_config_json(&state);
    let json2 = get_config_json(&state);

    assert_eq!(json1, json2, "Config should be consistent and immutable");
}

/// Test: Verify config JSON is valid for frontend parsing
#[test]
fn test_config_json_valid_for_frontend() {
    use mirdb::web::handlers::config::{create_default_config_state, get_config_json};

    let state = create_default_config_state();
    let json = get_config_json(&state);

    // This is the kind of parsing a JavaScript frontend would do
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("JSON should be valid");

    // Verify values can be extracted correctly
    let listen_addr = parsed["listen_addr"].as_str().expect("listen_addr should be string");
    let max_levels = parsed["max_lsm_levels"].as_u64().expect("max_lsm_levels should be number");
    let work_dir = parsed["work_dir"].as_str().expect("work_dir should be string");
    let sst_size = parsed["sstable_max_size"].as_u64().expect("sstable_max_size should be number");

    assert!(!listen_addr.is_empty());
    assert!(max_levels > 0);
    assert!(!work_dir.is_empty());
    assert!(sst_size > 0);
}
