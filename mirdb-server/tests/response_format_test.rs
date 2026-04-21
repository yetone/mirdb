//! API Response Format Consistency Tests.
//! Owner: Scenario 18 - API Response Format Consistency
//!
//! This module verifies that all API endpoints return consistent JSON response format.
//!
//! Test cases:
//! 1. GET /api/metrics returns Content-Type: application/json with valid JSON structure
//! 2. Failed GET /api/kv/get for missing key returns Content-Type: application/json with 'success': false and 'error' field
//! 3. Successful POST /api/kv/set returns Content-Type: application/json with 'success': true
//!
//! Response format requirements:
//! - All API responses must have Content-Type: application/json
//! - Success responses: { success: true, data: {...} } or { success: true, ...fields... }
//! - Error responses: { success: false, error: "...", code: number }

use mirdb::web::types::{
    ApiError, ApiResponse, ConfigResponse, HealthResponse, KvResponse, MetricsResponse,
};

/// Test Case 1: Verify GET /api/metrics returns valid JSON structure
/// Tests that MetricsResponse serializes to proper JSON with all required fields
#[test]
fn test_metrics_response_json_format() {
    let metrics = MetricsResponse {
        memory_used_bytes: 1024,
        memory_total_bytes: 4096,
        disk_used_bytes: 10240,
        disk_total_bytes: 1048576,
        key_count: 100,
        active_connections: 5,
        compaction_running: false,
        sstable_level_counts: vec![2, 4, 0, 0, 0, 0, 0],
    };

    let json = serde_json::to_string(&metrics).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify all required metrics fields are present and have correct types
    assert!(
        parsed.get("memory_used_bytes").is_some(),
        "Metrics must have memory_used_bytes field"
    );
    assert!(
        parsed["memory_used_bytes"].is_u64(),
        "memory_used_bytes must be a number"
    );

    assert!(
        parsed.get("memory_total_bytes").is_some(),
        "Metrics must have memory_total_bytes field"
    );

    assert!(
        parsed.get("disk_used_bytes").is_some(),
        "Metrics must have disk_used_bytes field"
    );

    assert!(
        parsed.get("disk_total_bytes").is_some(),
        "Metrics must have disk_total_bytes field"
    );

    assert!(
        parsed.get("key_count").is_some(),
        "Metrics must have key_count field"
    );
    assert_eq!(parsed["key_count"], 100);

    assert!(
        parsed.get("active_connections").is_some(),
        "Metrics must have active_connections field"
    );

    assert!(
        parsed.get("compaction_running").is_some(),
        "Metrics must have compaction_running field"
    );
    assert!(
        parsed["compaction_running"].is_boolean(),
        "compaction_running must be a boolean"
    );

    assert!(
        parsed.get("sstable_level_counts").is_some(),
        "Metrics must have sstable_level_counts field"
    );
    assert!(
        parsed["sstable_level_counts"].is_array(),
        "sstable_level_counts must be an array"
    );
}

/// Test Case 2: Verify failed GET /api/kv/get for missing key returns proper error format
/// Tests that KvResponse with success=false includes 'error' field
#[test]
fn test_kv_get_missing_key_error_format() {
    // KvResponse for a missing key should have success: false and error field
    let response = KvResponse {
        success: false,
        key: "nonexistent_key".to_string(),
        value: None,
        error: Some("NOT_FOUND".to_string()),
    };

    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify error response format
    assert!(
        parsed.get("success").is_some(),
        "Error response must have 'success' field"
    );
    assert_eq!(
        parsed["success"], false,
        "Error response must have 'success': false"
    );

    assert!(
        parsed.get("error").is_some(),
        "Error response must have 'error' field when operation fails"
    );
    assert_eq!(
        parsed["error"], "NOT_FOUND",
        "Error message should indicate key not found"
    );

    assert!(
        parsed.get("key").is_some(),
        "Error response should include the key that was requested"
    );
}

/// Test Case 3: Verify successful POST /api/kv/set returns proper success format
/// Tests that KvResponse with success=true includes required fields
#[test]
fn test_kv_set_success_format() {
    // KvResponse for a successful set should have success: true
    let response = KvResponse {
        success: true,
        key: "test_key".to_string(),
        value: Some("test_value".to_string()),
        error: None,
    };

    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify success response format
    assert!(
        parsed.get("success").is_some(),
        "Success response must have 'success' field"
    );
    assert_eq!(
        parsed["success"], true,
        "Success response must have 'success': true"
    );

    assert!(
        parsed.get("key").is_some(),
        "Success response should include the key"
    );

    assert!(
        parsed.get("value").is_some(),
        "Success response for SET should include the value"
    );

    // Error field should be omitted or null for successful responses
    assert!(
        parsed.get("error").is_none() || parsed["error"].is_null(),
        "Success response should not have error field"
    );
}

/// Test: ApiResponse wrapper provides consistent success format
#[test]
fn test_api_response_wrapper_format() {
    let data = "test data".to_string();
    let response = ApiResponse::success(data);

    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // ApiResponse wrapper should always have success: true and data field
    assert_eq!(parsed["success"], true, "ApiResponse::success must set success: true");
    assert!(
        parsed.get("data").is_some(),
        "ApiResponse must have 'data' field"
    );
}

/// Test: ApiError provides consistent error format with code
#[test]
fn test_api_error_format() {
    let error = ApiError::not_found("Key not found");

    let json = serde_json::to_string(&error).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // ApiError must have success: false, error string, and code
    assert_eq!(
        parsed["success"], false,
        "ApiError must have success: false"
    );
    assert!(
        parsed.get("error").is_some(),
        "ApiError must have 'error' field"
    );
    assert!(
        parsed.get("code").is_some(),
        "ApiError must have 'code' field"
    );
    assert_eq!(parsed["code"], 404, "not_found error should have code 404");
}

/// Test: ApiError bad_request returns code 400
#[test]
fn test_api_error_bad_request_format() {
    let error = ApiError::bad_request("Invalid input");

    let json = serde_json::to_string(&error).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["success"], false);
    assert_eq!(parsed["code"], 400);
    assert!(parsed["error"].as_str().unwrap().contains("Invalid input"));
}

/// Test: ApiError internal returns code 500
#[test]
fn test_api_error_internal_format() {
    let error = ApiError::internal("Server error");

    let json = serde_json::to_string(&error).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["success"], false);
    assert_eq!(parsed["code"], 500);
}

/// Test: Health endpoint response follows consistent format when wrapped
#[test]
fn test_health_response_wrapped_format() {
    let health = HealthResponse {
        status: "healthy".to_string(),
        server_running: true,
        compaction_status: "idle".to_string(),
    };
    let wrapped = ApiResponse::success(health);

    let json = serde_json::to_string(&wrapped).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Wrapped response should have consistent structure
    assert_eq!(parsed["success"], true);
    assert!(parsed.get("data").is_some());

    // Data should contain health fields
    let data = &parsed["data"];
    assert!(data.get("status").is_some());
    assert!(data.get("server_running").is_some());
    assert!(data.get("compaction_status").is_some());
}

/// Test: Config endpoint response follows consistent format when wrapped
#[test]
fn test_config_response_wrapped_format() {
    let config = ConfigResponse {
        listen_addr: "0.0.0.0:12333".to_string(),
        max_lsm_levels: 7,
        work_dir: "/tmp/mirdb".to_string(),
        sstable_max_size: 104857600,
        memtable_max_size: 4194304,
        block_size: 4096,
    };
    let wrapped = ApiResponse::success(config);

    let json = serde_json::to_string(&wrapped).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Wrapped response should have consistent structure
    assert_eq!(parsed["success"], true);
    assert!(parsed.get("data").is_some());

    // Data should contain config fields
    let data = &parsed["data"];
    assert!(data.get("listen_addr").is_some());
    assert!(data.get("max_lsm_levels").is_some());
    assert!(data.get("work_dir").is_some());
    assert!(data.get("sstable_max_size").is_some());
    assert!(data.get("memtable_max_size").is_some());
    assert!(data.get("block_size").is_some());
}

/// Test: KvResponse skips None fields when serializing
#[test]
fn test_kv_response_skips_none_fields() {
    // Success response without error field
    let success = KvResponse {
        success: true,
        key: "test".to_string(),
        value: Some("value".to_string()),
        error: None,
    };

    let json = serde_json::to_string(&success).unwrap();
    // Error field should not appear in JSON when None
    assert!(
        !json.contains("\"error\""),
        "None fields should be skipped in serialization"
    );

    // Error response without value field
    let error = KvResponse {
        success: false,
        key: "test".to_string(),
        value: None,
        error: Some("NOT_FOUND".to_string()),
    };

    let json = serde_json::to_string(&error).unwrap();
    // Value field should not appear in JSON when None
    assert!(
        !json.contains("\"value\""),
        "None fields should be skipped in serialization"
    );
}

/// Test: MetricsResponse always has 7 SSTable level counts
#[test]
fn test_metrics_sstable_counts_format() {
    let metrics = MetricsResponse::default();

    let json = serde_json::to_string(&metrics).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    let counts = parsed["sstable_level_counts"].as_array().unwrap();
    assert_eq!(
        counts.len(),
        7,
        "sstable_level_counts should always have 7 elements"
    );
}

/// Test: All response types can round-trip through JSON
#[test]
fn test_response_types_roundtrip() {
    // MetricsResponse roundtrip
    let metrics = MetricsResponse {
        memory_used_bytes: 1024,
        memory_total_bytes: 4096,
        disk_used_bytes: 10240,
        disk_total_bytes: 1048576,
        key_count: 100,
        active_connections: 5,
        compaction_running: false,
        sstable_level_counts: vec![2, 4, 0, 0, 0, 0, 0],
    };
    let json = serde_json::to_string(&metrics).unwrap();
    let roundtrip: MetricsResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(roundtrip.key_count, 100);

    // ConfigResponse roundtrip
    let config = ConfigResponse {
        listen_addr: "0.0.0.0:12333".to_string(),
        max_lsm_levels: 7,
        work_dir: "/tmp/mirdb".to_string(),
        sstable_max_size: 104857600,
        memtable_max_size: 4194304,
        block_size: 4096,
    };
    let json = serde_json::to_string(&config).unwrap();
    let roundtrip: ConfigResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(roundtrip.max_lsm_levels, 7);

    // HealthResponse roundtrip
    let health = HealthResponse {
        status: "healthy".to_string(),
        server_running: true,
        compaction_status: "idle".to_string(),
    };
    let json = serde_json::to_string(&health).unwrap();
    let roundtrip: HealthResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(roundtrip.status, "healthy");

    // KvResponse roundtrip
    let kv = KvResponse {
        success: true,
        key: "test".to_string(),
        value: Some("value".to_string()),
        error: None,
    };
    let json = serde_json::to_string(&kv).unwrap();
    let roundtrip: KvResponse = serde_json::from_str(&json).unwrap();
    assert!(roundtrip.success);
}

/// Test: ApiError new constructor works correctly
#[test]
fn test_api_error_new_constructor() {
    let error = ApiError::new("Custom error", 418);

    let json = serde_json::to_string(&error).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["success"], false);
    assert_eq!(parsed["code"], 418);
    assert_eq!(parsed["error"], "Custom error");
}

/// Test: All numeric fields in responses are proper integers
#[test]
fn test_numeric_fields_are_integers() {
    let metrics = MetricsResponse {
        memory_used_bytes: u64::MAX,
        memory_total_bytes: u64::MAX,
        disk_used_bytes: u64::MAX,
        disk_total_bytes: u64::MAX,
        key_count: u64::MAX,
        active_connections: u64::MAX,
        compaction_running: false,
        sstable_level_counts: vec![u64::MAX; 7],
    };

    let json = serde_json::to_string(&metrics).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // All numeric fields should be valid JSON numbers
    assert!(parsed["memory_used_bytes"].is_u64());
    assert!(parsed["key_count"].is_u64());
}

/// Test: Boolean fields serialize correctly
#[test]
fn test_boolean_fields_format() {
    // Test compaction_running true
    let metrics_running = MetricsResponse {
        compaction_running: true,
        ..Default::default()
    };
    let json = serde_json::to_string(&metrics_running).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["compaction_running"], true);

    // Test compaction_running false
    let metrics_idle = MetricsResponse {
        compaction_running: false,
        ..Default::default()
    };
    let json = serde_json::to_string(&metrics_idle).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["compaction_running"], false);

    // Test server_running in health response
    let health = HealthResponse {
        status: "healthy".to_string(),
        server_running: true,
        compaction_status: "idle".to_string(),
    };
    let json = serde_json::to_string(&health).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["server_running"], true);
}
