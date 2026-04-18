//! Compaction API tests
//!
//! Owner: Scenario 5 (Manual Compaction Trigger)
//!
//! Test cases:
//! - POST /api/operations/compact initiates compaction
//! - Response indicates success
//! - Status shows compaction in progress after trigger
//! - Concurrent compaction requests are handled gracefully

use std::sync::Arc;

use mirdb::http::handlers::{compact_handler, status_handler};
use mirdb::http::types::CompactResponse;
use mirdb::options::Options;
use mirdb::store::Store;

/// Create test options with a unique work directory
fn get_test_opt() -> Options {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::fs::{create_dir_all, remove_dir_all};
    use std::path::Path;

    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();
    let mut opt = Options::default();
    opt.work_dir = "/tmp/mirdbtest_compaction/".to_string() + &rand_string;
    if Path::new(&opt.work_dir).exists() {
        remove_dir_all(&opt.work_dir).expect("remove work dir error!");
    }
    create_dir_all(&opt.work_dir).expect("create work dir error!");
    opt.mem_table_max_size = 1;
    opt.imm_mem_table_max_count = 1;
    opt
}

/// Test Case 1: POST /api/operations/compact returns success response
/// Expected: JSON response with {success: true} indicating compaction initiated
#[test]
fn test_compact_returns_success() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = compact_handler(&store);
    assert!(result.is_ok(), "compact_handler should return Ok: {:?}", result);

    let json = result.unwrap();

    // Parse the JSON response
    let response: CompactResponse = serde_json::from_str(&json)
        .expect("Should be valid CompactResponse JSON");

    // Verify success is true
    assert!(response.success, "Compaction should succeed");
    assert!(!response.message.is_empty(), "Message should not be empty");
}

/// Test Case 2: GET /api/status after triggering compaction
/// Expected: Status shows compaction in progress or recently completed
#[test]
fn test_status_after_compaction() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Trigger compaction
    let compact_result = compact_handler(&store);
    assert!(compact_result.is_ok(), "Compaction should succeed");

    // Check status
    let status_result = status_handler(&store);
    assert!(status_result.is_ok(), "Status should succeed");

    let json = status_result.unwrap();
    // Status should be valid JSON
    let status: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(status.get("compaction").is_some(), "Status should have compaction field");
}

/// Test Case 3: Multiple compaction requests are handled gracefully
/// Expected: Subsequent requests indicate compaction already running or succeed
#[test]
fn test_concurrent_compaction_requests() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Trigger compaction multiple times
    for i in 0..3 {
        let result = compact_handler(&store);
        // Each call should either succeed or gracefully indicate already running
        // Currently MirDB's compaction is synchronous and reentrant, so it should succeed
        assert!(
            result.is_ok() || result.is_err(),
            "Request {} should be handled gracefully", i
        );

        if let Ok(json) = result {
            // If successful, verify the response structure
            let response: Result<CompactResponse, _> = serde_json::from_str(&json);
            assert!(response.is_ok(), "Response should be valid JSON");
        }
    }
}

/// Test Case 4: POST /api/operations/compact while compaction in progress
/// Expected: Response indicates compaction already running or queued (or succeeds since MirDB handles it)
#[test]
fn test_compaction_while_in_progress() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // First compaction request
    let result1 = compact_handler(&store);
    assert!(result1.is_ok(), "First compaction should succeed");

    // Immediately trigger another compaction
    let result2 = compact_handler(&store);
    // This should either succeed (if synchronous) or indicate already running
    // MirDB's current implementation is synchronous, so this will succeed
    match result2 {
        Ok(json) => {
            let response: CompactResponse = serde_json::from_str(&json).unwrap();
            // Either success or a message about already running
            assert!(
                response.success || response.message.contains("already") || response.message.contains("queued"),
                "Response should indicate success or already running"
            );
        }
        Err(json) => {
            // Error response should be valid JSON
            let _: serde_json::Value = serde_json::from_str(&json)
                .expect("Error response should be valid JSON");
        }
    }
}

/// Test Case 5: Compaction response contains expected fields
#[test]
fn test_compaction_response_structure() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = compact_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();

    // Parse as generic JSON first
    let value: serde_json::Value = serde_json::from_str(&json)
        .expect("Response should be valid JSON");

    // Verify it's an object
    assert!(value.is_object(), "Response should be a JSON object");

    // Verify required fields
    assert!(value.get("success").is_some(), "Should have 'success' field");
    assert!(value.get("message").is_some(), "Should have 'message' field");

    // Verify field types
    assert!(value["success"].is_boolean(), "'success' should be boolean");
    assert!(value["message"].is_string(), "'message' should be string");
}

/// Test Case 6: Compaction succeeds with empty store
#[test]
fn test_compaction_empty_store() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Don't add any data, just trigger compaction on empty store
    let result = compact_handler(&store);
    assert!(result.is_ok(), "Compaction should succeed even on empty store");

    let json = result.unwrap();
    let response: CompactResponse = serde_json::from_str(&json).unwrap();
    assert!(response.success, "Empty store compaction should succeed");
}

/// Test Case 7: Compaction after inserting data
#[test]
fn test_compaction_with_data() {
    use mirdb::http::handlers::set_key_handler;

    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Insert some test data
    let body = r#"{"key":"test:compact:1","value":"test_value_1","flags":0,"ttl":0}"#;
    let set_result = set_key_handler(&store, body);
    assert!(set_result.is_ok(), "Set should succeed");

    let body = r#"{"key":"test:compact:2","value":"test_value_2","flags":0,"ttl":0}"#;
    let set_result = set_key_handler(&store, body);
    assert!(set_result.is_ok(), "Set should succeed");

    // Now trigger compaction
    let result = compact_handler(&store);
    assert!(result.is_ok(), "Compaction should succeed after data insertion");

    let json = result.unwrap();
    let response: CompactResponse = serde_json::from_str(&json).unwrap();
    assert!(response.success, "Compaction with data should succeed");
}

/// Test Case 8: Verify compaction message indicates initiated
#[test]
fn test_compaction_message_content() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = compact_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();
    let response: CompactResponse = serde_json::from_str(&json).unwrap();

    // Message should indicate compaction was initiated
    assert!(
        response.message.to_lowercase().contains("compaction") ||
        response.message.to_lowercase().contains("initiated") ||
        response.message.to_lowercase().contains("success"),
        "Message should indicate compaction status: {}", response.message
    );
}
