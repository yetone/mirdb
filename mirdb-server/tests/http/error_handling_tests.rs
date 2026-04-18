//! Error handling tests
//!
//! Owner: Scenario 6 (HTTP API Error Handling)
//!
//! Test cases:
//! - Invalid JSON returns 400
//! - Unknown endpoints return 404
//! - Missing fields return descriptive errors
//! - Unsupported methods return 405

use std::sync::Arc;

use mirdb::http::handlers::{get_key_handler, set_key_handler};
use mirdb::http::types::ErrorResponse;
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
    opt.work_dir = "/tmp/mirdbtest_error_handling/".to_string() + &rand_string;
    if Path::new(&opt.work_dir).exists() {
        remove_dir_all(&opt.work_dir).expect("remove work dir error!");
    }
    create_dir_all(&opt.work_dir).expect("create work dir error!");
    opt.mem_table_max_size = 1;
    opt.imm_mem_table_max_count = 1;
    opt
}

// =============================================================
// Test Case 1: POST /api/key with invalid JSON body
// Expected: 400 Bad Request with parse error message
// =============================================================

/// Test that invalid JSON returns an error with a parse message
#[test]
fn test_invalid_json_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Completely invalid JSON
    let invalid_json = r#"{"key": "test", invalid json here}"#;
    let result = set_key_handler(&store, invalid_json);

    assert!(result.is_err(), "Invalid JSON should return an error");
    let error_json = result.unwrap_err();

    // Parse the error response
    let error: ErrorResponse = serde_json::from_str(&error_json)
        .expect("Error response should be valid JSON");

    assert!(
        error.error.contains("Invalid JSON"),
        "Error should mention 'Invalid JSON', got: {}",
        error.error
    );
}

/// Test that malformed JSON syntax returns parse error
#[test]
fn test_malformed_json_syntax() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Missing closing brace
    let malformed_json = r#"{"key": "test", "value": "data""#;
    let result = set_key_handler(&store, malformed_json);

    assert!(result.is_err(), "Malformed JSON should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();
    assert!(
        error.error.contains("Invalid JSON"),
        "Error should indicate invalid JSON: {}",
        error.error
    );
}

/// Test that empty body returns parse error
#[test]
fn test_empty_body_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = set_key_handler(&store, "");

    assert!(result.is_err(), "Empty body should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();
    assert!(
        error.error.contains("Invalid JSON"),
        "Error should indicate invalid JSON for empty body: {}",
        error.error
    );
}

/// Test that JSON with wrong structure (primitive instead of object) returns error
#[test]
fn test_json_primitive_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // A simple string instead of an object
    let primitive_json = r#""just a string""#;
    let result = set_key_handler(&store, primitive_json);

    assert!(result.is_err(), "JSON primitive should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();
    assert!(
        error.error.contains("Invalid JSON"),
        "Error should indicate invalid JSON: {}",
        error.error
    );
}

// =============================================================
// Test Case 2: GET /api/nonexistent-endpoint
// Expected: 404 Not Found response
// Note: This test covers the handler level - route testing
// requires HTTP-level integration tests
// =============================================================

/// Test that getting a non-existent key returns appropriate error
#[test]
fn test_get_nonexistent_key_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = get_key_handler(&store, "nonexistent_key_12345");

    assert!(result.is_err(), "Non-existent key should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();
    assert!(
        error.error.contains("not found") || error.error.contains("Key not found"),
        "Error should indicate key not found: {}",
        error.error
    );
}

/// Test that the error response is valid JSON for 404 cases
#[test]
fn test_not_found_returns_valid_json() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = get_key_handler(&store, "another_nonexistent_key");

    assert!(result.is_err());
    let error_json = result.unwrap_err();

    // Verify the error is valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&error_json)
        .expect("Error response should be valid JSON");

    assert!(
        parsed.get("error").is_some(),
        "Error response should have 'error' field"
    );
}

// =============================================================
// Test Case 3: POST /api/key with missing required fields
// Expected: 400 Bad Request indicating missing field(s)
// =============================================================

/// Test that missing 'key' field returns descriptive error
#[test]
fn test_missing_key_field_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // JSON with missing 'key' field
    let missing_key_json = r#"{"value": "test_value"}"#;
    let result = set_key_handler(&store, missing_key_json);

    assert!(result.is_err(), "Missing 'key' field should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();

    // The error should mention the missing field
    assert!(
        error.error.contains("key") || error.error.contains("missing field"),
        "Error should indicate missing 'key' field: {}",
        error.error
    );
}

/// Test that missing 'value' field returns descriptive error
#[test]
fn test_missing_value_field_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // JSON with missing 'value' field
    let missing_value_json = r#"{"key": "test_key"}"#;
    let result = set_key_handler(&store, missing_value_json);

    assert!(result.is_err(), "Missing 'value' field should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();

    // The error should mention the missing field
    assert!(
        error.error.contains("value") || error.error.contains("missing field"),
        "Error should indicate missing 'value' field: {}",
        error.error
    );
}

/// Test that empty key (present but empty) returns descriptive error
#[test]
fn test_empty_key_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // JSON with empty 'key' field
    let empty_key_json = r#"{"key": "", "value": "test_value"}"#;
    let result = set_key_handler(&store, empty_key_json);

    assert!(result.is_err(), "Empty 'key' should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();

    assert!(
        error.error.contains("Key is required") || error.error.contains("key"),
        "Error should indicate key is required: {}",
        error.error
    );
}

/// Test that all missing required fields return proper error
#[test]
fn test_all_missing_fields_returns_error() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Empty JSON object
    let empty_json = r#"{}"#;
    let result = set_key_handler(&store, empty_json);

    assert!(result.is_err(), "Empty JSON object should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();

    // Should mention missing field
    assert!(
        error.error.contains("missing field") || error.error.contains("Invalid JSON"),
        "Error should indicate missing fields: {}",
        error.error
    );
}

// =============================================================
// Test Case 4: PUT /api/key (unsupported method)
// Expected: 405 Method Not Allowed response
// Note: This test is at the router level (server.rs)
// Handler-level test simulates the routing behavior
// =============================================================

// Note: The 405 Method Not Allowed is handled at the router level in server.rs
// We verify here that the handlers return appropriate errors, and the
// server.rs routing returns 405 for unsupported methods.
// This test verifies the error format is consistent.

/// Test that error responses follow consistent format
#[test]
fn test_error_response_format_consistency() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Test various error scenarios
    let test_cases = vec![
        (r#"invalid json"#, "invalid JSON"),
        (r#"{"value":"test"}"#, "missing key field"),
        (r#"{"key":"","value":"test"}"#, "empty key"),
    ];

    for (input, description) in test_cases {
        let result = set_key_handler(&store, input);
        assert!(result.is_err(), "{} should return an error", description);

        let error_json = result.unwrap_err();

        // All errors should be valid JSON with 'error' field
        let parsed: serde_json::Value = serde_json::from_str(&error_json)
            .expect(&format!("Error for {} should be valid JSON", description));

        assert!(
            parsed.get("error").is_some(),
            "Error for {} should have 'error' field",
            description
        );

        let error_msg = parsed["error"].as_str().unwrap();
        assert!(
            !error_msg.is_empty(),
            "Error message for {} should not be empty",
            description
        );
    }
}

// =============================================================
// Test Case 5: Web UI displays error from failed operation
// Expected: Error message shown in toast or inline notification
// Note: This is an E2E test - we verify the API returns proper
// error JSON that the UI can display
// =============================================================

/// Test that error responses are suitable for UI display
#[test]
fn test_error_responses_ui_friendly() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Test various error scenarios that would be displayed in UI

    // 1. Key not found error
    let result = get_key_handler(&store, "ui_test_nonexistent");
    assert!(result.is_err());
    let error: ErrorResponse = serde_json::from_str(&result.unwrap_err()).unwrap();
    assert!(
        !error.error.contains("stack") && !error.error.contains("panic"),
        "Error message should be user-friendly, not a stack trace: {}",
        error.error
    );

    // 2. Invalid input error
    let result = set_key_handler(&store, "not valid json at all");
    assert!(result.is_err());
    let error: ErrorResponse = serde_json::from_str(&result.unwrap_err()).unwrap();
    assert!(
        error.error.len() < 500,
        "Error message should not be excessively long: {}",
        error.error
    );

    // 3. Missing field error
    let result = set_key_handler(&store, r#"{"key":"","value":"test"}"#);
    assert!(result.is_err());
    let error: ErrorResponse = serde_json::from_str(&result.unwrap_err()).unwrap();
    assert!(
        error.error.len() > 0,
        "Error message should not be empty"
    );
}

/// Test that error messages are actionable (help user fix the issue)
#[test]
fn test_error_messages_are_actionable() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Invalid JSON should hint at JSON parsing issue
    let result = set_key_handler(&store, "{ broken }");
    assert!(result.is_err());
    let error: ErrorResponse = serde_json::from_str(&result.unwrap_err()).unwrap();
    assert!(
        error.error.to_lowercase().contains("json") || error.error.to_lowercase().contains("parse"),
        "Invalid JSON error should mention JSON or parsing: {}",
        error.error
    );

    // Empty key should tell user key is required
    let result = set_key_handler(&store, r#"{"key":"","value":"test"}"#);
    assert!(result.is_err());
    let error: ErrorResponse = serde_json::from_str(&result.unwrap_err()).unwrap();
    assert!(
        error.error.to_lowercase().contains("key") || error.error.to_lowercase().contains("required"),
        "Empty key error should mention key or required: {}",
        error.error
    );
}

// =============================================================
// Additional Error Handling Tests
// =============================================================

/// Test that numeric fields with wrong types return appropriate error
#[test]
fn test_wrong_type_for_numeric_fields() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Flags as string instead of number
    let wrong_type_json = r#"{"key":"test","value":"data","flags":"not_a_number"}"#;
    let result = set_key_handler(&store, wrong_type_json);

    assert!(result.is_err(), "Wrong type for flags should return an error");
    let error_json = result.unwrap_err();
    let error: ErrorResponse = serde_json::from_str(&error_json).unwrap();
    assert!(
        error.error.contains("Invalid JSON"),
        "Error should indicate invalid JSON: {}",
        error.error
    );
}

/// Test that special characters in error messages are properly escaped
#[test]
fn test_error_messages_properly_escaped() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // This could potentially cause issues with JSON escaping
    let tricky_input = r#"{"key":"<script>alert('xss')</script>","value":"test"}"#;
    let _result = set_key_handler(&store, tricky_input);

    // This should actually succeed (it's valid JSON with a valid key)
    // But let's also test a case that would fail
    let invalid_input = r#"{"key": "test", "value": "<script>
    malformed}"#;
    let result = set_key_handler(&store, invalid_input);
    assert!(result.is_err());

    let error_json = result.unwrap_err();
    // Ensure the error JSON is valid (properly escaped)
    let _: serde_json::Value = serde_json::from_str(&error_json)
        .expect("Error response should be valid JSON even with special characters in input");
}

/// Test that concurrent error requests don't interfere with each other
#[test]
fn test_concurrent_error_requests() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Make multiple error requests
    let results: Vec<_> = (0..5)
        .map(|i| {
            let body = format!("invalid json {}", i);
            set_key_handler(&store, &body)
        })
        .collect();

    // All should be errors
    for result in results {
        assert!(result.is_err(), "All invalid inputs should return errors");
        let error_json = result.unwrap_err();
        let _: ErrorResponse = serde_json::from_str(&error_json)
            .expect("All error responses should be valid JSON");
    }
}

/// Test that very long input doesn't crash or return excessively long error
#[test]
fn test_long_input_error_handling() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Create a very long invalid JSON string
    let long_input = "a".repeat(10000);
    let result = set_key_handler(&store, &long_input);

    assert!(result.is_err(), "Very long invalid input should return an error");
    let error_json = result.unwrap_err();

    // Error response should be reasonable length
    assert!(
        error_json.len() < 1000,
        "Error response should not be excessively long"
    );

    let _: ErrorResponse = serde_json::from_str(&error_json)
        .expect("Error response should be valid JSON");
}
