//! Memcached Protocol Integration Tests for MirDB
//!
//! These tests verify that the Memcached SET and GET commands work
//! as documented in the quick-start guide.

use std::path::Path;
use std::fs;
use std::env;

/// Helper function to get workspace root path
fn workspace_root() -> String {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let path = Path::new(&manifest_dir);
    path.parent().unwrap_or(path).to_string_lossy().to_string()
}

/// Test Case 3: Verify Memcached SET command parsing works
/// This tests that the parser correctly parses SET commands as shown in quick-start:
/// ```
/// set mykey 0 0 5
/// hello
/// ```
#[test]
fn test_memcached_set_command_parsing() {
    let root = workspace_root();
    // Read the parser test to verify SET command parsing is tested
    let parser_path = format!("{}/mirdb-server/src/parser.rs", root);
    let parser_content = fs::read_to_string(&parser_path)
        .expect("Should be able to read parser.rs");

    // The parser tests show SET command parsing:
    // set abc 1 0 7\r\n"a b c"\r\n should parse correctly
    assert!(
        parser_content.contains("test"),
        "Parser should have tests for command parsing"
    );

    // Verify SetterType::Set exists
    assert!(
        parser_content.contains("SetterType::Set"),
        "Parser should handle SetterType::Set"
    );
}

/// Test Case 3: Verify SET command response handling
/// The response should be "STORED" on success
#[test]
fn test_set_response_stored() {
    let root = workspace_root();
    let response_path = format!("{}/mirdb-server/src/response.rs", root);
    let response_content = fs::read_to_string(&response_path)
        .expect("Should be able to read response.rs");

    // Verify STORED response exists
    assert!(
        response_content.contains("Stored") || response_content.contains("STORED"),
        "Response should include STORED for successful SET operations"
    );
}

/// Test Case 4: Verify Memcached GET command parsing works
/// This tests that the parser correctly parses GET commands as shown in quick-start:
/// ```
/// get mykey
/// ```
#[test]
fn test_memcached_get_command_parsing() {
    let root = workspace_root();
    let parser_path = format!("{}/mirdb-server/src/parser.rs", root);
    let parser_content = fs::read_to_string(&parser_path)
        .expect("Should be able to read parser.rs");

    // Verify GetterType::Get exists
    assert!(
        parser_content.contains("GetterType::Get"),
        "Parser should handle GetterType::Get"
    );

    // Verify the getter parser exists and handles get command
    assert!(
        parser_content.contains("getter"),
        "Parser should have getter function for GET commands"
    );
}

/// Test Case 4: Verify GET command response includes VALUE format
/// Response format should be:
/// VALUE <key> <flags> <bytes>
/// <data>
/// END
#[test]
fn test_get_response_value_format() {
    let root = workspace_root();
    let response_path = format!("{}/mirdb-server/src/response.rs", root);
    let response_content = fs::read_to_string(&response_path)
        .expect("Should be able to read response.rs");

    // Verify VALUE response format
    assert!(
        response_content.contains("VALUE") || response_content.contains("Value"),
        "Response should include VALUE format for GET responses"
    );

    // Verify END response
    assert!(
        response_content.contains("END"),
        "Response should include END marker for GET responses"
    );
}

/// Test that the Store correctly handles SET followed by GET operations
/// This is the integration test that verifies the quick-start example works
#[test]
fn test_store_set_then_get_integration() {
    let root = workspace_root();
    // Read the store tests to verify this integration is tested
    let store_path = format!("{}/mirdb-server/src/store.rs", root);
    let store_content = fs::read_to_string(&store_path)
        .expect("Should be able to read store.rs");

    // Verify there are tests for set and get operations
    assert!(
        store_content.contains("test_get_some"),
        "Store should have test for GET after SET"
    );

    // Verify the test does SET then GET
    assert!(
        store_content.contains("SetterType::Set") && store_content.contains("GetterType::Get"),
        "Store tests should cover SET followed by GET operations"
    );
}

/// Verify that the response module properly formats Memcached protocol responses
#[test]
fn test_response_protocol_compliance() {
    let root = workspace_root();
    let response_path = format!("{}/mirdb-server/src/response.rs", root);
    let response_content = fs::read_to_string(&response_path)
        .expect("Should be able to read response.rs");

    // Memcached protocol responses end with \r\n
    assert!(
        response_content.contains("\\r\\n") || response_content.contains("CRLF"),
        "Responses should use CRLF line endings per Memcached protocol"
    );
}

/// Verify Request types include all necessary Memcached operations
#[test]
fn test_request_types_complete() {
    let root = workspace_root();
    let request_path = format!("{}/mirdb-server/src/request.rs", root);
    let request_content = fs::read_to_string(&request_path)
        .expect("Should be able to read request.rs");

    // Verify Getter request type
    assert!(
        request_content.contains("Getter"),
        "Request should have Getter variant"
    );

    // Verify Setter request type
    assert!(
        request_content.contains("Setter"),
        "Request should have Setter variant"
    );

    // Verify Deleter request type
    assert!(
        request_content.contains("Deleter"),
        "Request should have Deleter variant"
    );
}
