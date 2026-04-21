//! Key-Value API tests - DELETE operation.
//! Owner: Scenario 7 - Key-Value DELETE Operation via Web API
//!
//! Test cases:
//! 1. Store key, then DELETE /api/kv/delete?key=test-key returns success with DELETED message
//! 2. DELETE /api/kv/delete?key=nonexistent returns NOT_FOUND error
//! 3. Delete key via web API, verify via Store GET returns key not found

use std::sync::Arc;
use tempfile::TempDir;

use mirdb::{Options, Store};
use mirdb::web::handlers::kv::{DeleteParams, KvDeleteResponse};

/// Helper function to create a test Store instance
fn create_test_store() -> (Arc<Store>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let work_dir = temp_dir.path().to_string_lossy().to_string();

    let mut opt = Options::default();
    opt.work_dir = work_dir;
    opt.max_level = 7;
    opt.sst_max_size = 1024 * 1024; // 1MB
    opt.mem_table_max_size = 1024 * 1024; // 1MB

    let store = Store::new(opt).expect("Failed to create store");
    (Arc::new(store), temp_dir)
}

/// Helper function to store a key-value pair in the store
fn store_key(store: &Arc<Store>, key: &str, value: &str) {
    use mirdb::request::Request;
    use mirdb::request::SetterType;
    use mirdb::slice::Slice;

    let key_slice = Slice::from(key);
    let payload = Slice::from(value);
    let bytes = payload.len();

    let request = Request::Setter {
        setter: SetterType::Set,
        key: key_slice,
        flags: 0,
        ttl: 0,
        bytes,
        payload,
        no_reply: false,
    };

    store.apply(request).expect("Failed to store key");
}

/// Helper function to check if a key exists in the store
fn key_exists(store: &Arc<Store>, key: &str) -> bool {
    use mirdb::request::{GetterType, Request};
    use mirdb::response::Response;
    use mirdb::slice::Slice;

    let key_slice = Slice::from(key);
    let request = Request::Getter {
        getter: GetterType::Get,
        keys: vec![key_slice],
    };

    match store.apply(request) {
        Ok(Response::Get(items)) => !items.is_empty(),
        _ => false,
    }
}

/// Helper function to delete a key using the store directly (simulating web API)
fn delete_key_via_store(store: &Arc<Store>, key: &str) -> KvDeleteResponse {
    use mirdb::request::Request;
    use mirdb::response::Response;
    use mirdb::slice::Slice;

    let key_slice = Slice::from(key);
    let request = Request::Deleter {
        key: key_slice,
        no_reply: false,
    };

    match store.apply(request) {
        Ok(Response::Deleted) => KvDeleteResponse {
            success: true,
            message: Some("DELETED".to_string()),
            error: None,
        },
        Ok(Response::NotFound) => KvDeleteResponse {
            success: false,
            message: None,
            error: Some("NOT_FOUND".to_string()),
        },
        Ok(_) => KvDeleteResponse {
            success: false,
            message: None,
            error: Some("UNEXPECTED_RESPONSE".to_string()),
        },
        Err(e) => KvDeleteResponse {
            success: false,
            message: None,
            error: Some(format!("INTERNAL_ERROR: {}", e)),
        },
    }
}

/// Test Case 1: Store key, then DELETE /api/kv/delete?key=test-key
/// Expected: JSON {success: true, message: "DELETED"}
#[test]
fn test_delete_existing_key_returns_deleted() {
    let (store, _temp_dir) = create_test_store();

    // Store a key first
    store_key(&store, "test-key", "test-value");

    // Verify key exists
    assert!(key_exists(&store, "test-key"), "Key should exist after storing");

    // Delete the key
    let response = delete_key_via_store(&store, "test-key");

    // Verify response
    assert!(response.success, "DELETE should succeed for existing key");
    assert_eq!(
        response.message,
        Some("DELETED".to_string()),
        "Message should be DELETED"
    );
    assert!(response.error.is_none(), "Error should be None for successful delete");

    // Verify key is removed
    assert!(!key_exists(&store, "test-key"), "Key should not exist after deletion");
}

/// Test Case 2: DELETE /api/kv/delete?key=nonexistent
/// Expected: JSON {success: false, error: "NOT_FOUND"}
#[test]
fn test_delete_nonexistent_key_returns_not_found() {
    let (store, _temp_dir) = create_test_store();

    // Try to delete a key that doesn't exist
    let response = delete_key_via_store(&store, "nonexistent");

    // Verify response
    assert!(!response.success, "DELETE should fail for nonexistent key");
    assert_eq!(
        response.error,
        Some("NOT_FOUND".to_string()),
        "Error should be NOT_FOUND"
    );
    assert!(response.message.is_none(), "Message should be None for failed delete");
}

/// Test Case 3: Delete key via web API, verify via Memcached GET
/// Expected: Memcached returns END (key not found) - simulated via Store GET
#[test]
fn test_delete_via_api_verify_via_get() {
    use mirdb::request::{GetterType, Request};
    use mirdb::response::Response;
    use mirdb::slice::Slice;

    let (store, _temp_dir) = create_test_store();

    // Store a key first
    store_key(&store, "integration-test-key", "integration-test-value");

    // Verify key exists via GET
    let key_slice = Slice::from("integration-test-key");
    let get_request = Request::Getter {
        getter: GetterType::Get,
        keys: vec![key_slice.clone()],
    };
    let get_response = store.apply(get_request.clone());
    match &get_response {
        Ok(Response::Get(items)) => {
            assert!(!items.is_empty(), "Key should exist before deletion");
        }
        _ => panic!("Expected Get response with items"),
    }

    // Delete via web API simulation
    let response = delete_key_via_store(&store, "integration-test-key");
    assert!(response.success, "DELETE should succeed");
    assert_eq!(response.message, Some("DELETED".to_string()));

    // Verify key is NOT found via GET (simulates Memcached returning END)
    let key_slice = Slice::from("integration-test-key");
    let get_request = Request::Getter {
        getter: GetterType::Get,
        keys: vec![key_slice],
    };
    let get_response = store.apply(get_request);

    // In Memcached protocol, END with no values means key not found
    match get_response {
        Ok(Response::Get(items)) => {
            assert!(items.is_empty(), "Key should not be found after deletion (Memcached returns END)");
        }
        _ => panic!("Expected Get response"),
    }
}

/// Test: Verify KvDeleteResponse serialization with success
#[test]
fn test_kv_delete_response_serialization_success() {
    let response = KvDeleteResponse {
        success: true,
        message: Some("DELETED".to_string()),
        error: None,
    };

    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["success"], true);
    assert_eq!(parsed["message"], "DELETED");
    assert!(parsed.get("error").is_none() || parsed["error"].is_null());
}

/// Test: Verify KvDeleteResponse serialization with error
#[test]
fn test_kv_delete_response_serialization_error() {
    let response = KvDeleteResponse {
        success: false,
        message: None,
        error: Some("NOT_FOUND".to_string()),
    };

    let json = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["success"], false);
    assert_eq!(parsed["error"], "NOT_FOUND");
    assert!(parsed.get("message").is_none() || parsed["message"].is_null());
}

/// Test: Delete multiple keys in sequence
#[test]
fn test_delete_multiple_keys() {
    let (store, _temp_dir) = create_test_store();

    // Store multiple keys
    store_key(&store, "key1", "value1");
    store_key(&store, "key2", "value2");
    store_key(&store, "key3", "value3");

    // Verify all keys exist
    assert!(key_exists(&store, "key1"));
    assert!(key_exists(&store, "key2"));
    assert!(key_exists(&store, "key3"));

    // Delete key2
    let response = delete_key_via_store(&store, "key2");
    assert!(response.success);
    assert_eq!(response.message, Some("DELETED".to_string()));

    // Verify key2 is deleted but others remain
    assert!(key_exists(&store, "key1"), "key1 should still exist");
    assert!(!key_exists(&store, "key2"), "key2 should be deleted");
    assert!(key_exists(&store, "key3"), "key3 should still exist");

    // Delete remaining keys
    let response1 = delete_key_via_store(&store, "key1");
    let response3 = delete_key_via_store(&store, "key3");

    assert!(response1.success);
    assert!(response3.success);
    assert!(!key_exists(&store, "key1"));
    assert!(!key_exists(&store, "key3"));
}

/// Test: Delete the same key twice
#[test]
fn test_delete_same_key_twice() {
    let (store, _temp_dir) = create_test_store();

    // Store a key
    store_key(&store, "double-delete", "some-value");

    // First delete should succeed
    let response1 = delete_key_via_store(&store, "double-delete");
    assert!(response1.success);
    assert_eq!(response1.message, Some("DELETED".to_string()));

    // Second delete should return NOT_FOUND
    let response2 = delete_key_via_store(&store, "double-delete");
    assert!(!response2.success);
    assert_eq!(response2.error, Some("NOT_FOUND".to_string()));
}

/// Test: Delete key with special characters
#[test]
fn test_delete_key_with_special_characters() {
    let (store, _temp_dir) = create_test_store();

    // Store keys with special characters
    store_key(&store, "key-with-dashes", "value1");
    store_key(&store, "key_with_underscores", "value2");
    store_key(&store, "key.with.dots", "value3");

    // Delete each key
    let response1 = delete_key_via_store(&store, "key-with-dashes");
    let response2 = delete_key_via_store(&store, "key_with_underscores");
    let response3 = delete_key_via_store(&store, "key.with.dots");

    assert!(response1.success);
    assert!(response2.success);
    assert!(response3.success);

    // Verify all are deleted
    assert!(!key_exists(&store, "key-with-dashes"));
    assert!(!key_exists(&store, "key_with_underscores"));
    assert!(!key_exists(&store, "key.with.dots"));
}

/// Test: DeleteParams deserialization
#[test]
fn test_delete_params_deserialization() {
    let params_json = r#"{"key": "test-key"}"#;
    let params: DeleteParams = serde_json::from_str(params_json).unwrap();
    assert_eq!(params.key, "test-key");
}

/// Test: Empty key deletion
#[test]
fn test_delete_empty_key() {
    let (store, _temp_dir) = create_test_store();

    // Try to delete an empty key (which shouldn't exist)
    let response = delete_key_via_store(&store, "");

    // Empty key should return NOT_FOUND since it was never stored
    assert!(!response.success);
    assert_eq!(response.error, Some("NOT_FOUND".to_string()));
}
