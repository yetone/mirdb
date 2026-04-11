//! Key CRUD API Tests
//! Owner: Scenario 4 - Key Browser View Keys
//! Co-owners: Scenarios 5, 6, 7
//!
//! Tests:
//! - List keys with pagination
//! - Get single key
//! - Set new key
//! - Delete key
//! - Cross-protocol consistency

use std::sync::Arc;

/// Test helper to create a test store
fn create_test_store() -> Arc<mirdb::store::Store> {
    let mut opts = mirdb::options::Options::default();

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .collect();

    opts.work_dir = format!("/tmp/mirdbtest_keys/{}", rand_string);
    std::fs::create_dir_all(&opts.work_dir).expect("create work dir");
    opts.mem_table_max_size = 1;
    opts.imm_mem_table_max_count = 1;

    Arc::new(mirdb::store::Store::new(opts).unwrap())
}

/// Test helper to insert a key into the store
fn insert_key(store: &Arc<mirdb::store::Store>, key: &str, value: &str, flags: u32, ttl: u32) {
    use mirdb::request::{Request, SetterType};
    use mirdb::slice::Slice;

    let key_slice = Slice::from(key);
    let value_slice = Slice::from(value);
    let request = Request::Setter {
        setter: SetterType::Set,
        key: key_slice,
        flags,
        ttl,
        bytes: value_slice.len(),
        payload: value_slice,
        no_reply: false,
    };
    store.apply(request).unwrap();
}

#[test]
fn test_list_keys_returns_json_array() {
    let store = create_test_store();

    // Insert test keys
    insert_key(&store, "test_key_1", "value1", 0, 0);
    insert_key(&store, "test_key_2", "value2", 0, 0);
    insert_key(&store, "test_key_3", "value3", 0, 0);

    // Get keys list
    let (keys, total) = store.list_keys(0, 10).unwrap();

    // Verify response structure
    assert_eq!(total, 3, "Should have 3 keys total");
    assert_eq!(keys.len(), 3, "Should return 3 keys");

    // Verify each key has required fields
    for (key, payload) in &keys {
        assert!(!key.is_empty(), "Key should not be empty");
        assert!(payload.bytes > 0, "Key should have size > 0");
    }
}

#[test]
fn test_list_keys_pagination() {
    let store = create_test_store();

    // Insert more keys than the page size
    for i in 0..12 {
        insert_key(&store, &format!("pkey_{:02}", i), &format!("value{}", i), 0, 0);
    }

    // Get first page (limit 10)
    let (page1_keys, total) = store.list_keys(0, 10).unwrap();
    assert_eq!(total, 12, "Total should be 12");
    assert_eq!(page1_keys.len(), 10, "First page should have 10 keys");

    // Get second page (skip 10, limit 10)
    let (page2_keys, total2) = store.list_keys(10, 10).unwrap();
    assert_eq!(total2, 12, "Total should still be 12");
    assert_eq!(page2_keys.len(), 2, "Second page should have 2 keys");
}

#[test]
fn test_list_keys_empty_store() {
    let store = create_test_store();

    // Get keys from empty store
    let (keys, total) = store.list_keys(0, 10).unwrap();

    assert_eq!(total, 0, "Total should be 0 for empty store");
    assert_eq!(keys.len(), 0, "Should return empty array");
}

#[test]
fn test_get_single_key() {
    use mirdb::http::handlers::keys::{handle_get_key, GetKeyResult};

    let store = create_test_store();

    // Insert a key
    insert_key(&store, "my_key", "my_value", 42, 3600);

    // Get the key via handler
    let result = handle_get_key(Arc::clone(&store), "my_key".to_string());

    match result {
        GetKeyResult::Found(response) => {
            assert_eq!(response.key, "my_key");
            assert_eq!(response.value, "my_value");
            assert_eq!(response.flags, 42);
            assert_eq!(response.size, 8); // "my_value".len()
        }
        _ => panic!("Expected Found result"),
    }
}

#[test]
fn test_get_nonexistent_key() {
    use mirdb::http::handlers::keys::{handle_get_key, GetKeyResult};

    let store = create_test_store();

    // Get a key that doesn't exist
    let result = handle_get_key(store, "nonexistent_key".to_string());

    match result {
        GetKeyResult::NotFound(error) => {
            assert_eq!(error.error, "not_found");
            assert!(error.message.contains("nonexistent_key"));
        }
        _ => panic!("Expected NotFound result"),
    }
}

#[test]
fn test_keys_response_includes_size_and_ttl() {
    use mirdb::http::handlers::keys::{handle_list_keys, ListKeysResult};

    let store = create_test_store();

    // Insert a key with known size
    let value = "test_value_with_known_size";
    insert_key(&store, "sized_key", value, 0, 0);

    // List keys
    let result = handle_list_keys(store, 1, 10);

    match result {
        ListKeysResult::Success(response) => {
            assert_eq!(response.keys.len(), 1);
            let key_info = &response.keys[0];
            assert_eq!(key_info.key, "sized_key");
            assert_eq!(key_info.size as usize, value.len());
            // TTL field exists
            let _ = key_info.ttl;
        }
        ListKeysResult::Error(e) => panic!("Expected Success, got error: {}", e.message),
    }
}
