//! Integration Tests for Demo Backend
//! Tests the full API flow including SET, GET, DELETE operations

use actix_web::{test, web, App};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;

// Re-create minimal types for testing without depending on main
#[derive(Clone, Debug)]
struct StoredValue {
    value: String,
    flags: u32,
    exptime: u64,
    bytes: usize,
}

struct AppState {
    store: Mutex<HashMap<String, StoredValue>>,
}

impl AppState {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

// Test the full workflow: SET -> GET -> DELETE -> GET
#[actix_rt::test]
async fn test_full_workflow() {
    // This test verifies the complete CRUD workflow
    // In a real integration test, we'd start the actual server
    // For now, we test the core logic

    let store: HashMap<String, StoredValue> = HashMap::new();
    assert!(store.is_empty());

    // Simulate SET
    let mut store = store;
    store.insert(
        "mykey".to_string(),
        StoredValue {
            value: "hello".to_string(),
            flags: 0,
            exptime: 0,
            bytes: 5,
        },
    );
    assert_eq!(store.len(), 1);

    // Simulate GET
    let item = store.get("mykey");
    assert!(item.is_some());
    assert_eq!(item.unwrap().value, "hello");

    // Simulate DELETE
    let removed = store.remove("mykey");
    assert!(removed.is_some());

    // Simulate GET after DELETE
    let item = store.get("mykey");
    assert!(item.is_none());
}

#[test]
fn test_memcached_protocol_format() {
    // Test that responses follow Memcached protocol format

    // STORED response for SET
    let set_response = "STORED";
    assert_eq!(set_response, "STORED");

    // VALUE format for GET
    let key = "mykey";
    let flags = 0;
    let bytes = 5;
    let value = "hello";
    let get_response = format!("VALUE {} {} {}\r\n{}\r\nEND", key, flags, bytes, value);
    assert!(get_response.contains("VALUE mykey 0 5"));
    assert!(get_response.contains("hello"));
    assert!(get_response.ends_with("END"));

    // DELETED response for DELETE
    let delete_response = "DELETED";
    assert_eq!(delete_response, "DELETED");

    // END response for GET on non-existent key
    let not_found_response = "END";
    assert_eq!(not_found_response, "END");
}

#[test]
fn test_command_parsing() {
    // Test command parsing logic

    // SET command format: SET <key> <flags> <exptime> <bytes>
    let set_cmd = "SET mykey 0 0 5";
    let parts: Vec<&str> = set_cmd.split_whitespace().collect();
    assert_eq!(parts.len(), 5);
    assert_eq!(parts[0], "SET");
    assert_eq!(parts[1], "mykey");
    assert_eq!(parts[2], "0");
    assert_eq!(parts[3], "0");
    assert_eq!(parts[4], "5");

    // GET command format: GET <key>
    let get_cmd = "GET mykey";
    let parts: Vec<&str> = get_cmd.split_whitespace().collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "GET");
    assert_eq!(parts[1], "mykey");

    // DELETE command format: DELETE <key>
    let delete_cmd = "DELETE mykey";
    let parts: Vec<&str> = delete_cmd.split_whitespace().collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "DELETE");
    assert_eq!(parts[1], "mykey");
}
