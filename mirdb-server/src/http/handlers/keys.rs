//! Key Operations API Handler
//! Owner: Scenario 4 - Key Browser View Keys
//! Co-owners: Scenarios 5, 6, 7 (Set, Get, Delete)
//!
//! Expected exports:
//! - handle_list_keys(store: Arc<Store>, page: u32, limit: u32) -> Response
//! - handle_get_key(store: Arc<Store>, key: String) -> Response
//! - handle_set_key(store: Arc<Store>, body: SetKeyRequest) -> Response
//! - handle_delete_key(store: Arc<Store>, key: String) -> Response

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::request::{GetterType, Request};
use crate::slice::Slice;
use crate::store::Store;
use crate::utils::to_str;

/// Request to set a key
#[derive(Debug, Deserialize)]
pub struct SetKeyRequest {
    pub key: String,
    pub value: String,
    pub flags: u32,
    pub ttl: u32,
}

/// Response for key listing
#[derive(Debug, Serialize)]
pub struct KeysListResponse {
    pub keys: Vec<KeyInfo>,
    pub page: u32,
    pub limit: u32,
    pub total: u32,
}

/// Key information
#[derive(Debug, Serialize)]
pub struct KeyInfo {
    pub key: String,
    pub size: u64,
    pub ttl: u32,
}

/// Single key value response with full metadata
#[derive(Debug, Serialize)]
pub struct KeyValueResponse {
    pub key: String,
    pub value: String,
    pub flags: u32,
    pub ttl: u32,
    pub size: usize,
}

/// Error response for API errors
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Result of handle_get_key operation
pub enum GetKeyResult {
    /// Key found with value and metadata
    Found(KeyValueResponse),
    /// Key not found
    NotFound(ErrorResponse),
    /// Internal error occurred
    Error(ErrorResponse),
}

/// Handle GET /api/keys/{key} request
/// Retrieves a single key's value and metadata from the store
///
/// # Arguments
/// * `store` - Arc reference to the Store
/// * `key` - The key name to retrieve
///
/// # Returns
/// * `GetKeyResult::Found` - Key exists, returns value and metadata
/// * `GetKeyResult::NotFound` - Key does not exist
/// * `GetKeyResult::Error` - Internal error occurred
pub fn handle_get_key(store: Arc<Store>, key: String) -> GetKeyResult {
    let start = Instant::now();

    // Create a Slice from the key string
    let key_slice = Slice::from(key.as_str());

    // Create a getter request
    let request = Request::Getter {
        getter: GetterType::Get,
        keys: vec![key_slice],
    };

    // Apply the request to the store
    match store.apply(request) {
        Ok(response) => {
            match response {
                crate::response::Response::Get(items) => {
                    if items.is_empty() {
                        // Key not found
                        GetKeyResult::NotFound(ErrorResponse {
                            error: "not_found".to_string(),
                            message: format!("Key '{}' not found", key),
                        })
                    } else {
                        // Key found - extract first item
                        let item = &items[0];
                        let value = String::from_utf8_lossy(item.data.as_ref()).to_string();

                        let elapsed = start.elapsed();
                        log::debug!(
                            "GET key '{}' completed in {:?}",
                            key,
                            elapsed
                        );

                        GetKeyResult::Found(KeyValueResponse {
                            key,
                            value,
                            flags: item.flags,
                            ttl: 0, // TTL from response (memcached protocol doesn't return TTL)
                            size: item.bytes,
                        })
                    }
                }
                _ => GetKeyResult::Error(ErrorResponse {
                    error: "internal_error".to_string(),
                    message: "Unexpected response type".to_string(),
                }),
            }
        }
        Err(e) => GetKeyResult::Error(ErrorResponse {
            error: "internal_error".to_string(),
            message: format!("Store error: {:?}", e),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::SetterType;
    use crate::test_utils::get_test_opt;

    fn create_test_store() -> Arc<Store> {
        let opts = get_test_opt();
        Arc::new(Store::new(opts).unwrap())
    }

    #[test]
    fn test_get_existing_key() {
        let store = create_test_store();

        // First, set a key
        let key = Slice::from("test_key");
        let value = Slice::from("test_value");
        let set_request = Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 42,
            ttl: 3600,
            bytes: value.len(),
            payload: value,
            no_reply: false,
        };
        store.apply(set_request).unwrap();

        // Now get the key
        let result = handle_get_key(store, "test_key".to_string());

        match result {
            GetKeyResult::Found(response) => {
                assert_eq!(response.key, "test_key");
                assert_eq!(response.value, "test_value");
                assert_eq!(response.flags, 42);
                assert_eq!(response.size, 10);
            }
            _ => panic!("Expected Found result"),
        }
    }

    #[test]
    fn test_get_nonexistent_key() {
        let store = create_test_store();

        let result = handle_get_key(store, "nonexistent".to_string());

        match result {
            GetKeyResult::NotFound(error) => {
                assert_eq!(error.error, "not_found");
                assert!(error.message.contains("nonexistent"));
            }
            _ => panic!("Expected NotFound result"),
        }
    }

    #[test]
    fn test_get_key_performance() {
        let store = create_test_store();

        // Set a key
        let key = Slice::from("perf_test");
        let value = Slice::from("performance_test_value");
        let set_request = Request::Setter {
            setter: SetterType::Set,
            key,
            flags: 0,
            ttl: 0,
            bytes: value.len(),
            payload: value,
            no_reply: false,
        };
        store.apply(set_request).unwrap();

        // Measure get performance
        let start = Instant::now();
        let _result = handle_get_key(store, "perf_test".to_string());
        let elapsed = start.elapsed();

        // Should complete within 500ms
        assert!(
            elapsed.as_millis() < 500,
            "Get operation took {}ms, expected < 500ms",
            elapsed.as_millis()
        );
    }
}
