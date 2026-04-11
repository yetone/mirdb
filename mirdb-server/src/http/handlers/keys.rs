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

/// Result of handle_list_keys operation
pub enum ListKeysResult {
    /// Successfully listed keys
    Success(KeysListResponse),
    /// Internal error occurred
    Error(ErrorResponse),
}

/// Handle GET /api/keys request with pagination
/// Lists all keys in the store with their sizes and TTL values
///
/// # Arguments
/// * `store` - Arc reference to the Store
/// * `page` - Page number (1-indexed)
/// * `limit` - Number of items per page
///
/// # Returns
/// * `ListKeysResult::Success` - List of keys with pagination metadata
/// * `ListKeysResult::Error` - Internal error occurred
pub fn handle_list_keys(store: Arc<Store>, page: u32, limit: u32) -> ListKeysResult {
    let start = Instant::now();

    // Validate pagination parameters
    let page = if page < 1 { 1 } else { page };
    let limit = if limit < 1 { 10 } else if limit > 100 { 100 } else { limit };

    // Calculate skip offset (0-indexed internally)
    let skip = ((page - 1) * limit) as usize;
    let limit_usize = limit as usize;

    // Get keys from store
    match store.list_keys(skip, limit_usize) {
        Ok((keys, total)) => {
            let key_infos: Vec<KeyInfo> = keys
                .into_iter()
                .map(|(key, payload)| {
                    let key_str = to_str(&key).to_string();
                    KeyInfo {
                        key: key_str,
                        size: payload.bytes as u64,
                        ttl: 0, // TTL not directly accessible from StorePayload public fields
                    }
                })
                .collect();

            let elapsed = start.elapsed();
            log::debug!(
                "LIST keys page={} limit={} returned {} keys (total={}) in {:?}",
                page,
                limit,
                key_infos.len(),
                total,
                elapsed
            );

            ListKeysResult::Success(KeysListResponse {
                keys: key_infos,
                page,
                limit,
                total: total as u32,
            })
        }
        Err(e) => {
            log::error!("Error listing keys: {:?}", e);
            ListKeysResult::Error(ErrorResponse {
                error: "internal_error".to_string(),
                message: format!("Failed to list keys: {:?}", e),
            })
        }
    }
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
    fn test_list_keys_empty_store() {
        let store = create_test_store();

        let result = handle_list_keys(store, 1, 10);

        match result {
            ListKeysResult::Success(response) => {
                assert_eq!(response.keys.len(), 0);
                assert_eq!(response.total, 0);
                assert_eq!(response.page, 1);
                assert_eq!(response.limit, 10);
            }
            ListKeysResult::Error(_) => panic!("Expected Success result"),
        }
    }

    #[test]
    fn test_list_keys_with_data() {
        let store = create_test_store();

        // Insert some keys
        for i in 0..5 {
            let key = Slice::from(format!("key{}", i));
            let value = Slice::from(format!("value{}", i));
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
        }

        let result = handle_list_keys(store, 1, 10);

        match result {
            ListKeysResult::Success(response) => {
                assert_eq!(response.keys.len(), 5);
                assert_eq!(response.total, 5);
                assert_eq!(response.page, 1);
                assert_eq!(response.limit, 10);
            }
            ListKeysResult::Error(e) => panic!("Expected Success result, got error: {:?}", e.message),
        }
    }

    #[test]
    fn test_list_keys_pagination() {
        let store = create_test_store();

        // Insert 6 keys to test pagination (simpler dataset)
        for i in 0..6 {
            let key = Slice::from(format!("key{:02}", i));
            let value = Slice::from(format!("value{}", i));
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
        }

        // Get first page with limit 3
        let result = handle_list_keys(Arc::clone(&store), 1, 3);
        match result {
            ListKeysResult::Success(response) => {
                assert_eq!(response.keys.len(), 3);
                assert_eq!(response.total, 6);
                assert_eq!(response.page, 1);
                assert_eq!(response.limit, 3);
            }
            ListKeysResult::Error(e) => panic!("Expected Success, got error: {:?}", e.message),
        }

        // Get second page
        let result = handle_list_keys(store, 2, 3);
        match result {
            ListKeysResult::Success(response) => {
                assert_eq!(response.keys.len(), 3);
                assert_eq!(response.total, 6);
                assert_eq!(response.page, 2);
            }
            ListKeysResult::Error(e) => panic!("Expected Success, got error: {:?}", e.message),
        }
    }

    #[test]
    fn test_list_keys_returns_key_info() {
        let store = create_test_store();

        // Insert a key with known size
        let key = Slice::from("test_key");
        let value = Slice::from("test_value_12345");
        let set_request = Request::Setter {
            setter: SetterType::Set,
            key,
            flags: 42,
            ttl: 3600,
            bytes: value.len(),
            payload: value,
            no_reply: false,
        };
        store.apply(set_request).unwrap();

        let result = handle_list_keys(store, 1, 10);

        match result {
            ListKeysResult::Success(response) => {
                assert_eq!(response.keys.len(), 1);
                let key_info = &response.keys[0];
                assert_eq!(key_info.key, "test_key");
                assert_eq!(key_info.size, 16); // "test_value_12345".len()
            }
            ListKeysResult::Error(e) => panic!("Expected Success, got error: {:?}", e.message),
        }
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
