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

use crate::request::{GetterType, Request, SetterType};
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

/// Success response for delete operation
#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub success: bool,
    pub message: String,
}

/// Success response for set key operation
#[derive(Debug, Serialize)]
pub struct SetKeyResponse {
    pub success: bool,
    pub message: String,
    pub key: String,
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

/// Result of handle_delete_key operation
pub enum DeleteKeyResult {
    /// Key successfully deleted
    Deleted(DeleteResponse),
    /// Key not found
    NotFound(ErrorResponse),
    /// Internal error occurred
    Error(ErrorResponse),
}

/// Result of handle_set_key operation
pub enum SetKeyResult {
    /// Key stored successfully
    Stored(SetKeyResponse),
    /// Bad request (validation error)
    BadRequest(ErrorResponse),
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

/// Handle POST /api/keys request
/// Stores a new key-value pair in the store
///
/// # Arguments
/// * `store` - Arc reference to the Store
/// * `request` - SetKeyRequest containing key, value, flags, and TTL
///
/// # Returns
/// * `SetKeyResult::Stored` - Key stored successfully
/// * `SetKeyResult::BadRequest` - Invalid request data
/// * `SetKeyResult::Error` - Internal error occurred
pub fn handle_set_key(store: Arc<Store>, request: SetKeyRequest) -> SetKeyResult {
    let start = Instant::now();

    // Validate key name
    if request.key.is_empty() {
        return SetKeyResult::BadRequest(ErrorResponse {
            error: "bad_request".to_string(),
            message: "Key name cannot be empty".to_string(),
        });
    }

    // Create key and value slices
    let key = Slice::from(request.key.as_str());
    let value = Slice::from(request.value.as_str());
    let bytes = value.len();

    // Create a setter request
    let set_request = Request::Setter {
        setter: SetterType::Set,
        key: key.clone(),
        flags: request.flags,
        ttl: request.ttl,
        bytes,
        payload: value,
        no_reply: false,
    };

    // Apply the request to the store
    match store.apply(set_request) {
        Ok(response) => {
            match response {
                crate::response::Response::Stored => {
                    let elapsed = start.elapsed();
                    log::debug!(
                        "SET key '{}' completed in {:?}",
                        request.key,
                        elapsed
                    );

                    SetKeyResult::Stored(SetKeyResponse {
                        success: true,
                        message: "Key stored successfully".to_string(),
                        key: request.key,
                    })
                }
                crate::response::Response::ClientError(msg) => {
                    SetKeyResult::BadRequest(ErrorResponse {
                        error: "bad_request".to_string(),
                        message: msg,
                    })
                }
                _ => SetKeyResult::Error(ErrorResponse {
                    error: "internal_error".to_string(),
                    message: "Unexpected response type".to_string(),
                }),
            }
        }
        Err(e) => SetKeyResult::Error(ErrorResponse {
            error: "internal_error".to_string(),
            message: format!("Store error: {:?}", e),
        }),
    }
}

/// Handle DELETE /api/keys/{key} request
/// Deletes a key from the store
///
/// # Arguments
/// * `store` - Arc reference to the Store
/// * `key` - The key name to delete
///
/// # Returns
/// * `DeleteKeyResult::Deleted` - Key was successfully deleted
/// * `DeleteKeyResult::NotFound` - Key does not exist
/// * `DeleteKeyResult::Error` - Internal error occurred
pub fn handle_delete_key(store: Arc<Store>, key: String) -> DeleteKeyResult {
    let start = Instant::now();

    // Create a Slice from the key string
    let key_slice = Slice::from(key.as_str());

    // Create a delete request
    let request = Request::Deleter {
        key: key_slice,
        no_reply: false,
    };

    // Apply the request to the store
    match store.apply(request) {
        Ok(response) => {
            match response {
                crate::response::Response::Deleted => {
                    let elapsed = start.elapsed();
                    log::debug!(
                        "DELETE key '{}' completed in {:?}",
                        key,
                        elapsed
                    );
                    DeleteKeyResult::Deleted(DeleteResponse {
                        success: true,
                        message: format!("Key '{}' deleted successfully", key),
                    })
                }
                crate::response::Response::NotFound => {
                    DeleteKeyResult::NotFound(ErrorResponse {
                        error: "not_found".to_string(),
                        message: format!("Key '{}' not found", key),
                    })
                }
                _ => DeleteKeyResult::Error(ErrorResponse {
                    error: "internal_error".to_string(),
                    message: "Unexpected response type".to_string(),
                }),
            }
        }
        Err(e) => DeleteKeyResult::Error(ErrorResponse {
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

    // Tests for handle_set_key (Scenario 5 - Key Operations - Set Key)

    #[test]
    fn test_set_key_success() {
        let store = create_test_store();

        let request = SetKeyRequest {
            key: "test".to_string(),
            value: "hello".to_string(),
            flags: 0,
            ttl: 3600,
        };

        let result = handle_set_key(store, request);

        match result {
            SetKeyResult::Stored(response) => {
                assert!(response.success);
                assert_eq!(response.key, "test");
                assert_eq!(response.message, "Key stored successfully");
            }
            _ => panic!("Expected Stored result"),
        }
    }

    #[test]
    fn test_set_key_empty_key_name() {
        let store = create_test_store();

        let request = SetKeyRequest {
            key: "".to_string(),
            value: "hello".to_string(),
            flags: 0,
            ttl: 3600,
        };

        let result = handle_set_key(store, request);

        match result {
            SetKeyResult::BadRequest(error) => {
                assert_eq!(error.error, "bad_request");
                assert!(error.message.contains("cannot be empty"));
            }
            _ => panic!("Expected BadRequest result"),
        }
    }

    #[test]
    fn test_set_key_with_flags_and_ttl() {
        let store = create_test_store();

        let request = SetKeyRequest {
            key: "mykey".to_string(),
            value: "myvalue".to_string(),
            flags: 42,
            ttl: 7200,
        };

        let result = handle_set_key(store.clone(), request);

        match result {
            SetKeyResult::Stored(response) => {
                assert!(response.success);
                assert_eq!(response.key, "mykey");
            }
            _ => panic!("Expected Stored result"),
        }

        // Verify the key was stored correctly
        let get_result = handle_get_key(store, "mykey".to_string());
        match get_result {
            GetKeyResult::Found(response) => {
                assert_eq!(response.key, "mykey");
                assert_eq!(response.value, "myvalue");
                assert_eq!(response.flags, 42);
            }
            _ => panic!("Expected Found result"),
        }
    }

    #[test]
    fn test_set_key_performance() {
        let store = create_test_store();

        let request = SetKeyRequest {
            key: "perf_set_test".to_string(),
            value: "performance_test_value".to_string(),
            flags: 0,
            ttl: 3600,
        };

        // Measure set performance
        let start = Instant::now();
        let result = handle_set_key(store, request);
        let elapsed = start.elapsed();

        // Should complete within 500ms
        assert!(
            elapsed.as_millis() < 500,
            "Set operation took {}ms, expected < 500ms",
            elapsed.as_millis()
        );

        match result {
            SetKeyResult::Stored(_) => {}
            _ => panic!("Expected Stored result"),
        }
    }

    #[test]
    fn test_set_key_overwrite_existing() {
        let store = create_test_store();

        // Set initial key
        let request1 = SetKeyRequest {
            key: "overwrite_test".to_string(),
            value: "initial_value".to_string(),
            flags: 1,
            ttl: 3600,
        };
        handle_set_key(store.clone(), request1);

        // Overwrite with new value
        let request2 = SetKeyRequest {
            key: "overwrite_test".to_string(),
            value: "new_value".to_string(),
            flags: 2,
            ttl: 7200,
        };
        let result = handle_set_key(store.clone(), request2);

        match result {
            SetKeyResult::Stored(response) => {
                assert!(response.success);
            }
            _ => panic!("Expected Stored result"),
        }

        // Verify new value
        let get_result = handle_get_key(store, "overwrite_test".to_string());
        match get_result {
            GetKeyResult::Found(response) => {
                assert_eq!(response.value, "new_value");
                assert_eq!(response.flags, 2);
            }
            _ => panic!("Expected Found result"),
        }
    }

    #[test]
    fn test_set_key_empty_value_allowed() {
        let store = create_test_store();

        let request = SetKeyRequest {
            key: "empty_value_key".to_string(),
            value: "".to_string(),
            flags: 0,
            ttl: 3600,
        };

        let result = handle_set_key(store.clone(), request);

        match result {
            SetKeyResult::Stored(response) => {
                assert!(response.success);
            }
            _ => panic!("Expected Stored result"),
        }

        // Verify the key was stored with empty value
        let get_result = handle_get_key(store, "empty_value_key".to_string());
        match get_result {
            GetKeyResult::Found(response) => {
                assert_eq!(response.value, "");
                assert_eq!(response.size, 0);
            }
            _ => panic!("Expected Found result"),
        }
    }

    // Tests for handle_delete_key (Scenario 7 - Key Operations - Delete Key)

    #[test]
    fn test_delete_existing_key() {
        let store = create_test_store();

        // First, set a key
        let key = Slice::from("delete_test_key");
        let value = Slice::from("delete_test_value");
        let set_request = Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 0,
            ttl: 0,
            bytes: value.len(),
            payload: value,
            no_reply: false,
        };
        store.apply(set_request).unwrap();

        // Delete the key
        let result = handle_delete_key(Arc::clone(&store), "delete_test_key".to_string());

        match result {
            DeleteKeyResult::Deleted(response) => {
                assert!(response.success);
                assert!(response.message.contains("delete_test_key"));
            }
            _ => panic!("Expected Deleted result"),
        }

        // Verify the key is no longer accessible
        let get_result = handle_get_key(store, "delete_test_key".to_string());
        match get_result {
            GetKeyResult::NotFound(_) => {}
            _ => panic!("Expected key to be not found after deletion"),
        }
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let store = create_test_store();

        let result = handle_delete_key(store, "nonexistent_key".to_string());

        match result {
            DeleteKeyResult::NotFound(error) => {
                assert_eq!(error.error, "not_found");
                assert!(error.message.contains("nonexistent_key"));
            }
            _ => panic!("Expected NotFound result"),
        }
    }

    #[test]
    fn test_delete_key_performance() {
        let store = create_test_store();

        // Set a key
        let key = Slice::from("delete_perf_test");
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

        // Measure delete performance
        let start = Instant::now();
        let _result = handle_delete_key(store, "delete_perf_test".to_string());
        let elapsed = start.elapsed();

        // Should complete within 500ms
        assert!(
            elapsed.as_millis() < 500,
            "Delete operation took {}ms, expected < 500ms",
            elapsed.as_millis()
        );
    }
}
