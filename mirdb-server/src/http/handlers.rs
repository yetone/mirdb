//! API request handlers
//!
//! Each handler is owned by a specific scenario:
//! - `status_handler` - Scenario 1: Server Status Display
//! - `get_key_handler` - Scenario 2: GET Key Operation
//! - `set_key_handler` - Scenario 3: SET Key Operation
//! - `delete_key_handler` - Scenario 4: DELETE Key Operation
//! - `compact_handler` - Scenario 5: Manual Compaction
//! - Error handling utilities - Scenario 6: Error Handling
//!
//! Each scenario should implement ONLY its assigned handler(s).

use std::sync::Arc;

use serde_json;

use crate::request::{Request, SetterType};
use crate::response::Response;
use crate::slice::Slice;
use crate::store::Store;

use super::types::{ErrorResponse, SetKeyRequest, SuccessResponse};

/// Scenario 3: SET Key Operation
///
/// Handles POST /api/key requests to store a key-value pair.
///
/// Request body: SetKeyRequest { key, value, flags?, ttl? }
/// Response: SuccessResponse { success: true } on success
///           ErrorResponse { error: "..." } on failure
pub fn set_key_handler(store: &Arc<Store>, body: &str) -> Result<String, String> {
    // Parse the JSON request body
    let request: SetKeyRequest = match serde_json::from_str(body) {
        Ok(req) => req,
        Err(e) => {
            let error = ErrorResponse::new(format!("Invalid JSON: {}", e));
            return Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Invalid JSON"}"#.to_string()
            ));
        }
    };

    // Validate required fields
    if request.key.is_empty() {
        let error = ErrorResponse::new("Key is required");
        return Err(serde_json::to_string(&error).unwrap_or_else(|_|
            r#"{"error":"Key is required"}"#.to_string()
        ));
    }

    // Create the memcached-style request
    let key = Slice::from(request.key.as_bytes().to_vec());
    let payload = Slice::from(request.value.as_bytes().to_vec());
    let bytes = payload.len();

    let store_request = Request::Setter {
        setter: SetterType::Set,
        key,
        flags: request.flags,
        ttl: request.ttl,
        bytes,
        payload,
        no_reply: false,
    };

    // Apply the request to the store
    match store.apply(store_request) {
        Ok(Response::Stored) => {
            let success = SuccessResponse::new(true);
            Ok(serde_json::to_string(&success).unwrap_or_else(|_|
                r#"{"success":true}"#.to_string()
            ))
        }
        Ok(Response::ClientError(msg)) => {
            let error = ErrorResponse::new(msg);
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Client error"}"#.to_string()
            ))
        }
        Ok(Response::ServerError(msg)) => {
            let error = ErrorResponse::new(msg);
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Server error"}"#.to_string()
            ))
        }
        Ok(_) => {
            let error = ErrorResponse::new("Unexpected response");
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Unexpected response"}"#.to_string()
            ))
        }
        Err(e) => {
            let error = ErrorResponse::new(format!("Store error: {}", e.msg));
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Store error"}"#.to_string()
            ))
        }
    }
}

// Placeholder for Scenario 1: Server Status Display
pub fn status_handler(_store: &Arc<Store>) -> Result<String, String> {
    Err(r#"{"error":"Not implemented"}"#.to_string())
}

// Placeholder for Scenario 2: GET Key Operation
pub fn get_key_handler(_store: &Arc<Store>, _key: &str) -> Result<String, String> {
    Err(r#"{"error":"Not implemented"}"#.to_string())
}

// Placeholder for Scenario 4: DELETE Key Operation
pub fn delete_key_handler(_store: &Arc<Store>, _key: &str) -> Result<String, String> {
    Err(r#"{"error":"Not implemented"}"#.to_string())
}

// Placeholder for Scenario 5: Manual Compaction
pub fn compact_handler(_store: &Arc<Store>) -> Result<String, String> {
    Err(r#"{"error":"Not implemented"}"#.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::get_test_opt;

    #[test]
    fn test_set_key_with_all_fields() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let body = r#"{"key":"test:1","value":"hello","flags":0,"ttl":3600}"#;
        let result = set_key_handler(&store, body);

        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true, got {}", response);
    }

    #[test]
    fn test_set_key_minimal_data() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let body = r#"{"key":"test:2","value":"world"}"#;
        let result = set_key_handler(&store, body);

        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true, got {}", response);
    }

    #[test]
    fn test_set_key_empty_key() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let body = r#"{"key":"","value":"hello"}"#;
        let result = set_key_handler(&store, body);

        assert!(result.is_err(), "Expected Err for empty key");
        let error = result.unwrap_err();
        assert!(error.contains("Key is required"), "Expected key required error, got {}", error);
    }

    #[test]
    fn test_set_key_empty_value() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Empty value is valid - should succeed
        let body = r#"{"key":"test:empty","value":""}"#;
        let result = set_key_handler(&store, body);

        assert!(result.is_ok(), "Expected Ok for empty value, got {:?}", result);
        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true, got {}", response);
    }

    #[test]
    fn test_set_key_overwrite() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set initial value
        let body1 = r#"{"key":"test:overwrite","value":"initial"}"#;
        let result1 = set_key_handler(&store, body1);
        assert!(result1.is_ok(), "Initial set failed");

        // Overwrite with new value
        let body2 = r#"{"key":"test:overwrite","value":"updated"}"#;
        let result2 = set_key_handler(&store, body2);
        assert!(result2.is_ok(), "Overwrite failed");

        let response = result2.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true on overwrite");
    }

    #[test]
    fn test_set_key_invalid_json() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let body = r#"{"key":"test", invalid json}"#;
        let result = set_key_handler(&store, body);

        assert!(result.is_err(), "Expected Err for invalid JSON");
        let error = result.unwrap_err();
        assert!(error.contains("Invalid JSON"), "Expected invalid JSON error, got {}", error);
    }
}
