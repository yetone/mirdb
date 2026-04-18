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

use super::types::{ErrorResponse, KeyResponse, SetKeyRequest, SuccessResponse};

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

/// Scenario 2: GET Key Operation
///
/// Handles GET /api/key/{key} requests to retrieve a key's value and metadata.
///
/// Path parameter: key - The key to look up
/// Response: KeyResponse { value, flags, ttl, bytes } on success
///           ErrorResponse { error: "..." } on failure (key not found, invalid key)
pub fn get_key_handler(store: &Arc<Store>, key: &str) -> Result<String, String> {
    // Validate key is not empty
    if key.is_empty() {
        let error = ErrorResponse::new("Invalid key format: key cannot be empty");
        return Err(serde_json::to_string(&error).unwrap_or_else(|_|
            r#"{"error":"Invalid key format: key cannot be empty"}"#.to_string()
        ));
    }

    // URL decode the key (simple percent-decoding)
    let decoded_key = percent_decode(key);

    // Create the memcached-style getter request
    let key_slice = Slice::from(decoded_key.as_bytes().to_vec());
    let store_request = Request::Getter {
        getter: crate::request::GetterType::Get,
        keys: vec![key_slice],
    };

    // Apply the request to the store
    match store.apply(store_request) {
        Ok(Response::Get(items)) => {
            if items.is_empty() {
                let error = ErrorResponse::new("Key not found");
                return Err(serde_json::to_string(&error).unwrap_or_else(|_|
                    r#"{"error":"Key not found"}"#.to_string()
                ));
            }

            // Get the first item (we only queried one key)
            let item = &items[0];

            // Convert Slice to String - binary data will be lossy converted
            let value = String::from_utf8_lossy(item.data.as_ref()).to_string();

            let response = KeyResponse {
                value,
                flags: item.flags,
                ttl: 0, // Note: memcached GET doesn't return TTL in response
                bytes: item.bytes,
            };

            Ok(serde_json::to_string(&response).unwrap_or_else(|_|
                r#"{"error":"Failed to serialize response"}"#.to_string()
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
            let error = ErrorResponse::new("Key not found");
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Key not found"}"#.to_string()
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

/// Simple percent-decoding for URL-encoded strings
fn percent_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            // Try to decode a percent-encoded sequence
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            // If decoding fails, keep the original characters
            result.push('%');
            result.push_str(&hex);
        } else if c == '+' {
            // Plus sign represents space in URL encoding
            result.push(' ');
        } else {
            result.push(c);
        }
    }

    result
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

    // =============================================================
    // Scenario 3: SET Key Tests
    // =============================================================

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

    // =============================================================
    // Scenario 2: GET Key Tests
    // =============================================================

    /// Test Case 1: GET /api/key/user:123 (existing key)
    /// Expected: JSON response with 'value', 'flags', 'ttl', and 'bytes' fields
    #[test]
    fn test_get_key_existing() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // First, set a key via SET handler
        let set_body = r#"{"key":"user:123","value":"test_value","flags":42,"ttl":3600}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // Now GET the key
        let result = get_key_handler(&store, "user:123");
        assert!(result.is_ok(), "GET should succeed for existing key: {:?}", result);

        let response = result.unwrap();

        // Parse the response to verify structure
        let parsed: KeyResponse = serde_json::from_str(&response)
            .expect("Response should be valid KeyResponse JSON");

        assert_eq!(parsed.value, "test_value", "Value should match what was set");
        assert_eq!(parsed.flags, 42, "Flags should match what was set");
        assert_eq!(parsed.bytes, 10, "Bytes should be length of 'test_value'");

        // Verify all required fields are present
        assert!(response.contains("\"value\""), "Response must contain 'value' field");
        assert!(response.contains("\"flags\""), "Response must contain 'flags' field");
        assert!(response.contains("\"ttl\""), "Response must contain 'ttl' field");
        assert!(response.contains("\"bytes\""), "Response must contain 'bytes' field");
    }

    /// Test Case 2: GET /api/key/nonexistent-key
    /// Expected: JSON response with error indicating key not found (404 status)
    #[test]
    fn test_get_key_nonexistent() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // GET a key that doesn't exist
        let result = get_key_handler(&store, "nonexistent-key");

        assert!(result.is_err(), "GET should fail for non-existent key");
        let error = result.unwrap_err();
        assert!(error.contains("Key not found"), "Error should indicate key not found: {}", error);
    }

    /// Test Case 5: GET /api/key/ (empty key)
    /// Expected: Error response indicating invalid key format
    #[test]
    fn test_get_key_empty() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // GET with empty key
        let result = get_key_handler(&store, "");

        assert!(result.is_err(), "GET should fail for empty key");
        let error = result.unwrap_err();
        assert!(error.contains("Invalid key format"), "Error should indicate invalid key format: {}", error);
    }

    /// Test: GET key with special characters (URL encoded)
    #[test]
    fn test_get_key_special_characters() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key with special characters
        let set_body = r#"{"key":"key:with:colons","value":"special_value","flags":1,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // GET the key
        let result = get_key_handler(&store, "key:with:colons");
        assert!(result.is_ok(), "GET should succeed for key with colons: {:?}", result);

        let parsed: KeyResponse = serde_json::from_str(&result.unwrap())
            .expect("Response should be valid JSON");
        assert_eq!(parsed.value, "special_value");
    }

    /// Test: GET key with URL-encoded characters
    #[test]
    fn test_get_key_url_encoded() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key with a space in it
        let set_body = r#"{"key":"key with space","value":"space_value","flags":0,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // GET the key using URL encoding (%20 for space)
        let result = get_key_handler(&store, "key%20with%20space");
        assert!(result.is_ok(), "GET should decode URL-encoded key: {:?}", result);

        let parsed: KeyResponse = serde_json::from_str(&result.unwrap())
            .expect("Response should be valid JSON");
        assert_eq!(parsed.value, "space_value");
    }

    /// Test: GET key returns correct byte count
    #[test]
    fn test_get_key_bytes_count() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key with a known value length
        let value = "hello world"; // 11 bytes
        let set_body = format!(r#"{{"key":"bytes_test","value":"{}","flags":0,"ttl":0}}"#, value);
        let set_result = set_key_handler(&store, &set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // GET the key
        let result = get_key_handler(&store, "bytes_test");
        assert!(result.is_ok(), "GET should succeed: {:?}", result);

        let parsed: KeyResponse = serde_json::from_str(&result.unwrap())
            .expect("Response should be valid JSON");
        assert_eq!(parsed.bytes, 11, "Bytes should be 11 for 'hello world'");
    }

    /// Test: GET key with empty value (value can be empty)
    #[test]
    fn test_get_key_empty_value() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key with empty value
        let set_body = r#"{"key":"empty_value_key","value":"","flags":5,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed for empty value: {:?}", set_result);

        // GET the key
        let result = get_key_handler(&store, "empty_value_key");
        assert!(result.is_ok(), "GET should succeed for key with empty value: {:?}", result);

        let parsed: KeyResponse = serde_json::from_str(&result.unwrap())
            .expect("Response should be valid JSON");
        assert_eq!(parsed.value, "", "Value should be empty string");
        assert_eq!(parsed.flags, 5, "Flags should be 5");
        assert_eq!(parsed.bytes, 0, "Bytes should be 0 for empty value");
    }

    /// Test: Percent decode helper function
    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("key%3Avalue"), "key:value");
        assert_eq!(percent_decode("hello+world"), "hello world");
        assert_eq!(percent_decode("normal"), "normal");
        assert_eq!(percent_decode("%2F"), "/");
    }
}
