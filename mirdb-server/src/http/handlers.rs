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

/// Scenario 1: Server Status Display
///
/// Handles GET /api/status requests to retrieve server status.
///
/// Response: StatusResponse { levels, memory, compaction }
///           ErrorResponse { error: "..." } on failure
pub fn status_handler(store: &Arc<Store>) -> Result<String, String> {
    use super::types::{LevelInfo, MemoryInfo, StatusResponse};
    use crate::request::Request;

    // Get the INFO response from the store
    let info_response = match store.apply(Request::Info) {
        Ok(crate::response::Response::Info(info)) => info,
        Ok(_) => return Err(r#"{"error":"Unexpected response type"}"#.to_string()),
        Err(e) => {
            let error = ErrorResponse::new(format!("Store error: {}", e.msg));
            return Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Store error"}"#.to_string()
            ));
        }
    };

    // Parse the info string to extract level counts
    // Format: "Level0 (count):\n\tfiles..."
    let levels = parse_level_info(&info_response);

    // Memory info - estimate based on available data
    // For now, return placeholder values as the current codebase
    // doesn't expose detailed memory tracking
    let memory = MemoryInfo {
        used_bytes: 0,
        percentage: 0.0,
    };

    // Compaction status - "idle" when no active compaction
    // The current implementation doesn't expose compaction state directly,
    // so we default to "idle"
    let compaction = "idle".to_string();

    let status = StatusResponse {
        levels,
        memory,
        compaction,
    };

    serde_json::to_string(&status).map_err(|e| {
        let error = ErrorResponse::new(format!("Serialization error: {}", e));
        serde_json::to_string(&error).unwrap_or_else(|_|
            r#"{"error":"Serialization error"}"#.to_string()
        )
    })
}

/// Parse the INFO string output to extract level file counts
fn parse_level_info(info: &str) -> Vec<super::types::LevelInfo> {
    use super::types::LevelInfo;
    use std::collections::HashMap;

    let mut levels: HashMap<usize, usize> = HashMap::new();

    // Parse lines like "Level0 (5):" to extract level and file count
    for line in info.lines() {
        let line = line.trim();
        if line.starts_with("Level") && line.contains('(') && line.contains(')') {
            // Extract level number and file count
            if let Some(level_end) = line.find('(') {
                if let Some(count_end) = line.find(')') {
                    let level_str = &line[5..level_end].trim();
                    let count_str = &line[level_end + 1..count_end];

                    if let (Ok(level), Ok(count)) = (level_str.parse::<usize>(), count_str.parse::<usize>()) {
                        levels.insert(level, count);
                    }
                }
            }
        }
    }

    // Convert to sorted vector of LevelInfo
    let mut result: Vec<LevelInfo> = levels
        .into_iter()
        .map(|(level, files)| LevelInfo { level, files })
        .collect();
    result.sort_by_key(|l| l.level);

    // Ensure we always have at least Level 0 and Level 1
    if result.is_empty() {
        result.push(LevelInfo { level: 0, files: 0 });
        result.push(LevelInfo { level: 1, files: 0 });
    } else {
        // Fill in missing levels up to the max level found
        let max_level = result.iter().map(|l| l.level).max().unwrap_or(0);
        for level in 0..=max_level {
            if !result.iter().any(|l| l.level == level) {
                result.push(LevelInfo { level, files: 0 });
            }
        }
        result.sort_by_key(|l| l.level);
    }

    result
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

/// Scenario 4: DELETE Key Operation
///
/// Handles DELETE /api/key/{key} requests to remove a key from the store.
///
/// Path parameter: key - The key to delete (URL-encoded)
/// Response: SuccessResponse { success: true } on success (idempotent - both existing and non-existing keys)
///           ErrorResponse { error: "..." } on failure (invalid key format)
///
/// Note: DELETE is idempotent per REST principles - deleting a non-existent key
/// returns success rather than an error.
pub fn delete_key_handler(store: &Arc<Store>, key: &str) -> Result<String, String> {
    // Validate key is not empty
    if key.is_empty() {
        let error = ErrorResponse::new("Invalid key format: key cannot be empty");
        return Err(serde_json::to_string(&error).unwrap_or_else(|_|
            r#"{"error":"Invalid key format: key cannot be empty"}"#.to_string()
        ));
    }

    // URL decode the key (simple percent-decoding)
    let decoded_key = percent_decode(key);

    // Create the memcached-style delete request
    let key_slice = Slice::from(decoded_key.as_bytes().to_vec());
    let store_request = Request::Deleter {
        key: key_slice,
        no_reply: false,
    };

    // Apply the request to the store
    match store.apply(store_request) {
        Ok(Response::Deleted) => {
            // Key existed and was deleted
            let success = SuccessResponse::new(true);
            Ok(serde_json::to_string(&success).unwrap_or_else(|_|
                r#"{"success":true}"#.to_string()
            ))
        }
        Ok(Response::NotFound) => {
            // Key didn't exist - return success for idempotent delete
            // This follows REST best practices where DELETE is idempotent
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

/// Scenario 5: Manual Compaction Trigger
///
/// Handles POST /api/operations/compact requests to trigger major compaction.
///
/// Response: CompactResponse { success: true, message: "..." } on success
///           ErrorResponse { error: "..." } on failure
pub fn compact_handler(store: &Arc<Store>) -> Result<String, String> {
    use super::types::CompactResponse;

    // Trigger major compaction via the store
    match store.apply(Request::MajorCompaction) {
        Ok(Response::Ok) => {
            let response = CompactResponse {
                success: true,
                message: "Compaction initiated".to_string(),
            };
            serde_json::to_string(&response).map_err(|e| {
                let error = ErrorResponse::new(format!("Serialization error: {}", e));
                serde_json::to_string(&error).unwrap_or_else(|_|
                    r#"{"error":"Serialization error"}"#.to_string()
                )
            })
        }
        Ok(Response::ServerError(msg)) => {
            let error = ErrorResponse::new(msg);
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Server error"}"#.to_string()
            ))
        }
        Ok(_) => {
            let error = ErrorResponse::new("Unexpected response from compaction");
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Unexpected response"}"#.to_string()
            ))
        }
        Err(e) => {
            let error = ErrorResponse::new(format!("Compaction error: {}", e.msg));
            Err(serde_json::to_string(&error).unwrap_or_else(|_|
                r#"{"error":"Compaction error"}"#.to_string()
            ))
        }
    }
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
    // Scenario 1: Server Status Display tests
    // =============================================================

    #[test]
    fn test_status_handler_returns_json() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let result = status_handler(&store);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);

        let json = result.unwrap();
        assert!(json.contains("\"levels\""), "Expected levels field in response");
        assert!(json.contains("\"memory\""), "Expected memory field in response");
        assert!(json.contains("\"compaction\""), "Expected compaction field in response");
    }

    #[test]
    fn test_status_handler_levels_array() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let result = status_handler(&store);
        assert!(result.is_ok());

        let json = result.unwrap();

        // Parse the response to verify structure
        let status: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(status["levels"].is_array(), "levels should be an array");

        // Should have at least level 0 and level 1
        let levels = status["levels"].as_array().unwrap();
        assert!(levels.len() >= 2, "Should have at least 2 levels");
    }

    #[test]
    fn test_status_handler_memory_fields() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let result = status_handler(&store);
        assert!(result.is_ok());

        let json = result.unwrap();
        let status: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(status["memory"]["used_bytes"].is_number(), "memory.used_bytes should be a number");
        assert!(status["memory"]["percentage"].is_number(), "memory.percentage should be a number");
    }

    #[test]
    fn test_status_handler_compaction_field() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let result = status_handler(&store);
        assert!(result.is_ok());

        let json = result.unwrap();
        let status: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(status["compaction"].is_string(), "compaction should be a string");
    }

    #[test]
    fn test_parse_level_info_empty() {
        let info = "";
        let levels = parse_level_info(info);

        // Should have default levels 0 and 1
        assert!(levels.len() >= 2);
        assert_eq!(levels[0].level, 0);
        assert_eq!(levels[0].files, 0);
    }

    #[test]
    fn test_parse_level_info_with_data() {
        let info = "Next file number: 5\n\nLevel0 (3):\n\t0.sst, 1.sst, 2.sst\nLevel1 (2):\n\t3.sst, 4.sst";
        let levels = parse_level_info(info);

        assert!(levels.len() >= 2);
        assert_eq!(levels[0].level, 0);
        assert_eq!(levels[0].files, 3);
        assert_eq!(levels[1].level, 1);
        assert_eq!(levels[1].files, 2);
    }

    #[test]
    fn test_parse_level_info_multiple_levels() {
        let info = "Level0 (1):\nLevel1 (5):\nLevel2 (10):";
        let levels = parse_level_info(info);

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0].files, 1);
        assert_eq!(levels[1].files, 5);
        assert_eq!(levels[2].files, 10);
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

    // =============================================================
    // Scenario 4: DELETE Key Tests
    // =============================================================

    /// Test Case 1: DELETE /api/key/existing-key
    /// Expected: JSON response with {success: true}
    #[test]
    fn test_delete_key_existing() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // First, set a key
        let set_body = r#"{"key":"delete-me","value":"test_value","flags":0,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // Verify the key exists
        let get_result = get_key_handler(&store, "delete-me");
        assert!(get_result.is_ok(), "GET should succeed before delete: {:?}", get_result);

        // Delete the key
        let result = delete_key_handler(&store, "delete-me");
        assert!(result.is_ok(), "DELETE should succeed for existing key: {:?}", result);

        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true, got {}", response);

        // Parse the response to verify structure
        let parsed: SuccessResponse = serde_json::from_str(&response)
            .expect("Response should be valid SuccessResponse JSON");
        assert!(parsed.success, "Success field should be true");
    }

    /// Test Case 2: DELETE /api/key/nonexistent-key
    /// Expected: JSON response with {success: true} (idempotent delete)
    #[test]
    fn test_delete_key_nonexistent() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Delete a key that doesn't exist - should still return success (idempotent)
        let result = delete_key_handler(&store, "nonexistent-key");
        assert!(result.is_ok(), "DELETE should succeed for non-existent key (idempotent): {:?}", result);

        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true for idempotent delete, got {}", response);

        let parsed: SuccessResponse = serde_json::from_str(&response)
            .expect("Response should be valid SuccessResponse JSON");
        assert!(parsed.success, "Success field should be true for idempotent delete");
    }

    /// Test Case 3: GET /api/key/deleted-key after deletion
    /// Expected: 404 response indicating key not found
    #[test]
    fn test_get_key_after_deletion() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key
        let set_body = r#"{"key":"to-be-deleted","value":"temp_value","flags":1,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // Verify it exists
        let get_result = get_key_handler(&store, "to-be-deleted");
        assert!(get_result.is_ok(), "GET should succeed before delete");

        // Delete the key
        let delete_result = delete_key_handler(&store, "to-be-deleted");
        assert!(delete_result.is_ok(), "DELETE should succeed");

        // Try to GET the deleted key - should return error (key not found)
        let result = get_key_handler(&store, "to-be-deleted");
        assert!(result.is_err(), "GET should fail for deleted key");

        let error = result.unwrap_err();
        assert!(error.contains("Key not found"), "Error should indicate key not found: {}", error);
    }

    /// Test: DELETE key with empty key
    /// Expected: Error response indicating invalid key format
    #[test]
    fn test_delete_key_empty() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // DELETE with empty key
        let result = delete_key_handler(&store, "");

        assert!(result.is_err(), "DELETE should fail for empty key");
        let error = result.unwrap_err();
        assert!(error.contains("Invalid key format"), "Error should indicate invalid key format: {}", error);
    }

    /// Test: DELETE key with URL-encoded characters
    #[test]
    fn test_delete_key_url_encoded() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key with a space in it
        let set_body = r#"{"key":"key with space","value":"space_value","flags":0,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // Delete the key using URL encoding (%20 for space)
        let result = delete_key_handler(&store, "key%20with%20space");
        assert!(result.is_ok(), "DELETE should decode URL-encoded key: {:?}", result);

        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true");

        // Verify the key is deleted
        let get_result = get_key_handler(&store, "key%20with%20space");
        assert!(get_result.is_err(), "GET should fail after delete");
    }

    /// Test: DELETE key with special characters (colons)
    #[test]
    fn test_delete_key_special_characters() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key with colons
        let set_body = r#"{"key":"user:session:123","value":"session_data","flags":0,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

        // Delete the key
        let result = delete_key_handler(&store, "user:session:123");
        assert!(result.is_ok(), "DELETE should succeed for key with colons: {:?}", result);

        let response = result.unwrap();
        assert!(response.contains(r#""success":true"#), "Expected success:true");

        // Verify the key is deleted
        let get_result = get_key_handler(&store, "user:session:123");
        assert!(get_result.is_err(), "GET should fail after delete");
    }

    /// Test: DELETE is idempotent - multiple deletes should all succeed
    #[test]
    fn test_delete_key_idempotent() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        // Set a key
        let set_body = r#"{"key":"idempotent-key","value":"test","flags":0,"ttl":0}"#;
        let set_result = set_key_handler(&store, set_body);
        assert!(set_result.is_ok(), "SET should succeed");

        // First delete
        let result1 = delete_key_handler(&store, "idempotent-key");
        assert!(result1.is_ok(), "First DELETE should succeed");
        assert!(result1.unwrap().contains(r#""success":true"#));

        // Second delete (key no longer exists)
        let result2 = delete_key_handler(&store, "idempotent-key");
        assert!(result2.is_ok(), "Second DELETE should succeed (idempotent)");
        assert!(result2.unwrap().contains(r#""success":true"#));

        // Third delete
        let result3 = delete_key_handler(&store, "idempotent-key");
        assert!(result3.is_ok(), "Third DELETE should succeed (idempotent)");
        assert!(result3.unwrap().contains(r#""success":true"#));
    }
}
