//! JSON request and response types
//!
//! Owner: Scenario 10 (JSON API Response Format)
//!
//! Expected types:
//! - `StatusResponse` - Server status info (levels, memory, compaction)
//! - `KeyResponse` - Key value with metadata (value, flags, ttl, bytes)
//! - `SetKeyRequest` - Key set request body (key, value, flags?, ttl?)
//! - `SuccessResponse` - Generic success response {success: bool}
//! - `ErrorResponse` - Error response with message
//!
//! All types should derive Serialize/Deserialize

use serde::{Deserialize, Serialize};

/// Request to set a key-value pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetKeyRequest {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub flags: u32,
    #[serde(default)]
    pub ttl: u32,
}

/// Response for a key lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyResponse {
    pub value: String,
    pub flags: u32,
    pub ttl: u32,
    pub bytes: usize,
}

/// Generic success response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessResponse {
    pub success: bool,
}

/// Error response with message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Server status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub levels: Vec<LevelInfo>,
    pub memory: MemoryInfo,
    pub compaction: String,
}

/// Level information for status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelInfo {
    pub level: usize,
    pub files: usize,
}

/// Memory information for status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    pub used_bytes: usize,
    pub percentage: f64,
}

impl SuccessResponse {
    pub fn new(success: bool) -> Self {
        Self { success }
    }
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self { error: error.into() }
    }
}

/// Compaction response (Scenario 5)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactResponse {
    pub success: bool,
    pub message: String,
}

// =============================================================
// Scenario 10: JSON API Response Format - Tests
// =============================================================
// These tests verify that all HTTP API responses are in JSON format
// with Content-Type: application/json and valid JSON body.

#[cfg(test)]
mod json_format_tests {
    use super::*;

    // =========================================================
    // Test Case 1: GET /api/status - JSON format validation
    // =========================================================

    /// Verify StatusResponse serializes to valid JSON with required fields
    #[test]
    fn test_status_response_json_format() {
        let status = StatusResponse {
            levels: vec![
                LevelInfo { level: 0, files: 3 },
                LevelInfo { level: 1, files: 12 },
            ],
            memory: MemoryInfo {
                used_bytes: 1048576,
                percentage: 45.5,
            },
            compaction: "idle".to_string(),
        };

        // Serialize to JSON
        let json = serde_json::to_string(&status).expect("Should serialize to JSON");

        // Verify it's valid JSON by parsing
        let parsed: serde_json::Value = serde_json::from_str(&json)
            .expect("Serialized output should be valid JSON");

        // Verify required fields are present
        assert!(parsed.get("levels").is_some(), "Should have 'levels' field");
        assert!(parsed.get("memory").is_some(), "Should have 'memory' field");
        assert!(parsed.get("compaction").is_some(), "Should have 'compaction' field");

        // Verify levels is an array
        assert!(parsed["levels"].is_array(), "levels should be an array");

        // Verify memory is an object with required fields
        assert!(parsed["memory"]["used_bytes"].is_number(), "memory.used_bytes should be a number");
        assert!(parsed["memory"]["percentage"].is_number(), "memory.percentage should be a number");

        // Verify compaction is a string
        assert!(parsed["compaction"].is_string(), "compaction should be a string");
    }

    /// Verify StatusResponse round-trip serialization
    #[test]
    fn test_status_response_roundtrip() {
        let original = StatusResponse {
            levels: vec![
                LevelInfo { level: 0, files: 5 },
                LevelInfo { level: 1, files: 20 },
                LevelInfo { level: 2, files: 0 },
            ],
            memory: MemoryInfo {
                used_bytes: 2097152,
                percentage: 75.0,
            },
            compaction: "compacting level 0 to level 1".to_string(),
        };

        let json = serde_json::to_string(&original).expect("Serialize");
        let restored: StatusResponse = serde_json::from_str(&json).expect("Deserialize");

        assert_eq!(original.levels.len(), restored.levels.len());
        assert_eq!(original.memory.used_bytes, restored.memory.used_bytes);
        assert_eq!(original.memory.percentage, restored.memory.percentage);
        assert_eq!(original.compaction, restored.compaction);
    }

    // =========================================================
    // Test Case 2: GET /api/key/{key} - JSON format validation
    // =========================================================

    /// Verify KeyResponse serializes to valid JSON with required fields
    #[test]
    fn test_key_response_json_format() {
        let key_resp = KeyResponse {
            value: "test_value_123".to_string(),
            flags: 42,
            ttl: 3600,
            bytes: 14,
        };

        let json = serde_json::to_string(&key_resp).expect("Should serialize to JSON");

        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json)
            .expect("Serialized output should be valid JSON");

        // Verify required fields are present with correct types
        assert!(parsed["value"].is_string(), "value should be a string");
        assert!(parsed["flags"].is_number(), "flags should be a number");
        assert!(parsed["ttl"].is_number(), "ttl should be a number");
        assert!(parsed["bytes"].is_number(), "bytes should be a number");

        // Verify field values
        assert_eq!(parsed["value"], "test_value_123");
        assert_eq!(parsed["flags"], 42);
        assert_eq!(parsed["ttl"], 3600);
        assert_eq!(parsed["bytes"], 14);
    }

    /// Verify KeyResponse handles special characters in value
    #[test]
    fn test_key_response_special_characters() {
        let special_values = vec![
            r#"{"nested": "json"}"#,
            "line1\nline2\ttabbed",
            "unicode: café ñ 日本語",
            r#"quotes: "hello" and 'world'"#,
            "backslash: \\path\\to\\file",
        ];

        for value in special_values {
            let key_resp = KeyResponse {
                value: value.to_string(),
                flags: 0,
                ttl: 0,
                bytes: value.len(),
            };

            let json = serde_json::to_string(&key_resp)
                .expect(&format!("Should serialize value: {}", value));

            // Verify it's valid JSON
            let parsed: KeyResponse = serde_json::from_str(&json)
                .expect(&format!("Should deserialize value: {}", value));

            assert_eq!(parsed.value, value, "Value should roundtrip correctly");
        }
    }

    /// Verify KeyResponse handles empty value
    #[test]
    fn test_key_response_empty_value() {
        let key_resp = KeyResponse {
            value: "".to_string(),
            flags: 0,
            ttl: 0,
            bytes: 0,
        };

        let json = serde_json::to_string(&key_resp).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");

        assert_eq!(parsed["value"], "", "Empty value should serialize as empty string");
        assert_eq!(parsed["bytes"], 0);
    }

    // =========================================================
    // Test Case 3: POST /api/key - JSON format validation
    // =========================================================

    /// Verify SuccessResponse serializes to valid JSON
    #[test]
    fn test_success_response_json_format() {
        let success = SuccessResponse::new(true);
        let json = serde_json::to_string(&success).expect("Should serialize");

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");
        assert!(parsed["success"].is_boolean(), "success should be a boolean");
        assert_eq!(parsed["success"], true);

        // Test false case
        let failure = SuccessResponse::new(false);
        let json = serde_json::to_string(&failure).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");
        assert_eq!(parsed["success"], false);
    }

    /// Verify SetKeyRequest deserializes from valid JSON
    #[test]
    fn test_set_key_request_json_format() {
        let json = r#"{"key":"user:123","value":"hello world","flags":42,"ttl":3600}"#;

        let request: SetKeyRequest = serde_json::from_str(json)
            .expect("Should deserialize valid JSON");

        assert_eq!(request.key, "user:123");
        assert_eq!(request.value, "hello world");
        assert_eq!(request.flags, 42);
        assert_eq!(request.ttl, 3600);
    }

    /// Verify SetKeyRequest handles optional fields
    #[test]
    fn test_set_key_request_optional_fields() {
        // Minimal request with only required fields
        let json = r#"{"key":"minimal","value":"data"}"#;

        let request: SetKeyRequest = serde_json::from_str(json)
            .expect("Should deserialize with minimal fields");

        assert_eq!(request.key, "minimal");
        assert_eq!(request.value, "data");
        assert_eq!(request.flags, 0, "flags should default to 0");
        assert_eq!(request.ttl, 0, "ttl should default to 0");
    }

    // =========================================================
    // Test Case 4: Error responses - JSON format validation
    // =========================================================

    /// Verify ErrorResponse serializes to valid JSON with error field
    #[test]
    fn test_error_response_json_format() {
        let error = ErrorResponse::new("Key not found");
        let json = serde_json::to_string(&error).expect("Should serialize");

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");
        assert!(parsed["error"].is_string(), "error should be a string");
        assert_eq!(parsed["error"], "Key not found");
    }

    /// Verify ErrorResponse handles various error messages
    #[test]
    fn test_error_response_various_messages() {
        let error_messages = vec![
            "Key not found",
            "Invalid JSON: unexpected end of input",
            "Key is required",
            "Method not allowed. Supported methods: GET, POST, DELETE",
            "Server error: connection timeout",
        ];

        for msg in error_messages {
            let error = ErrorResponse::new(msg);
            let json = serde_json::to_string(&error)
                .expect(&format!("Should serialize: {}", msg));

            let parsed: ErrorResponse = serde_json::from_str(&json)
                .expect(&format!("Should deserialize: {}", msg));

            assert_eq!(parsed.error, msg, "Error message should roundtrip");
        }
    }

    /// Verify ErrorResponse handles special characters in error messages
    #[test]
    fn test_error_response_special_characters() {
        let error = ErrorResponse::new(r#"Invalid JSON: expected '"' at line 1"#);
        let json = serde_json::to_string(&error).expect("Should serialize");

        // Should be valid JSON even with quotes in the message
        let parsed: ErrorResponse = serde_json::from_str(&json)
            .expect("Should deserialize");

        assert!(parsed.error.contains("Invalid JSON"));
    }

    // =========================================================
    // Additional JSON format validation tests
    // =========================================================

    /// Verify CompactResponse serializes to valid JSON
    #[test]
    fn test_compact_response_json_format() {
        let response = CompactResponse {
            success: true,
            message: "Compaction initiated".to_string(),
        };

        let json = serde_json::to_string(&response).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");

        assert!(parsed["success"].is_boolean(), "success should be a boolean");
        assert!(parsed["message"].is_string(), "message should be a string");
        assert_eq!(parsed["success"], true);
        assert_eq!(parsed["message"], "Compaction initiated");
    }

    /// Verify LevelInfo serializes with correct field names
    #[test]
    fn test_level_info_json_format() {
        let level = LevelInfo { level: 0, files: 5 };
        let json = serde_json::to_string(&level).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");

        assert!(parsed["level"].is_number(), "level should be a number");
        assert!(parsed["files"].is_number(), "files should be a number");
    }

    /// Verify MemoryInfo serializes with correct field names
    #[test]
    fn test_memory_info_json_format() {
        let memory = MemoryInfo {
            used_bytes: 1048576,
            percentage: 50.5,
        };
        let json = serde_json::to_string(&memory).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Valid JSON");

        assert!(parsed["used_bytes"].is_number(), "used_bytes should be a number");
        assert!(parsed["percentage"].is_number(), "percentage should be a number");
    }

    /// Verify all response types produce Content-Type compatible JSON
    #[test]
    fn test_all_responses_content_type_compatible() {
        // All response types should produce valid JSON that can be served
        // with Content-Type: application/json

        // StatusResponse
        let status_json = serde_json::to_string(&StatusResponse {
            levels: vec![LevelInfo { level: 0, files: 0 }],
            memory: MemoryInfo { used_bytes: 0, percentage: 0.0 },
            compaction: "idle".to_string(),
        }).expect("StatusResponse should serialize");
        assert!(status_json.starts_with("{"), "Should be JSON object");

        // KeyResponse
        let key_json = serde_json::to_string(&KeyResponse {
            value: "test".to_string(),
            flags: 0,
            ttl: 0,
            bytes: 4,
        }).expect("KeyResponse should serialize");
        assert!(key_json.starts_with("{"), "Should be JSON object");

        // SuccessResponse
        let success_json = serde_json::to_string(&SuccessResponse::new(true))
            .expect("SuccessResponse should serialize");
        assert!(success_json.starts_with("{"), "Should be JSON object");

        // ErrorResponse
        let error_json = serde_json::to_string(&ErrorResponse::new("error"))
            .expect("ErrorResponse should serialize");
        assert!(error_json.starts_with("{"), "Should be JSON object");

        // CompactResponse
        let compact_json = serde_json::to_string(&CompactResponse {
            success: true,
            message: "ok".to_string(),
        }).expect("CompactResponse should serialize");
        assert!(compact_json.starts_with("{"), "Should be JSON object");
    }

    /// Verify JSON output is not pretty-printed (compact for API responses)
    #[test]
    fn test_json_is_compact() {
        let response = SuccessResponse::new(true);
        let json = serde_json::to_string(&response).expect("Should serialize");

        // Compact JSON should not have newlines or excessive whitespace
        assert!(!json.contains("\n"), "Compact JSON should not have newlines");
        assert!(!json.contains("  "), "Compact JSON should not have double spaces");
    }

    /// Verify large responses still produce valid JSON
    #[test]
    fn test_large_response_valid_json() {
        // Create a response with many levels
        let levels: Vec<LevelInfo> = (0..100)
            .map(|i| LevelInfo { level: i, files: i * 10 })
            .collect();

        let status = StatusResponse {
            levels,
            memory: MemoryInfo {
                used_bytes: 1073741824, // 1GB
                percentage: 99.9,
            },
            compaction: "idle".to_string(),
        };

        let json = serde_json::to_string(&status).expect("Should serialize large response");
        let _: StatusResponse = serde_json::from_str(&json).expect("Should deserialize large response");
    }

    /// Verify KeyResponse with very long value produces valid JSON
    #[test]
    fn test_large_value_valid_json() {
        let large_value = "x".repeat(100000);
        let response = KeyResponse {
            value: large_value.clone(),
            flags: 0,
            ttl: 0,
            bytes: large_value.len(),
        };

        let json = serde_json::to_string(&response).expect("Should serialize large value");
        let restored: KeyResponse = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(restored.value.len(), large_value.len());
    }
}
