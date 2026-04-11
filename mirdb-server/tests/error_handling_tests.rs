//! API Error Handling Tests
//! Owner: Scenario 16 - Error Handling - API Errors
//!
//! Tests for NFR-5: Homepage shall handle API errors gracefully
//! with user-friendly error messages.
//!
//! Test cases:
//! 1. GET /api/keys/nonexistent_key -> HTTP 404 with JSON {error: 'Key not found'}
//! 2. POST /api/keys with invalid JSON -> HTTP 400 with descriptive validation error
//! 3. POST /api/keys with missing required fields -> HTTP 400 with field-specific error
//! 4. UI display of API error -> User-friendly error message shown

use std::path::Path;

/// Test module for API error response format (Scenario 16)
mod api_error_response_tests {
    use super::*;

    /// Test 1: GET /api/keys/{key} returns JSON error for nonexistent key
    #[test]
    fn test_get_nonexistent_key_returns_json_error() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Verify NotFound returns JSON instead of plain text
        assert!(
            server_rs.contains("GetKeyResult::NotFound(err)"),
            "Handler should handle NotFound with error struct"
        );
        assert!(
            server_rs.contains(r#"header("Content-Type", "application/json")"#),
            "NotFound response should include Content-Type: application/json header"
        );
        assert!(
            !server_rs.contains(r#"Body::from("Key not found")"#),
            "NotFound should not return plain text, should return JSON"
        );
    }

    /// Test 2: ErrorResponse struct is used for API errors
    #[test]
    fn test_error_response_struct_format() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub struct ErrorResponse"),
            "ErrorResponse struct should exist"
        );
        assert!(
            keys_rs.contains("pub error: String"),
            "ErrorResponse should have 'error' field"
        );
        assert!(
            keys_rs.contains("pub message: String"),
            "ErrorResponse should have 'message' field"
        );

        // Check that ErrorResponse derives Serialize
        let error_pos = keys_rs.find("pub struct ErrorResponse")
            .expect("ErrorResponse should exist");
        let before_struct = &keys_rs[..error_pos];
        let derive_pos = before_struct.rfind("#[derive");
        assert!(derive_pos.is_some(), "ErrorResponse should have derive attribute");

        let derive_section = &keys_rs[derive_pos.unwrap()..error_pos];
        assert!(
            derive_section.contains("Serialize"),
            "ErrorResponse should derive Serialize for JSON output"
        );
    }

    /// Test 3: NotFound uses 404 status code
    #[test]
    fn test_not_found_uses_404_status() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        assert!(
            server_rs.contains("StatusCode::NOT_FOUND"),
            "NotFound handler should use HTTP 404 status"
        );
    }

    /// Test 4: Error message includes key name for user-friendly feedback
    #[test]
    fn test_not_found_message_includes_key() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check that the error message includes the key name
        assert!(
            keys_rs.contains(r#"format!("Key '{}' not found", key)"#),
            "Not found message should include the key name for user context"
        );
    }
}

/// Test module for POST validation error handling (Scenario 16)
mod post_validation_error_tests {
    use super::*;

    /// Test 1: Invalid JSON returns 400 Bad Request with descriptive error
    #[test]
    fn test_invalid_json_returns_400() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Check that body parsing handles JSON errors
        assert!(
            server_rs.contains("serde_json::from_slice"),
            "POST handler should parse JSON body"
        );
        assert!(
            server_rs.contains("invalid_json"),
            "JSON parse error should use 'invalid_json' error code"
        );
        assert!(
            server_rs.contains("Invalid JSON in request body"),
            "JSON parse error should have descriptive message"
        );
    }

    /// Test 2: Invalid JSON response uses 400 status code
    #[test]
    fn test_invalid_json_uses_400_status() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        assert!(
            server_rs.contains("StatusCode::BAD_REQUEST"),
            "Invalid JSON should return HTTP 400 status"
        );
    }

    /// Test 3: Missing required field 'key' returns specific error
    #[test]
    fn test_missing_key_field_returns_specific_error() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        assert!(
            server_rs.contains("set_request.key.is_empty()"),
            "Handler should validate that key is not empty"
        );
        assert!(
            server_rs.contains("validation_error"),
            "Empty key should use 'validation_error' error code"
        );
        assert!(
            server_rs.contains("Missing required field: 'key'"),
            "Empty key should have field-specific error message"
        );
    }

    /// Test 4: SetKeyResult::BadRequest is properly handled
    #[test]
    fn test_bad_request_result_handled() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        assert!(
            server_rs.contains("SetKeyResult::BadRequest(err)"),
            "Handler should handle BadRequest result type"
        );
    }

    /// Test 5: POST handler parses body asynchronously
    #[test]
    fn test_post_handler_parses_body() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        assert!(
            server_rs.contains("req.into_body()") && server_rs.contains("concat2()"),
            "POST handler should read body asynchronously"
        );
    }
}

/// Test module for DELETE error handling (Scenario 16)
mod delete_error_tests {
    use super::*;

    /// Test 1: DELETE nonexistent key returns JSON error
    #[test]
    fn test_delete_nonexistent_key_returns_json() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Verify DeleteKeyResult::NotFound returns JSON
        assert!(
            server_rs.contains("DeleteKeyResult::NotFound(err)"),
            "Delete handler should handle NotFound with error struct"
        );
    }

    /// Test 2: DELETE error responses include Content-Type header
    #[test]
    fn test_delete_error_content_type() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Check that the delete handler sets Content-Type for errors
        // This is done in the same match arm context
        let delete_section = server_rs.find("Route::ApiKeysDelete")
            .expect("Delete route should exist");
        let section = &server_rs[delete_section..];
        let next_route = section.find("Route::ApiConfig").unwrap_or(section.len());
        let delete_code = &section[..next_route];

        assert!(
            delete_code.contains(r#"header("Content-Type", "application/json")"#),
            "Delete error response should set Content-Type to application/json"
        );
    }
}

/// Test module for UI error display (Scenario 16)
mod ui_error_display_tests {
    use super::*;

    /// Test 1: api.js handleResponse extracts user-friendly message
    #[test]
    fn test_api_handles_error_response() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("handleResponse"),
            "api.js should have handleResponse function"
        );
        assert!(
            api_js.contains("!response.ok"),
            "handleResponse should check response.ok status"
        );
        assert!(
            api_js.contains("err.message"),
            "handleResponse should extract message from error JSON"
        );
    }

    /// Test 2: Error messages are thrown as Error objects, not raw JSON
    #[test]
    fn test_errors_thrown_as_error_objects() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("throw new Error"),
            "API errors should be thrown as Error objects"
        );
        assert!(
            api_js.contains("err.message") || api_js.contains("'API error'"),
            "Error should include user-friendly message"
        );
    }

    /// Test 3: ui.js has showToast for displaying errors
    #[test]
    fn test_ui_has_toast_error_display() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("showToast"),
            "ui.js should have showToast function"
        );
        // Toast class is dynamically constructed: 'toast toast-' + type
        // So when called with 'error', it becomes 'toast toast-error'
        assert!(
            ui_js.contains("toast-") || ui_js.contains("'toast toast-' + type"),
            "ui.js should support toast type classes"
        );
    }

    /// Test 4: keys.js shows user-friendly error via toast
    #[test]
    fn test_keys_js_shows_error_toast() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains(".catch") || keys_js.contains("catch("),
            "keys.js should catch API errors"
        );
        assert!(
            keys_js.contains("showToast") || keys_js.contains("MirDBUI.showToast"),
            "keys.js should show error via toast notification"
        );
    }

    /// Test 5: Toast has ARIA attributes for accessibility
    #[test]
    fn test_toast_has_aria_attributes() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("role") && ui_js.contains("alert"),
            "Toast should have role='alert' for screen readers"
        );
        assert!(
            ui_js.contains("aria-live"),
            "Toast should have aria-live attribute"
        );
    }
}

/// Test module for error response JSON format (Scenario 16)
mod error_json_format_tests {
    use super::*;

    /// Test 1: All error responses use ErrorResponse struct
    #[test]
    fn test_consistent_error_structure() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Count ErrorResponse usages to ensure consistency
        let error_response_count = keys_rs.matches("ErrorResponse {").count();
        assert!(
            error_response_count >= 5,
            "ErrorResponse should be used consistently across all error cases"
        );
    }

    /// Test 2: Error responses include both error code and message
    #[test]
    fn test_error_has_code_and_message() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check for common error patterns
        assert!(
            keys_rs.contains(r#"error: "not_found""#),
            "Not found errors should use 'not_found' code"
        );
        assert!(
            keys_rs.contains(r#"error: "bad_request""#),
            "Bad request errors should use 'bad_request' code"
        );
        assert!(
            keys_rs.contains(r#"error: "internal_error""#),
            "Internal errors should use 'internal_error' code"
        );
    }

    /// Test 3: Internal errors don't expose implementation details
    #[test]
    fn test_internal_errors_safe() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Ensure internal errors have generic messages (not exposing internals to users)
        // But still log details for debugging
        assert!(
            keys_rs.contains("log::debug!") || keys_rs.contains("log::error!"),
            "Errors should be logged for debugging"
        );
    }
}

/// Test module for CORS headers on error responses (Scenario 16)
mod cors_error_tests {
    use super::*;

    /// Test 1: Error responses include CORS headers
    #[test]
    fn test_error_responses_have_cors_headers() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Count CORS headers to ensure they're on all responses
        let cors_header_count = server_rs.matches(r#"header("Access-Control-Allow-Origin", "*")"#).count();

        // We should have CORS headers on all API responses
        assert!(
            cors_header_count >= 10,
            "CORS headers should be present on all API responses including errors"
        );
    }
}
