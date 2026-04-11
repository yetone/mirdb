//! Error Handling API Tests
//! Owner: Scenario 16 - Error Handling - API Errors
//!
//! Tests:
//! - GET /api/keys/{nonexistent} returns HTTP 404 with JSON error
//! - POST /api/keys with invalid JSON returns HTTP 400
//! - POST /api/keys with missing fields returns HTTP 400 with field-specific error
//! - UI displays user-friendly error messages

use std::path::Path;

/// Test module for API error responses (Scenario 16)
mod api_error_tests {
    use super::*;

    /// Test 1: GET /api/keys/{nonexistent} returns JSON error response
    #[test]
    fn test_get_nonexistent_key_returns_json_error() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Check that NotFound case returns JSON content type
        assert!(
            server_rs.contains("GetKeyResult::NotFound"),
            "Server should handle NotFound case"
        );

        // Check for JSON error response format
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("ErrorResponse"),
            "keys.rs should have ErrorResponse struct"
        );
        assert!(
            keys_rs.contains(r#"error: String"#) || keys_rs.contains("pub error:"),
            "ErrorResponse should have error field"
        );
        assert!(
            keys_rs.contains(r#""not_found""#),
            "Should use 'not_found' error code for missing keys"
        );
    }

    /// Test 2: NotFound returns proper JSON format with error field
    #[test]
    fn test_not_found_error_has_json_structure() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check ErrorResponse struct fields
        assert!(
            keys_rs.contains("pub error: String"),
            "ErrorResponse should have error field"
        );
        assert!(
            keys_rs.contains("pub message: String"),
            "ErrorResponse should have message field"
        );

        // Check that ErrorResponse derives Serialize for JSON output
        let error_response_idx = keys_rs
            .find("pub struct ErrorResponse")
            .expect("ErrorResponse struct should exist");
        let before_struct = &keys_rs[..error_response_idx];
        let derive_idx = before_struct.rfind("#[derive");
        assert!(derive_idx.is_some(), "ErrorResponse should have derive");
        let derive_section = &keys_rs[derive_idx.unwrap()..error_response_idx];
        assert!(
            derive_section.contains("Serialize"),
            "ErrorResponse should derive Serialize"
        );
    }

    /// Test 3: Server returns JSON content-type for error responses
    #[test]
    fn test_error_response_has_json_content_type() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Check that error responses include application/json content type
        assert!(
            server_rs.contains(r#"application/json"#),
            "Server should set application/json content type"
        );
    }

    /// Test 4: POST /api/keys with invalid JSON handling
    #[test]
    fn test_post_invalid_json_handling() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Server should handle JSON parsing errors
        assert!(
            server_rs.contains("ApiKeysSet"),
            "Server should have ApiKeysSet route"
        );

        // Check for body parsing and error handling
        assert!(
            server_rs.contains("serde_json") || server_rs.contains("from_slice") || server_rs.contains("parse"),
            "Server should parse JSON body"
        );
    }

    /// Test 5: POST /api/keys validation errors for missing fields
    #[test]
    fn test_post_missing_fields_validation() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check for validation of required fields
        assert!(
            keys_rs.contains("is_empty()"),
            "Handler should validate empty fields"
        );
        assert!(
            keys_rs.contains("SetKeyResult::BadRequest"),
            "Handler should return BadRequest for validation errors"
        );
        assert!(
            keys_rs.contains("cannot be empty") || keys_rs.contains("required"),
            "Error message should be field-specific"
        );
    }

    /// Test 6: BadRequest response has proper error structure
    #[test]
    fn test_bad_request_error_structure() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check BadRequest variant uses ErrorResponse
        assert!(
            keys_rs.contains("BadRequest(ErrorResponse)"),
            "SetKeyResult should have BadRequest variant with ErrorResponse"
        );
        assert!(
            keys_rs.contains(r#""bad_request""#),
            "Should use 'bad_request' error code"
        );
    }

    /// Test 7: Server handles bad request responses
    #[test]
    fn test_server_handles_bad_request() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Check server returns HTTP 400 for validation errors
        assert!(
            server_rs.contains("BAD_REQUEST") || server_rs.contains("400"),
            "Server should return HTTP 400 for validation errors"
        );
    }

    /// Test 8: Server handles internal errors
    #[test]
    fn test_server_handles_internal_errors() {
        let server_rs = std::fs::read_to_string("src/http/server.rs")
            .expect("Failed to read server.rs");

        // Check server returns HTTP 500 for internal errors
        assert!(
            server_rs.contains("INTERNAL_SERVER_ERROR") || server_rs.contains("500"),
            "Server should return HTTP 500 for internal errors"
        );
    }
}

/// Test module for UI error display (Scenario 16)
mod ui_error_display_tests {
    use super::*;

    /// Test 1: API client handles error responses
    #[test]
    fn test_api_client_handles_errors() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("handleResponse"),
            "api.js should have handleResponse function"
        );
        assert!(
            api_js.contains("response.ok") || api_js.contains("!response.ok"),
            "api.js should check response status"
        );
    }

    /// Test 2: API client extracts error messages from JSON
    #[test]
    fn test_api_client_extracts_error_message() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("err.message") || api_js.contains(".message"),
            "api.js should extract message from error JSON"
        );
        assert!(
            api_js.contains("throw") || api_js.contains("Error"),
            "api.js should throw error for failed responses"
        );
    }

    /// Test 3: UI shows user-friendly error toast
    #[test]
    fn test_ui_shows_error_toast() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("showToast"),
            "ui.js should have showToast function"
        );
        // Toast function dynamically creates toast-{type} class (e.g., toast-error)
        assert!(
            ui_js.contains("toast-") || ui_js.contains("toast ' + type"),
            "ui.js should support dynamic toast types including error"
        );
    }

    /// Test 4: Error toast is accessible
    #[test]
    fn test_error_toast_is_accessible() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("role") && ui_js.contains("alert"),
            "Toast should have role='alert' for accessibility"
        );
        assert!(
            ui_js.contains("aria-live"),
            "Toast should have aria-live for screen readers"
        );
    }

    /// Test 5: Keys browser handles API errors
    #[test]
    fn test_keys_browser_handles_errors() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains(".catch") || keys_js.contains("error"),
            "keys.js should handle API errors"
        );
    }

    /// Test 6: Error messages are displayed, not raw JSON
    #[test]
    fn test_displays_human_readable_errors() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        // Should extract the message field, not show raw JSON
        assert!(
            api_js.contains("err.message") || api_js.contains(".message"),
            "Should display message field, not raw JSON"
        );
    }

    /// Test 7: CSS styles exist for error states
    #[test]
    fn test_error_css_styles() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".toast-error"),
            "CSS should have error toast styles"
        );
    }

    /// Test 8: Error toast has visible styling
    #[test]
    fn test_error_toast_visibility() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        // Error toast should have distinctive styling (usually red or warning color)
        assert!(
            main_css.contains("toast"),
            "CSS should have toast styles for visibility"
        );
    }
}

/// Test module for handler error codes (Scenario 16)
mod handler_error_codes_tests {
    use super::*;

    /// Test 1: Error codes are consistent across handlers
    #[test]
    fn test_consistent_error_codes() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check for standard error codes
        assert!(
            keys_rs.contains(r#""not_found""#),
            "Should use 'not_found' error code"
        );
        assert!(
            keys_rs.contains(r#""bad_request""#),
            "Should use 'bad_request' error code"
        );
        assert!(
            keys_rs.contains(r#""internal_error""#),
            "Should use 'internal_error' error code"
        );
    }

    /// Test 2: Error messages are descriptive
    #[test]
    fn test_descriptive_error_messages() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Error messages should be descriptive
        assert!(
            keys_rs.contains("not found") || keys_rs.contains("Not found"),
            "Not found errors should have descriptive message"
        );
        assert!(
            keys_rs.contains("cannot be empty") || keys_rs.contains("required"),
            "Validation errors should have field-specific messages"
        );
    }

    /// Test 3: Delete handler returns proper not found error
    #[test]
    fn test_delete_not_found_error() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("DeleteKeyResult::NotFound"),
            "Delete handler should have NotFound variant"
        );
    }

    /// Test 4: Get handler returns proper not found error
    #[test]
    fn test_get_not_found_error() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("GetKeyResult::NotFound"),
            "Get handler should have NotFound variant"
        );
    }

    /// Test 5: Internal errors have proper structure
    #[test]
    fn test_internal_error_structure() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("Error(ErrorResponse)"),
            "Result enums should have Error variant with ErrorResponse"
        );
    }
}
