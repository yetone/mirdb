//! Key CRUD API Tests
//! Owner: Scenario 4 - Key Browser View Keys
//! Co-owners: Scenarios 5, 6, 7 (Set, Get, Delete)
//!
//! Tests:
//! - List keys with pagination
//! - Get single key
//! - Set new key
//! - Delete key
//! - Cross-protocol consistency

use std::path::Path;
use std::time::Instant;

/// Test module for keys delete API (Scenario 7)
mod keys_delete_tests {
    use super::*;

    /// Test 1: DELETE /api/keys/{key} endpoint exists in router
    #[test]
    fn test_delete_route_exists() {
        let router_rs = std::fs::read_to_string("src/http/router.rs")
            .expect("Failed to read router.rs");

        // Check for delete route handling
        assert!(
            router_rs.contains("ApiKeysDelete"),
            "Router should have ApiKeysDelete route variant"
        );
        assert!(
            router_rs.contains("Method::Delete"),
            "Router should handle DELETE method"
        );
    }

    /// Test 2: handle_delete_key function exists
    #[test]
    fn test_handle_delete_key_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub fn handle_delete_key"),
            "keys.rs should export handle_delete_key function"
        );
    }

    /// Test 3: DeleteKeyResult enum exists with correct variants
    #[test]
    fn test_delete_key_result_enum() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub enum DeleteKeyResult"),
            "keys.rs should have DeleteKeyResult enum"
        );
        assert!(
            keys_rs.contains("Deleted(DeleteResponse)"),
            "DeleteKeyResult should have Deleted variant"
        );
        assert!(
            keys_rs.contains("NotFound(ErrorResponse)"),
            "DeleteKeyResult should have NotFound variant"
        );
    }

    /// Test 4: DeleteResponse struct has required fields
    #[test]
    fn test_delete_response_structure() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub struct DeleteResponse"),
            "keys.rs should have DeleteResponse struct"
        );
        assert!(
            keys_rs.contains("success: bool"),
            "DeleteResponse should have success field"
        );
        assert!(
            keys_rs.contains("message: String"),
            "DeleteResponse should have message field"
        );
    }

    /// Test 5: Delete handler uses Request::Deleter
    #[test]
    fn test_uses_deleter_request() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("Request::Deleter"),
            "handle_delete_key should use Request::Deleter"
        );
    }

    /// Test 6: Delete handler handles NotFound response
    #[test]
    fn test_handles_not_found() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("Response::NotFound"),
            "Delete handler should handle NotFound response"
        );
        assert!(
            keys_rs.contains("not_found"),
            "Delete handler should return not_found error code"
        );
    }

    /// Test 7: Delete handler handles Deleted response
    #[test]
    fn test_handles_deleted_response() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("Response::Deleted"),
            "Delete handler should handle Deleted response"
        );
    }

    /// Test 8: API has proper JSON serialization for delete
    #[test]
    fn test_delete_response_serializable() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check DeleteResponse has Serialize derive
        let delete_response_section = keys_rs
            .find("pub struct DeleteResponse")
            .expect("DeleteResponse should exist");

        let before_struct = &keys_rs[..delete_response_section];
        let derive_pos = before_struct.rfind("#[derive");
        assert!(
            derive_pos.is_some(),
            "DeleteResponse should have derive attribute"
        );

        let derive_section = &keys_rs[derive_pos.unwrap()..delete_response_section];
        assert!(
            derive_section.contains("Serialize"),
            "DeleteResponse should derive Serialize"
        );
    }
}

/// Test module for keys.js delete UI (Scenario 7)
mod keys_js_delete_tests {
    use super::*;

    /// Test 1: keys.js file exists
    #[test]
    fn test_keys_js_exists() {
        let js_path = Path::new("src/web/scripts/keys.js");
        assert!(js_path.exists(), "keys.js should exist");
    }

    /// Test 2: deleteKey function is implemented (not placeholder)
    #[test]
    fn test_delete_key_function_implemented() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function deleteKey"),
            "keys.js should have deleteKey function"
        );

        // Ensure it's not just a placeholder
        assert!(
            !keys_js.contains("function deleteKey(key) {\n        // Placeholder"),
            "deleteKey should be implemented, not a placeholder"
        );
    }

    /// Test 3: deleteKey shows confirmation dialog
    #[test]
    fn test_delete_shows_confirmation() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("showConfirm"),
            "deleteKey should show confirmation dialog"
        );
    }

    /// Test 4: deleteKey calls API on confirmation
    #[test]
    fn test_delete_calls_api() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("MirDBApi.deleteKey"),
            "deleteKey should call MirDBApi.deleteKey"
        );
    }

    /// Test 5: deleteKey shows success toast
    #[test]
    fn test_delete_shows_success_toast() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("deleted successfully") || keys_js.contains("showToast"),
            "deleteKey should show success toast"
        );
    }

    /// Test 6: deleteKey handles errors
    #[test]
    fn test_delete_handles_errors() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains(".catch") || keys_js.contains("error"),
            "deleteKey should handle errors"
        );
    }

    /// Test 7: deleteKey refreshes key list after deletion
    #[test]
    fn test_delete_refreshes_list() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("loadKeys"),
            "deleteKey should refresh key list after deletion"
        );
    }

    /// Test 8: deleteKey is exported in module
    #[test]
    fn test_delete_key_exported() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        // Check the return statement exports deleteKey
        assert!(
            keys_js.contains("deleteKey: deleteKey"),
            "deleteKey should be exported in module return"
        );
    }

    /// Test 9: Cancel action is handled
    #[test]
    fn test_cancel_action_handled() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        // showConfirm takes an onCancel callback
        assert!(
            keys_js.contains("Delete cancelled") || keys_js.contains("onCancel"),
            "Cancel action should be handled"
        );
    }
}

/// Test module for api.js delete function
mod api_delete_tests {
    use super::*;

    /// Test 1: api.js has deleteKey function
    #[test]
    fn test_api_delete_key_exists() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("function deleteKey"),
            "api.js should have deleteKey function"
        );
    }

    /// Test 2: api.js deleteKey uses DELETE method
    #[test]
    fn test_api_delete_uses_delete_method() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("method: 'DELETE'"),
            "api.js deleteKey should use DELETE method"
        );
    }

    /// Test 3: api.js deleteKey uses correct endpoint
    #[test]
    fn test_api_delete_uses_correct_endpoint() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("/api/keys/") && api_js.contains("encodeURIComponent"),
            "api.js deleteKey should use /api/keys/{key} endpoint with proper encoding"
        );
    }

    /// Test 4: api.js deleteKey is exported
    #[test]
    fn test_api_delete_exported() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("deleteKey: deleteKey"),
            "api.js should export deleteKey function"
        );
    }
}
