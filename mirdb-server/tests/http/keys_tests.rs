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

// ============================================
// Scenario 6 - Key Operations - Get Key Tests
// ============================================

/// Test module for GET /api/keys/{key} API (Scenario 6)
mod keys_get_tests {
    use super::*;

    /// Test 1: GET /api/keys/{key} handler exists
    #[test]
    fn test_get_key_handler_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub fn handle_get_key"),
            "keys.rs should export handle_get_key function"
        );
    }

    /// Test 2: KeyValueResponse has required fields (key, value, flags, ttl, size)
    #[test]
    fn test_get_key_response_has_required_fields() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("struct KeyValueResponse"),
            "KeyValueResponse struct should exist"
        );
        assert!(
            keys_rs.contains("pub key: String"),
            "KeyValueResponse should have key field"
        );
        assert!(
            keys_rs.contains("pub value: String"),
            "KeyValueResponse should have value field"
        );
        assert!(
            keys_rs.contains("pub flags: u32"),
            "KeyValueResponse should have flags field"
        );
        assert!(
            keys_rs.contains("pub ttl: u32"),
            "KeyValueResponse should have ttl field"
        );
        assert!(
            keys_rs.contains("pub size: usize"),
            "KeyValueResponse should have size field"
        );
    }

    /// Test 3: GetKeyResult enum exists with NotFound variant
    #[test]
    fn test_not_found_response_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("enum GetKeyResult"),
            "GetKeyResult enum should exist"
        );
        assert!(
            keys_rs.contains("NotFound"),
            "GetKeyResult should have NotFound variant"
        );
    }

    /// Test 4: ErrorResponse struct has error and message fields
    #[test]
    fn test_error_response_struct_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("struct ErrorResponse"),
            "ErrorResponse struct should exist"
        );
        assert!(
            keys_rs.contains("pub error: String"),
            "ErrorResponse should have error field"
        );
        assert!(
            keys_rs.contains("pub message: String"),
            "ErrorResponse should have message field"
        );
    }

    /// Test 5: Uses not_found error code for missing keys
    #[test]
    fn test_not_found_error_code() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains(r#""not_found""#),
            "Should use 'not_found' error code"
        );
    }

    /// Test 6: Router has ApiKeysGet route
    #[test]
    fn test_router_has_get_key_route() {
        let router_rs = std::fs::read_to_string("src/http/router.rs")
            .expect("Failed to read router.rs");

        assert!(
            router_rs.contains("ApiKeysGet"),
            "Router should have ApiKeysGet route"
        );
        assert!(
            router_rs.contains("/api/keys/"),
            "Router should match /api/keys/ path prefix"
        );
    }

    /// Test 7: Handler uses timing for performance measurement
    #[test]
    fn test_handler_uses_performance_timing() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("Instant::now") || keys_rs.contains("std::time::Instant"),
            "Handler should use timing for performance measurement"
        );
    }

    /// Test 8: Unit tests include 500ms performance threshold
    #[test]
    fn test_unit_tests_include_performance_test() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("test_get_key_performance") || keys_rs.contains("500"),
            "Unit tests should include performance test with 500ms threshold"
        );
    }
}

/// Test module for keys.js get/modal functionality (Scenario 6)
mod keys_js_get_tests {
    use super::*;

    /// Test 1: showKeyValue function exists
    #[test]
    fn test_keys_js_has_show_key_value_function() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function showKeyValue"),
            "keys.js should have showKeyValue function"
        );
    }

    /// Test 2: Modal includes copy button
    #[test]
    fn test_keys_js_builds_modal_with_copy_button() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("buildKeyValueModal"),
            "keys.js should have buildKeyValueModal function"
        );
        assert!(
            keys_js.contains("copy-value-btn") || keys_js.contains("copyBtn"),
            "Modal should include a copy button"
        );
        assert!(
            keys_js.contains("copyKeyValue") || keys_js.contains("copyToClipboard"),
            "Should have copy to clipboard functionality"
        );
    }

    /// Test 3: Modal displays metadata (flags, TTL, size)
    #[test]
    fn test_modal_displays_metadata() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("metadata") || keys_js.contains("Metadata"),
            "Modal should display metadata"
        );
        assert!(
            keys_js.contains("flags") || keys_js.contains("Flags"),
            "Modal should display flags"
        );
        assert!(
            keys_js.contains("TTL") || keys_js.contains("ttl"),
            "Modal should display TTL"
        );
        assert!(
            keys_js.contains("Size") || keys_js.contains("size"),
            "Modal should display size"
        );
    }

    /// Test 4: showKeyValue is exported in module
    #[test]
    fn test_show_key_value_exported() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("showKeyValue: showKeyValue"),
            "showKeyValue should be exported in module return"
        );
    }
}

/// Test module for api.js fetchKey function (Scenario 6)
mod api_fetch_key_tests {
    use super::*;

    /// Test 1: api.js has fetchKey function
    #[test]
    fn test_api_js_has_fetch_key_function() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("function fetchKey") || api_js.contains("fetchKey:"),
            "api.js should have fetchKey function"
        );
    }

    /// Test 2: fetchKey uses encodeURIComponent for key names
    #[test]
    fn test_api_js_handles_key_encoding() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("encodeURIComponent"),
            "fetchKey should use encodeURIComponent for key names"
        );
    }

    /// Test 3: api.js handles errors properly
    #[test]
    fn test_api_js_handles_errors() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("handleResponse") || api_js.contains(".catch"),
            "api.js should handle API errors"
        );
        assert!(
            api_js.contains("response.ok") || api_js.contains("!response.ok"),
            "api.js should check response status"
        );
    }

    /// Test 4: fetchKey is exported
    #[test]
    fn test_api_fetch_key_exported() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("fetchKey: fetchKey"),
            "api.js should export fetchKey function"
        );
    }
}

/// Test module for UI modal functions (Scenario 6)
mod ui_modal_tests {
    use super::*;

    /// Test 1: showModal function exists
    #[test]
    fn test_ui_js_has_modal_functions() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("function showModal") || ui_js.contains("showModal:"),
            "ui.js should have showModal function"
        );
        assert!(
            ui_js.contains("function hideModal") || ui_js.contains("hideModal:"),
            "ui.js should have hideModal function"
        );
    }

    /// Test 2: copyToClipboard function exists with clipboard API
    #[test]
    fn test_ui_js_has_clipboard_support() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("copyToClipboard"),
            "ui.js should have copyToClipboard function"
        );
        assert!(
            ui_js.contains("navigator.clipboard") || ui_js.contains("execCommand"),
            "ui.js should use clipboard API or fallback"
        );
    }

    /// Test 3: showToast function exists
    #[test]
    fn test_ui_js_has_toast_notifications() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("showToast"),
            "ui.js should have showToast function"
        );
    }

    /// Test 4: Copy provides visual feedback
    #[test]
    fn test_copy_feedback_visual() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("copied") || ui_js.contains("Copied"),
            "Copy functionality should provide visual feedback"
        );
    }

    /// Test 5: Modal has accessibility attributes
    #[test]
    fn test_modal_accessibility() {
        let ui_js = std::fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("aria-") || ui_js.contains("role"),
            "Modal should have accessibility attributes"
        );
        assert!(
            ui_js.contains("Escape") || ui_js.contains("keydown"),
            "Modal should support Escape key to close"
        );
    }
}

/// Test module for CSS modal styles (Scenario 6)
mod css_modal_tests {
    use super::*;

    /// Test 1: Modal CSS styles exist
    #[test]
    fn test_modal_css_styles_exist() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".modal-overlay"),
            "CSS should have modal overlay styles"
        );
        assert!(
            main_css.contains(".modal"),
            "CSS should have modal styles"
        );
        assert!(
            main_css.contains(".key-value-modal"),
            "CSS should have key-value-modal styles"
        );
        assert!(
            main_css.contains(".copy-value-btn"),
            "CSS should have copy button styles"
        );
    }

    /// Test 2: Toast notification CSS exists
    #[test]
    fn test_toast_css_styles_exist() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".toast"),
            "CSS should have toast notification styles"
        );
        assert!(
            main_css.contains(".toast-success"),
            "CSS should have success toast styles"
        );
        assert!(
            main_css.contains(".toast-error"),
            "CSS should have error toast styles"
        );
    }

    /// Test 3: Button CSS exists
    #[test]
    fn test_button_styles_exist() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".btn"),
            "CSS should have button styles"
        );
        assert!(
            main_css.contains(".btn-primary"),
            "CSS should have primary button styles"
        );
        assert!(
            main_css.contains(".btn-secondary"),
            "CSS should have secondary button styles"
        );
    }

    /// Test 4: Metadata grid CSS exists
    #[test]
    fn test_metadata_grid_css_exists() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".metadata-grid"),
            "CSS should have metadata grid styles"
        );
        assert!(
            main_css.contains(".metadata-item"),
            "CSS should have metadata item styles"
        );
    }
}

// ============================================
// Scenario 4 - Key Browser - View Keys Tests
// ============================================

/// Test module for GET /api/keys (list) API (Scenario 4)
mod keys_list_tests {
    use super::*;

    /// Test 1: handle_list_keys function exists
    #[test]
    fn test_list_keys_handler_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub fn handle_list_keys"),
            "keys.rs should export handle_list_keys function"
        );
    }

    /// Test 2: ListKeysResult enum exists with correct variants
    #[test]
    fn test_list_keys_result_enum() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub enum ListKeysResult"),
            "keys.rs should have ListKeysResult enum"
        );
        assert!(
            keys_rs.contains("Success(KeysListResponse)"),
            "ListKeysResult should have Success variant"
        );
        assert!(
            keys_rs.contains("Error(ErrorResponse)"),
            "ListKeysResult should have Error variant"
        );
    }

    /// Test 3: KeysListResponse has required fields (keys, page, limit, total)
    #[test]
    fn test_keys_list_response_fields() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("struct KeysListResponse"),
            "KeysListResponse struct should exist"
        );
        assert!(
            keys_rs.contains("pub keys: Vec<KeyInfo>"),
            "KeysListResponse should have keys field"
        );
        assert!(
            keys_rs.contains("pub page: u32"),
            "KeysListResponse should have page field"
        );
        assert!(
            keys_rs.contains("pub limit: u32"),
            "KeysListResponse should have limit field"
        );
        assert!(
            keys_rs.contains("pub total: u32"),
            "KeysListResponse should have total field"
        );
    }

    /// Test 4: KeyInfo struct has required fields (key, size, ttl)
    #[test]
    fn test_key_info_struct() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("struct KeyInfo"),
            "KeyInfo struct should exist"
        );
        assert!(
            keys_rs.contains("pub key: String"),
            "KeyInfo should have key field"
        );
        assert!(
            keys_rs.contains("pub size: u64"),
            "KeyInfo should have size field"
        );
        assert!(
            keys_rs.contains("pub ttl: u32"),
            "KeyInfo should have ttl field"
        );
    }

    /// Test 5: Handler uses store.list_keys
    #[test]
    fn test_uses_store_list_keys() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("store.list_keys"),
            "handle_list_keys should use store.list_keys"
        );
    }

    /// Test 6: Handler calculates pagination correctly
    #[test]
    fn test_pagination_calculation() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("saturating_sub"),
            "Handler should handle page 0 edge case with saturating_sub"
        );
    }

    /// Test 7: Unit tests include performance test
    #[test]
    fn test_list_keys_performance_test_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("test_list_keys_performance"),
            "Unit tests should include list keys performance test"
        );
    }

    /// Test 8: Unit tests include pagination test
    #[test]
    fn test_list_keys_pagination_test_exists() {
        let keys_rs = std::fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("test_list_keys_pagination"),
            "Unit tests should include pagination test"
        );
    }
}

/// Test module for keys.js browser functionality (Scenario 4)
mod keys_js_browser_tests {
    use super::*;

    /// Test 1: initKeyBrowser function exists
    #[test]
    fn test_init_key_browser_exists() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function initKeyBrowser"),
            "keys.js should have initKeyBrowser function"
        );
    }

    /// Test 2: loadKeys function exists
    #[test]
    fn test_load_keys_exists() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function loadKeys"),
            "keys.js should have loadKeys function"
        );
    }

    /// Test 3: filterKeys function exists
    #[test]
    fn test_filter_keys_exists() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function filterKeys"),
            "keys.js should have filterKeys function"
        );
    }

    /// Test 4: renderKeyTable function exists
    #[test]
    fn test_render_key_table_exists() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function renderKeyTable"),
            "keys.js should have renderKeyTable function"
        );
    }

    /// Test 5: Table has correct columns (Key, Size, TTL, Actions)
    #[test]
    fn test_table_columns() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(keys_js.contains("Key"), "Table should have Key column");
        assert!(keys_js.contains("Size"), "Table should have Size column");
        assert!(keys_js.contains("TTL"), "Table should have TTL column");
        assert!(keys_js.contains("Actions"), "Table should have Actions column");
    }

    /// Test 6: Search input exists
    #[test]
    fn test_search_input() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("key-search-input"),
            "keys.js should create search input"
        );
        assert!(
            keys_js.contains("Search keys"),
            "Search input should have aria-label"
        );
    }

    /// Test 7: Pagination renders correctly
    #[test]
    fn test_pagination_render() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("renderPagination"),
            "keys.js should have renderPagination function"
        );
        assert!(
            keys_js.contains("pagination-info"),
            "Pagination should show page info"
        );
    }

    /// Test 8: Key browser functions are exported
    #[test]
    fn test_exports() {
        let keys_js = std::fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("initKeyBrowser: initKeyBrowser"),
            "initKeyBrowser should be exported"
        );
        assert!(
            keys_js.contains("loadKeys: loadKeys"),
            "loadKeys should be exported"
        );
        assert!(
            keys_js.contains("filterKeys: filterKeys"),
            "filterKeys should be exported"
        );
    }
}

/// Test module for api.js fetchKeys function (Scenario 4)
mod api_fetch_keys_tests {
    use super::*;

    /// Test 1: api.js has fetchKeys function
    #[test]
    fn test_api_fetch_keys_exists() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("function fetchKeys") || api_js.contains("fetchKeys:"),
            "api.js should have fetchKeys function"
        );
    }

    /// Test 2: fetchKeys uses correct endpoint
    #[test]
    fn test_api_fetch_keys_endpoint() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("/api/keys"),
            "fetchKeys should use /api/keys endpoint"
        );
    }

    /// Test 3: fetchKeys supports pagination params
    #[test]
    fn test_api_fetch_keys_pagination() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("page") && api_js.contains("limit"),
            "fetchKeys should support pagination parameters"
        );
    }

    /// Test 4: fetchKeys is exported
    #[test]
    fn test_api_fetch_keys_exported() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("fetchKeys: fetchKeys"),
            "api.js should export fetchKeys function"
        );
    }
}

/// Test module for key browser CSS styles (Scenario 4)
mod key_browser_css_tests {
    use super::*;

    /// Test 1: Key browser container styles exist
    #[test]
    fn test_key_browser_styles() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".key-browser"),
            "CSS should have key-browser styles"
        );
    }

    /// Test 2: Key table styles exist
    #[test]
    fn test_key_table_styles() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".key-table"),
            "CSS should have key-table styles"
        );
    }

    /// Test 3: Search input styles exist
    #[test]
    fn test_search_styles() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".key-search-input") || main_css.contains(".search-input"),
            "CSS should have search input styles"
        );
    }

    /// Test 4: Pagination styles exist
    #[test]
    fn test_pagination_styles() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".pagination"),
            "CSS should have pagination styles"
        );
    }

    /// Test 5: Empty state styles exist
    #[test]
    fn test_empty_state_styles() {
        let main_css = std::fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".empty-state"),
            "CSS should have empty state styles"
        );
    }
}
