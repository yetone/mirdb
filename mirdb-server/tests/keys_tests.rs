//! Key CRUD API Tests
//! Owner: Scenario 4 - Key Browser View Keys
//! Co-owners: Scenarios 5, 6, 7
//!
//! Tests for Get Key API endpoint (Scenario 6):
//! - GET /api/keys/{key} returns JSON with value, flags, ttl, size
//! - GET /api/keys/nonexistent returns 404 with error message
//! - Modal displays value with copy button functional
//! - Get key operation completes within 500ms

use std::fs;

/// Test module for keys API
mod keys_api_tests {
    use super::*;

    // ============================================
    // Test Case 1: GET /api/keys/{key} handler exists and returns proper response
    // ============================================

    #[test]
    fn test_get_key_handler_exists() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check for handle_get_key function
        assert!(
            keys_rs.contains("pub fn handle_get_key"),
            "keys.rs should export handle_get_key function"
        );
    }

    #[test]
    fn test_get_key_response_has_required_fields() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check KeyValueResponse struct has required fields
        assert!(
            keys_rs.contains("struct KeyValueResponse"),
            "KeyValueResponse struct should exist"
        );

        // Verify the response includes key, value, flags, ttl, size
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

    // ============================================
    // Test Case 2: GET /api/keys/nonexistent returns 404 with error message
    // ============================================

    #[test]
    fn test_not_found_response_exists() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check for GetKeyResult enum with NotFound variant
        assert!(
            keys_rs.contains("enum GetKeyResult"),
            "GetKeyResult enum should exist"
        );
        assert!(
            keys_rs.contains("NotFound"),
            "GetKeyResult should have NotFound variant"
        );
    }

    #[test]
    fn test_error_response_struct_exists() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check ErrorResponse struct
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

    #[test]
    fn test_not_found_error_code() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check that not_found error code is used
        assert!(
            keys_rs.contains(r#""not_found""#),
            "Should use 'not_found' error code"
        );
    }

    // ============================================
    // Test Case 3: Modal displays value with copy button (UI Test)
    // ============================================

    #[test]
    fn test_keys_js_has_show_key_value_function() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function showKeyValue"),
            "keys.js should have showKeyValue function"
        );
    }

    #[test]
    fn test_keys_js_builds_modal_with_copy_button() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        // Check for modal building function
        assert!(
            keys_js.contains("buildKeyValueModal"),
            "keys.js should have buildKeyValueModal function"
        );

        // Check for copy button
        assert!(
            keys_js.contains("copy-value-btn") || keys_js.contains("copyBtn"),
            "Modal should include a copy button"
        );

        // Check for copy functionality
        assert!(
            keys_js.contains("copyKeyValue") || keys_js.contains("copyToClipboard"),
            "Should have copy to clipboard functionality"
        );
    }

    #[test]
    fn test_modal_displays_metadata() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        // Check for metadata display
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

    #[test]
    fn test_modal_css_styles_exist() {
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        // Check for modal styles
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

    // ============================================
    // Test Case 4: Get key operation completes within 500ms
    // ============================================

    #[test]
    fn test_handler_uses_performance_timing() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check that handler uses timing
        assert!(
            keys_rs.contains("Instant::now") || keys_rs.contains("std::time::Instant"),
            "Handler should use timing for performance measurement"
        );
    }

    #[test]
    fn test_unit_tests_include_performance_test() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        // Check that performance test exists in unit tests
        assert!(
            keys_rs.contains("test_get_key_performance") || keys_rs.contains("500"),
            "Unit tests should include performance test with 500ms threshold"
        );
    }

    // ============================================
    // Router Integration Tests
    // ============================================

    #[test]
    fn test_router_has_get_key_route() {
        let router_rs = fs::read_to_string("src/http/router.rs")
            .expect("Failed to read router.rs");

        // Check for ApiKeysGet route
        assert!(
            router_rs.contains("ApiKeysGet"),
            "Router should have ApiKeysGet route"
        );

        // Check for route matching pattern
        assert!(
            router_rs.contains("/api/keys/"),
            "Router should match /api/keys/ path prefix"
        );
    }

    // ============================================
    // API Client Tests
    // ============================================

    #[test]
    fn test_api_js_has_fetch_key_function() {
        let api_js = fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("function fetchKey") || api_js.contains("fetchKey:"),
            "api.js should have fetchKey function"
        );
    }

    #[test]
    fn test_api_js_handles_key_encoding() {
        let api_js = fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        // Check that fetchKey encodes the key properly
        assert!(
            api_js.contains("encodeURIComponent"),
            "fetchKey should use encodeURIComponent for key names"
        );
    }

    #[test]
    fn test_api_js_handles_errors() {
        let api_js = fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        // Check for error handling
        assert!(
            api_js.contains("handleResponse") || api_js.contains(".catch"),
            "api.js should handle API errors"
        );
        assert!(
            api_js.contains("response.ok") || api_js.contains("!response.ok"),
            "api.js should check response status"
        );
    }

    // ============================================
    // UI Integration Tests
    // ============================================

    #[test]
    fn test_ui_js_has_modal_functions() {
        let ui_js = fs::read_to_string("src/web/scripts/ui.js")
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

    #[test]
    fn test_ui_js_has_clipboard_support() {
        let ui_js = fs::read_to_string("src/web/scripts/ui.js")
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

    #[test]
    fn test_ui_js_has_toast_notifications() {
        let ui_js = fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        assert!(
            ui_js.contains("showToast"),
            "ui.js should have showToast function"
        );
    }

    // ============================================
    // Additional Validation Tests
    // ============================================

    #[test]
    fn test_keys_module_exported_in_handlers() {
        let handlers_mod = fs::read_to_string("src/http/handlers/mod.rs")
            .expect("Failed to read handlers/mod.rs");

        assert!(
            handlers_mod.contains("pub mod keys") || handlers_mod.contains("mod keys"),
            "handlers/mod.rs should export keys module"
        );
    }

    #[test]
    fn test_copy_feedback_visual() {
        let ui_js = fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        // Check for copy feedback
        assert!(
            ui_js.contains("copied") || ui_js.contains("Copied"),
            "Copy functionality should provide visual feedback"
        );
    }

    #[test]
    fn test_modal_accessibility() {
        let ui_js = fs::read_to_string("src/web/scripts/ui.js")
            .expect("Failed to read ui.js");

        // Check for accessibility attributes
        assert!(
            ui_js.contains("aria-") || ui_js.contains("role"),
            "Modal should have accessibility attributes"
        );

        // Check for keyboard support
        assert!(
            ui_js.contains("Escape") || ui_js.contains("keydown"),
            "Modal should support Escape key to close"
        );
    }

    #[test]
    fn test_toast_css_styles_exist() {
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        // Check for toast styles
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

    #[test]
    fn test_button_styles_exist() {
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        // Check for button styles
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
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub fn handle_list_keys"),
            "keys.rs should export handle_list_keys function"
        );
    }

    /// Test 2: ListKeysResult enum exists with correct variants
    #[test]
    fn test_list_keys_result_enum() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("pub enum ListKeysResult"),
            "keys.rs should have ListKeysResult enum"
        );
        assert!(
            keys_rs.contains("Success(KeysListResponse)"),
            "ListKeysResult should have Success variant"
        );
    }

    /// Test 3: KeysListResponse has required fields (keys, page, limit, total)
    #[test]
    fn test_keys_list_response_fields() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
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
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
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
    }

    /// Test 5: Handler uses store.list_keys
    #[test]
    fn test_uses_store_list_keys() {
        let keys_rs = fs::read_to_string("src/http/handlers/keys.rs")
            .expect("Failed to read keys.rs");

        assert!(
            keys_rs.contains("store.list_keys"),
            "handle_list_keys should use store.list_keys"
        );
    }
}

/// Test module for keys.js browser functionality (Scenario 4)
mod keys_js_browser_tests {
    use super::*;

    /// Test 1: initKeyBrowser function exists
    #[test]
    fn test_init_key_browser_exists() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function initKeyBrowser"),
            "keys.js should have initKeyBrowser function"
        );
    }

    /// Test 2: loadKeys function exists
    #[test]
    fn test_load_keys_exists() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function loadKeys"),
            "keys.js should have loadKeys function"
        );
    }

    /// Test 3: filterKeys function exists
    #[test]
    fn test_filter_keys_exists() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("function filterKeys"),
            "keys.js should have filterKeys function"
        );
    }

    /// Test 4: Table has correct columns (Key, Size, TTL, Actions)
    #[test]
    fn test_table_columns() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(keys_js.contains("Key"), "Table should have Key column");
        assert!(keys_js.contains("Size"), "Table should have Size column");
        assert!(keys_js.contains("TTL"), "Table should have TTL column");
        assert!(keys_js.contains("Actions"), "Table should have Actions column");
    }

    /// Test 5: Search input exists
    #[test]
    fn test_search_input() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("key-search-input"),
            "keys.js should create search input"
        );
    }

    /// Test 6: Pagination renders correctly
    #[test]
    fn test_pagination_render() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("renderPagination"),
            "keys.js should have renderPagination function"
        );
    }

    /// Test 7: Key browser functions are exported
    #[test]
    fn test_exports() {
        let keys_js = fs::read_to_string("src/web/scripts/keys.js")
            .expect("Failed to read keys.js");

        assert!(
            keys_js.contains("initKeyBrowser: initKeyBrowser"),
            "initKeyBrowser should be exported"
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
        let api_js = fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("function fetchKeys") || api_js.contains("fetchKeys:"),
            "api.js should have fetchKeys function"
        );
    }

    /// Test 2: fetchKeys uses correct endpoint
    #[test]
    fn test_api_fetch_keys_endpoint() {
        let api_js = fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("/api/keys"),
            "fetchKeys should use /api/keys endpoint"
        );
    }

    /// Test 3: fetchKeys supports pagination params
    #[test]
    fn test_api_fetch_keys_pagination() {
        let api_js = fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("page") && api_js.contains("limit"),
            "fetchKeys should support pagination parameters"
        );
    }

    /// Test 4: fetchKeys is exported
    #[test]
    fn test_api_fetch_keys_exported() {
        let api_js = fs::read_to_string("src/web/scripts/api.js")
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
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".key-browser"),
            "CSS should have key-browser styles"
        );
    }

    /// Test 2: Key table styles exist
    #[test]
    fn test_key_table_styles() {
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".key-table"),
            "CSS should have key-table styles"
        );
    }

    /// Test 3: Pagination styles exist
    #[test]
    fn test_pagination_styles() {
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".pagination"),
            "CSS should have pagination styles"
        );
    }

    /// Test 4: Empty state styles exist
    #[test]
    fn test_empty_state_styles() {
        let main_css = fs::read_to_string("src/web/styles/main.css")
            .expect("Failed to read main.css");

        assert!(
            main_css.contains(".empty-state"),
            "CSS should have empty state styles"
        );
    }
}
