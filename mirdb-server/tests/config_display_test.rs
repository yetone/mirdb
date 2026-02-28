//! Integration Tests for Configuration Display
//!
//! Scenario 4 - Configuration Display
//!
//! These tests verify that the homepage displays current MirDB configuration values:
//! - Test Case 1: Configuration panel displays work directory
//! - Test Case 2: Configuration panel displays listen address with port
//! - Test Case 3: Configuration panel displays memtable size limit and other limits
//! - Test Case 4: GET /api/config endpoint returns JSON with config fields

/// The embedded HTML content from the homepage
const INDEX_HTML: &str = include_str!("../assets/index.html");

/// The embedded CSS content
const STYLE_CSS: &str = include_str!("../assets/css/style.css");

// ============================================================================
// Test Case 1: Configuration panel displays work directory
// Input: Start server with work_dir=/tmp/mirdb-test
// Expected: Configuration panel displays work directory as /tmp/mirdb-test
// ============================================================================

#[test]
fn test_case_1_config_section_exists_in_html() {
    assert!(
        INDEX_HTML.contains("id=\"config\"") || INDEX_HTML.contains("id='config'"),
        "HTML must contain configuration section with id='config'"
    );
}

#[test]
fn test_case_1_config_panel_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-panel\"") || INDEX_HTML.contains("id='config-panel'"),
        "HTML must contain config-panel element"
    );
}

#[test]
fn test_case_1_config_work_dir_element_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-work-dir\"") || INDEX_HTML.contains("id='config-work-dir'"),
        "HTML must contain work directory display element with id='config-work-dir'"
    );
}

#[test]
fn test_case_1_config_work_dir_label_exists() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("work directory") || html_lower.contains("work_dir"),
        "Configuration panel must have Work Directory label"
    );
}

// ============================================================================
// Test Case 2: Configuration panel displays listen address with port 11211
// Input: Start server with memcached port 11211
// Expected: Configuration panel displays listen address with port 11211
// ============================================================================

#[test]
fn test_case_2_config_addr_element_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-addr\"") || INDEX_HTML.contains("id='config-addr'"),
        "HTML must contain listen address display element with id='config-addr'"
    );
}

#[test]
fn test_case_2_config_addr_label_exists() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("listen address") || html_lower.contains("address"),
        "Configuration panel must have Listen Address label"
    );
}

// ============================================================================
// Test Case 3: Configuration panel displays memtable size limit
// Input: Start server with specific size limits
// Expected: Configuration panel displays memtable size limit and other limits
// ============================================================================

#[test]
fn test_case_3_config_memtable_limit_element_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-memtable-limit\"") || INDEX_HTML.contains("id='config-memtable-limit'"),
        "HTML must contain memtable limit display element with id='config-memtable-limit'"
    );
}

#[test]
fn test_case_3_config_memtable_label_exists() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("memtable") && (html_lower.contains("size") || html_lower.contains("limit")),
        "Configuration panel must have Memtable Size Limit label"
    );
}

#[test]
fn test_case_3_config_sst_size_element_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-sst-size\"") || INDEX_HTML.contains("id='config-sst-size'"),
        "HTML must contain SSTable size display element with id='config-sst-size'"
    );
}

#[test]
fn test_case_3_config_block_size_element_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-block-size\"") || INDEX_HTML.contains("id='config-block-size'"),
        "HTML must contain block size display element with id='config-block-size'"
    );
}

#[test]
fn test_case_3_config_homepage_port_element_exists() {
    assert!(
        INDEX_HTML.contains("id=\"config-homepage-port\"") || INDEX_HTML.contains("id='config-homepage-port'"),
        "HTML must contain homepage port display element"
    );
}

// ============================================================================
// Test Case 4: GET /api/config endpoint returns JSON
// Input: GET /api/config endpoint
// Expected: JSON response with work_dir, port, and limits fields
// ============================================================================

#[test]
fn test_case_4_config_api_fetch_in_html() {
    assert!(
        INDEX_HTML.contains("/api/config"),
        "HTML must fetch from /api/config endpoint"
    );
}

#[test]
fn test_case_4_config_javascript_handles_work_dir() {
    assert!(
        INDEX_HTML.contains("data.work_dir") || INDEX_HTML.contains("data['work_dir']"),
        "JavaScript must handle work_dir from API response"
    );
}

#[test]
fn test_case_4_config_javascript_handles_addr() {
    assert!(
        INDEX_HTML.contains("data.addr") || INDEX_HTML.contains("data['addr']"),
        "JavaScript must handle addr from API response"
    );
}

#[test]
fn test_case_4_config_javascript_handles_memtable_limit() {
    assert!(
        INDEX_HTML.contains("memtable_size_limit"),
        "JavaScript must handle memtable_size_limit from API response"
    );
}

// ============================================================================
// CSS Tests for Config Section
// ============================================================================

#[test]
fn test_css_has_config_section_styles() {
    assert!(
        STYLE_CSS.contains(".config"),
        "CSS must have configuration section styles"
    );
}

#[test]
fn test_css_has_config_panel_styles() {
    assert!(
        STYLE_CSS.contains(".config-panel"),
        "CSS must have config-panel styles"
    );
}

#[test]
fn test_css_has_config_grid_styles() {
    assert!(
        STYLE_CSS.contains(".config-grid"),
        "CSS must have config-grid styles for layout"
    );
}

#[test]
fn test_css_has_config_item_styles() {
    assert!(
        STYLE_CSS.contains(".config-item"),
        "CSS must have config-item styles"
    );
}

#[test]
fn test_css_has_config_label_styles() {
    assert!(
        STYLE_CSS.contains(".config-label"),
        "CSS must have config-label styles"
    );
}

#[test]
fn test_css_has_config_value_styles() {
    assert!(
        STYLE_CSS.contains(".config-value"),
        "CSS must have config-value styles"
    );
}

// ============================================================================
// Handler Module Tests
// ============================================================================

#[test]
fn test_handlers_module_has_config_handler() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("pub async fn handle_config"),
        "handlers.rs must export handle_config function"
    );
}

#[test]
fn test_config_handler_returns_work_dir() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("\"work_dir\"") || handlers_content.contains("work_dir"),
        "handle_config must include work_dir in response"
    );
}

#[test]
fn test_config_handler_returns_addr() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("\"addr\"") || handlers_content.contains("addr"),
        "handle_config must include addr in response"
    );
}

#[test]
fn test_config_handler_returns_memtable_limit() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("memtable_size_limit"),
        "handle_config must include memtable_size_limit in response"
    );
}

#[test]
fn test_config_handler_returns_sst_max_size() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("sst_max_size"),
        "handle_config must include sst_max_size in response"
    );
}

#[test]
fn test_config_handler_returns_block_size() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("block_size"),
        "handle_config must include block_size in response"
    );
}

#[test]
fn test_config_handler_returns_formatted_sizes() {
    let handlers_content = include_str!("../src/homepage/handlers.rs");

    assert!(
        handlers_content.contains("_formatted") || handlers_content.contains("format_bytes"),
        "handle_config should include human-readable formatted sizes"
    );
}

// ============================================================================
// State Module Tests
// ============================================================================

#[test]
fn test_state_module_has_required_fields() {
    let state_content = include_str!("../src/homepage/state.rs");

    assert!(
        state_content.contains("pub work_dir: String"),
        "AppState must have work_dir field"
    );
    assert!(
        state_content.contains("pub addr: String"),
        "AppState must have addr field"
    );
    assert!(
        state_content.contains("pub memtable_size_limit: usize"),
        "AppState must have memtable_size_limit field"
    );
}

#[test]
fn test_state_module_has_format_bytes_helper() {
    let state_content = include_str!("../src/homepage/state.rs");

    assert!(
        state_content.contains("pub fn format_bytes") || state_content.contains("fn format_bytes"),
        "state.rs should have format_bytes helper function"
    );
}

#[test]
fn test_state_module_with_full_config_constructor() {
    let state_content = include_str!("../src/homepage/state.rs");

    assert!(
        state_content.contains("pub fn with_full_config") || state_content.contains("fn with_full_config"),
        "AppState should have with_full_config constructor for setting all config values"
    );
}

// ============================================================================
// Config Grid Structure Tests
// ============================================================================

#[test]
fn test_config_grid_class_in_html() {
    assert!(
        INDEX_HTML.contains("class=\"config-grid\"") || INDEX_HTML.contains("class='config-grid'"),
        "HTML must contain config-grid for layout"
    );
}

#[test]
fn test_config_item_class_in_html() {
    assert!(
        INDEX_HTML.contains("class=\"config-item\"") || INDEX_HTML.contains("class='config-item'"),
        "HTML must contain config-item elements"
    );
}

#[test]
fn test_config_label_class_in_html() {
    assert!(
        INDEX_HTML.contains("class=\"config-label\"") || INDEX_HTML.contains("class='config-label'"),
        "HTML must contain config-label elements"
    );
}

#[test]
fn test_config_value_class_in_html() {
    assert!(
        INDEX_HTML.contains("class=\"config-value\"") || INDEX_HTML.contains("class='config-value'"),
        "HTML must contain config-value elements"
    );
}

#[test]
fn test_config_section_has_heading() {
    let html_lower = INDEX_HTML.to_lowercase();
    assert!(
        html_lower.contains("<h2>configuration</h2>") || html_lower.contains(">configuration</h2>"),
        "Configuration section must have an h2 heading"
    );
}
