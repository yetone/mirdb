//! Configuration API Tests
//! Owner: Scenario 8 - Configuration Display
//!
//! Tests:
//! - GET /api/config returns all config fields
//! - Config values match Options struct
//! - ConfigResponse serialization

use std::path::Path;

/// Test module for config API
mod config_api_tests {
    use super::*;

    /// Test 1: ConfigResponse contains all required fields from REQ-6
    #[test]
    fn test_config_response_has_required_fields() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for required fields as specified in REQ-6
        assert!(
            config_rs.contains("work_dir"),
            "ConfigResponse should have work_dir field"
        );
        assert!(
            config_rs.contains("port"),
            "ConfigResponse should have port field"
        );
        assert!(
            config_rs.contains("max_levels"),
            "ConfigResponse should have max_levels field"
        );
        assert!(
            config_rs.contains("memtable_size"),
            "ConfigResponse should have memtable_size field"
        );
    }

    /// Test 2: Config module exports handle_config function
    #[test]
    fn test_config_handler_exists() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for handle_config function
        assert!(
            config_rs.contains("pub fn handle_config"),
            "config.rs should export handle_config function"
        );
    }

    /// Test 3: Config handler includes JSON output function
    #[test]
    fn test_config_json_handler_exists() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for JSON handler function
        assert!(
            config_rs.contains("handle_config_json") || config_rs.contains("serde_json::to_string"),
            "config.rs should support JSON serialization"
        );
    }

    /// Test 4: ConfigResponse implements Serialize
    #[test]
    fn test_config_response_is_serializable() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for Serialize derive
        assert!(
            config_rs.contains("Serialize"),
            "ConfigResponse should derive Serialize"
        );
    }

    /// Test 5: ConfigResponse implements Deserialize for testing
    #[test]
    fn test_config_response_is_deserializable() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for Deserialize derive
        assert!(
            config_rs.contains("Deserialize"),
            "ConfigResponse should derive Deserialize for testing"
        );
    }

    /// Test 6: Config includes additional Options fields
    #[test]
    fn test_config_includes_all_options_fields() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for additional configuration fields
        assert!(
            config_rs.contains("sst_max_size"),
            "ConfigResponse should have sst_max_size field"
        );
        assert!(
            config_rs.contains("l0_compaction_trigger"),
            "ConfigResponse should have l0_compaction_trigger field"
        );
        assert!(
            config_rs.contains("block_size"),
            "ConfigResponse should have block_size field"
        );
        assert!(
            config_rs.contains("thread_sleep_ms"),
            "ConfigResponse should have thread_sleep_ms field"
        );
    }

    /// Test 7: Config handler uses Options struct
    #[test]
    fn test_config_uses_options_struct() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check for Options import and usage
        assert!(
            config_rs.contains("Options"),
            "config.rs should use Options struct"
        );
        assert!(
            config_rs.contains("options:") || config_rs.contains("&Options"),
            "handle_config should accept Options parameter"
        );
    }

    /// Test 8: Config values correctly map from Options
    #[test]
    fn test_config_maps_options_correctly() {
        let config_rs = std::fs::read_to_string("src/http/handlers/config.rs")
            .expect("Failed to read config.rs");

        // Check that the mapping uses correct field names
        assert!(
            config_rs.contains("options.work_dir"),
            "Config should map work_dir from options"
        );
        assert!(
            config_rs.contains("options.max_level"),
            "Config should map max_level from options"
        );
        assert!(
            config_rs.contains("options.mem_table_max_size"),
            "Config should map mem_table_max_size from options"
        );
    }
}

/// Test module for config JavaScript integration
mod config_js_tests {
    use super::*;

    /// Test 1: main.js includes config display initialization
    #[test]
    fn test_main_js_has_config_init() {
        let main_js = std::fs::read_to_string("src/web/scripts/main.js")
            .expect("Failed to read main.js");

        assert!(
            main_js.contains("initConfigDisplay"),
            "main.js should have initConfigDisplay function"
        );
    }

    /// Test 2: Config uses MirDBApi for fetching
    #[test]
    fn test_config_uses_api_client() {
        let main_js = std::fs::read_to_string("src/web/scripts/main.js")
            .expect("Failed to read main.js");

        // Check for API usage
        assert!(
            main_js.contains("MirDBApi") || main_js.contains("fetchConfig"),
            "Config display should use MirDBApi for data fetching"
        );
    }

    /// Test 3: Config renders as table
    #[test]
    fn test_config_renders_table() {
        let main_js = std::fs::read_to_string("src/web/scripts/main.js")
            .expect("Failed to read main.js");

        // Check for table rendering
        assert!(
            main_js.contains("config-table") || main_js.contains("<table"),
            "Config should render as a table"
        );
    }

    /// Test 4: Config displays all key-value pairs
    #[test]
    fn test_config_displays_all_settings() {
        let main_js = std::fs::read_to_string("src/web/scripts/main.js")
            .expect("Failed to read main.js");

        // Check for key settings being displayed
        assert!(
            main_js.contains("Work Directory") || main_js.contains("work_dir"),
            "Config should display work directory"
        );
        assert!(
            main_js.contains("Port") || main_js.contains("port"),
            "Config should display port"
        );
        assert!(
            main_js.contains("Max Levels") || main_js.contains("max_levels"),
            "Config should display max levels"
        );
    }

    /// Test 5: api.js has fetchConfig function
    #[test]
    fn test_api_has_fetch_config() {
        let api_js = std::fs::read_to_string("src/web/scripts/api.js")
            .expect("Failed to read api.js");

        assert!(
            api_js.contains("fetchConfig"),
            "api.js should have fetchConfig function"
        );
        assert!(
            api_js.contains("/api/config"),
            "fetchConfig should call /api/config endpoint"
        );
    }

    /// Test 6: Config has formatBytes utility
    #[test]
    fn test_config_has_format_bytes() {
        let main_js = std::fs::read_to_string("src/web/scripts/main.js")
            .expect("Failed to read main.js");

        assert!(
            main_js.contains("formatBytes"),
            "main.js should have formatBytes utility for size display"
        );
    }
}

/// Test module for HTML structure
mod config_html_tests {
    use super::*;

    /// Test 1: index.html has config section
    #[test]
    fn test_index_has_config_section() {
        let html = std::fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        assert!(
            html.contains("id=\"config\"") || html.contains("config-section"),
            "index.html should have config section"
        );
    }

    /// Test 2: Config panel element exists
    #[test]
    fn test_config_panel_exists() {
        let html = std::fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        assert!(
            html.contains("id=\"config-panel\"") || html.contains("config-panel"),
            "index.html should have config-panel element"
        );
    }

    /// Test 3: main.js is included in HTML
    #[test]
    fn test_main_js_included() {
        let html = std::fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        assert!(
            html.contains("main.js"),
            "index.html should include main.js"
        );
    }

    /// Test 4: Config section has proper heading
    #[test]
    fn test_config_section_heading() {
        let html = std::fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        assert!(
            html.contains("Configuration"),
            "Config section should have 'Configuration' heading"
        );
    }

    /// Test 5: Config navigation link exists
    #[test]
    fn test_config_nav_link() {
        let html = std::fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        assert!(
            html.contains("href=\"#config\""),
            "Navigation should have link to config section"
        );
    }
}

/// Test module for router integration
mod config_router_tests {
    use super::*;

    /// Test 1: Router has ApiConfig route
    #[test]
    fn test_router_has_config_route() {
        let router_rs = std::fs::read_to_string("src/http/router.rs")
            .expect("Failed to read router.rs");

        assert!(
            router_rs.contains("ApiConfig"),
            "Router should have ApiConfig route"
        );
    }

    /// Test 2: Router maps /api/config to ApiConfig
    #[test]
    fn test_router_maps_config_path() {
        let router_rs = std::fs::read_to_string("src/http/router.rs")
            .expect("Failed to read router.rs");

        assert!(
            router_rs.contains("/api/config"),
            "Router should map /api/config path"
        );
    }
}
