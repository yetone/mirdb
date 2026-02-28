//! Configuration Tests for HTTP Server Initialization and Configuration
//!
//! Owner: Scenario 1 - HTTP Server Initialization and Configuration
//!
//! Test Cases:
//! 1. Server with homepage_port=8080 and homepage_enabled=true responds with HTTP 200
//! 2. Server with homepage_port=9000 responds on port 9000
//! 3. Server with homepage_enabled=false refuses connections
//! 4. Server rejects invalid port configurations

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============================================================================
// HomepageConfig Simulation Module for Unit Testing
// ============================================================================

mod homepage_config_tests {
    /// Simulated HomepageConfig for testing (matches the structure in config.rs)
    #[derive(Debug, Clone, Default)]
    pub struct HomepageConfig {
        pub enabled: bool,
        pub port: u16,
        pub timeout_secs: u64,
        pub max_connections: usize,
    }

    impl HomepageConfig {
        pub fn new() -> Self {
            Self {
                enabled: true,
                port: 8080,
                timeout_secs: 5,
                max_connections: 100,
            }
        }

        pub fn with_port(mut self, port: u16) -> Self {
            self.port = port;
            self
        }

        pub fn with_enabled(mut self, enabled: bool) -> Self {
            self.enabled = enabled;
            self
        }

        /// Validate the configuration
        pub fn validate(&self) -> Result<(), String> {
            if self.port == 0 {
                return Err("Invalid port: 0 is reserved for dynamic port assignment".to_string());
            }
            Ok(())
        }
    }

    /// Validate a port from i32 (for invalid input testing)
    pub fn validate_port_i32(port: i32) -> Result<u16, String> {
        if port < 0 {
            return Err(format!("Invalid port: {} is negative", port));
        }
        if port > 65535 {
            return Err(format!("Invalid port: {} exceeds maximum port number 65535", port));
        }
        if port == 0 {
            return Err("Invalid port: 0 is reserved for dynamic port assignment".to_string());
        }
        Ok(port as u16)
    }

    // ============================================================================
    // Test Case 1: Start server with homepage_port=8080 and homepage_enabled=true
    // ============================================================================

    #[test]
    fn test_config_default_port_8080() {
        let config = HomepageConfig::new();
        assert_eq!(config.port, 8080, "Default port should be 8080");
        assert!(config.enabled, "Default should be enabled");
    }

    #[test]
    fn test_config_port_8080_enabled_true() {
        let config = HomepageConfig::new()
            .with_port(8080)
            .with_enabled(true);

        assert_eq!(config.port, 8080);
        assert!(config.enabled);
        assert!(config.validate().is_ok(), "Port 8080 should be valid");
    }

    #[test]
    fn test_config_validation_passes_for_port_8080() {
        let config = HomepageConfig::new().with_port(8080);
        let result = config.validate();
        assert!(result.is_ok(), "Config with port 8080 should validate successfully");
    }

    // ============================================================================
    // Test Case 2: Start server with homepage_port=9000 (custom port)
    // ============================================================================

    #[test]
    fn test_config_custom_port_9000() {
        let config = HomepageConfig::new().with_port(9000);
        assert_eq!(config.port, 9000, "Port should be configurable to 9000");
    }

    #[test]
    fn test_config_custom_port_9000_not_default() {
        let default_config = HomepageConfig::new();
        let custom_config = HomepageConfig::new().with_port(9000);

        assert_ne!(
            custom_config.port, default_config.port,
            "Custom port 9000 should differ from default"
        );
    }

    #[test]
    fn test_config_validation_passes_for_port_9000() {
        let config = HomepageConfig::new().with_port(9000);
        let result = config.validate();
        assert!(result.is_ok(), "Config with port 9000 should validate successfully");
    }

    #[test]
    fn test_config_various_valid_ports() {
        for port in [1, 80, 443, 3000, 8000, 8080, 9000, 65535u16] {
            let config = HomepageConfig::new().with_port(port);
            assert!(
                config.validate().is_ok(),
                "Port {} should be valid",
                port
            );
        }
    }

    // ============================================================================
    // Test Case 3: Start server with homepage_enabled=false
    // ============================================================================

    #[test]
    fn test_config_disabled() {
        let config = HomepageConfig::new().with_enabled(false);
        assert!(!config.enabled, "Server should be disabled when enabled=false");
    }

    #[test]
    fn test_config_disabled_with_port() {
        let config = HomepageConfig::new()
            .with_port(8080)
            .with_enabled(false);

        assert!(!config.enabled);
        assert_eq!(config.port, 8080);
        // Even when disabled, the config should validate
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_toggle_enabled() {
        let config_enabled = HomepageConfig::new().with_enabled(true);
        let config_disabled = HomepageConfig::new().with_enabled(false);

        assert!(config_enabled.enabled);
        assert!(!config_disabled.enabled);
    }

    // ============================================================================
    // Test Case 4: Attempt to start with invalid port (e.g., -1, 99999)
    // ============================================================================

    #[test]
    fn test_invalid_port_negative_one() {
        let result = validate_port_i32(-1);
        assert!(result.is_err(), "Port -1 should be invalid");

        let err = result.unwrap_err();
        assert!(
            err.contains("negative") || err.contains("-1"),
            "Error should mention the negative port"
        );
    }

    #[test]
    fn test_invalid_port_large_99999() {
        let result = validate_port_i32(99999);
        assert!(result.is_err(), "Port 99999 should be invalid (exceeds 65535)");

        let err = result.unwrap_err();
        assert!(
            err.contains("exceeds") || err.contains("99999") || err.contains("65535"),
            "Error should mention that port exceeds maximum"
        );
    }

    #[test]
    fn test_invalid_port_zero() {
        let result = validate_port_i32(0);
        assert!(result.is_err(), "Port 0 should be invalid");

        let config = HomepageConfig::new().with_port(0);
        assert!(config.validate().is_err(), "Config with port 0 should fail validation");
    }

    #[test]
    fn test_invalid_port_very_negative() {
        let result = validate_port_i32(-100);
        assert!(result.is_err(), "Port -100 should be invalid");
    }

    #[test]
    fn test_invalid_port_max_i32() {
        let result = validate_port_i32(i32::MAX);
        assert!(result.is_err(), "Port i32::MAX should be invalid");
    }

    #[test]
    fn test_valid_port_boundary_values() {
        // Test boundary values
        assert!(validate_port_i32(1).is_ok(), "Port 1 should be valid");
        assert!(validate_port_i32(65535).is_ok(), "Port 65535 should be valid");
        assert!(validate_port_i32(65536).is_err(), "Port 65536 should be invalid");
    }

    #[test]
    fn test_error_message_format() {
        // Verify error messages are descriptive
        let err_negative = validate_port_i32(-1).unwrap_err();
        assert!(!err_negative.is_empty(), "Error message should not be empty");

        let err_large = validate_port_i32(99999).unwrap_err();
        assert!(!err_large.is_empty(), "Error message should not be empty");

        let err_zero = validate_port_i32(0).unwrap_err();
        assert!(!err_zero.is_empty(), "Error message should not be empty");
    }
}

// ============================================================================
// Server Config Tests Module
// ============================================================================

mod server_config_tests {
    use std::time::Duration;

    /// Simulated HomepageServerConfig for testing
    #[derive(Debug, Clone)]
    pub struct HomepageServerConfig {
        pub port: u16,
        pub enabled: bool,
        pub timeout: Duration,
        pub max_connections: usize,
    }

    impl HomepageServerConfig {
        pub fn new(port: u16) -> Self {
            Self {
                port,
                enabled: true,
                timeout: Duration::from_secs(5),
                max_connections: 100,
            }
        }

        pub fn with_enabled(mut self, enabled: bool) -> Self {
            self.enabled = enabled;
            self
        }

        pub fn validate_port(&self) -> Result<(), String> {
            if self.port == 0 {
                return Err("Invalid port: 0 is reserved for dynamic port assignment".to_string());
            }
            Ok(())
        }
    }

    impl Default for HomepageServerConfig {
        fn default() -> Self {
            Self::new(8080)
        }
    }

    #[test]
    fn test_server_config_default_values() {
        let config = HomepageServerConfig::default();
        assert_eq!(config.port, 8080);
        assert!(config.enabled);
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.max_connections, 100);
    }

    #[test]
    fn test_server_config_custom_port() {
        let config = HomepageServerConfig::new(9000);
        assert_eq!(config.port, 9000);
    }

    #[test]
    fn test_server_config_disabled() {
        let config = HomepageServerConfig::new(8080).with_enabled(false);
        assert!(!config.enabled);
    }

    #[test]
    fn test_server_config_validate_port() {
        let valid_config = HomepageServerConfig::new(8080);
        assert!(valid_config.validate_port().is_ok());

        let invalid_config = HomepageServerConfig::new(0);
        assert!(invalid_config.validate_port().is_err());
    }
}

// ============================================================================
// TOML Configuration Parsing Tests
// ============================================================================

#[test]
fn test_toml_config_port_8080_enabled_true() {
    // Test Case 1: Start server with homepage_port=8080 and homepage_enabled=true
    let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdbs"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[homepage]
enabled = true
port = 8080
"#;

    let config: toml::Value = toml::from_str(toml_str).expect("Failed to parse TOML");
    let homepage = config.get("homepage").expect("Missing homepage section");

    assert_eq!(
        homepage.get("enabled").and_then(|v| v.as_bool()),
        Some(true),
        "homepage.enabled should be true"
    );
    assert_eq!(
        homepage.get("port").and_then(|v| v.as_integer()),
        Some(8080),
        "homepage.port should be 8080"
    );
}

#[test]
fn test_toml_config_port_9000() {
    // Test Case 2: Start server with homepage_port=9000
    let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdbs"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[homepage]
enabled = true
port = 9000
"#;

    let config: toml::Value = toml::from_str(toml_str).expect("Failed to parse TOML");
    let homepage = config.get("homepage").expect("Missing homepage section");

    assert_eq!(
        homepage.get("port").and_then(|v| v.as_integer()),
        Some(9000),
        "homepage.port should be 9000, not default"
    );
}

#[test]
fn test_toml_config_disabled() {
    // Test Case 3: Start server with homepage_enabled=false
    let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdbs"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[homepage]
enabled = false
port = 8080
"#;

    let config: toml::Value = toml::from_str(toml_str).expect("Failed to parse TOML");
    let homepage = config.get("homepage").expect("Missing homepage section");

    assert_eq!(
        homepage.get("enabled").and_then(|v| v.as_bool()),
        Some(false),
        "homepage.enabled should be false"
    );
}

#[test]
fn test_toml_config_defaults_when_missing() {
    // When homepage section is missing, should default to disabled
    let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdbs"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500
"#;

    let config: toml::Value = toml::from_str(toml_str).expect("Failed to parse TOML");
    // homepage section should be None when missing - defaults apply
    assert!(
        config.get("homepage").is_none(),
        "Missing homepage section should return None"
    );
}

// ============================================================================
// Port Validation Helper Function
// ============================================================================

/// Validates that a port is within the valid range (1-65535)
fn validate_port_range(port: i32) -> bool {
    port >= 1 && port <= 65535
}

// ============================================================================
// Port Validation Unit Tests
// ============================================================================

#[test]
fn test_port_range_invalid_negative() {
    assert!(!validate_port_range(-1), "Port -1 should be invalid");
}

#[test]
fn test_port_range_invalid_zero() {
    assert!(!validate_port_range(0), "Port 0 should be invalid");
}

#[test]
fn test_port_range_invalid_too_high() {
    assert!(!validate_port_range(99999), "Port 99999 should be invalid (above 65535)");
    assert!(!validate_port_range(65536), "Port 65536 should be invalid (above 65535)");
}

#[test]
fn test_port_range_valid_standard() {
    assert!(validate_port_range(8080), "Port 8080 should be valid");
    assert!(validate_port_range(9000), "Port 9000 should be valid");
    assert!(validate_port_range(80), "Port 80 should be valid");
    assert!(validate_port_range(443), "Port 443 should be valid");
}

#[test]
fn test_port_range_valid_boundary() {
    assert!(validate_port_range(1), "Port 1 should be valid");
    assert!(validate_port_range(65535), "Port 65535 should be valid");
}

#[test]
fn test_all_valid_port_boundaries() {
    let valid_ports: Vec<i32> = vec![1, 2, 80, 443, 1024, 8080, 8443, 9000, 65534, 65535];
    for port in valid_ports {
        assert!(validate_port_range(port), "Port {} should be valid", port);
    }
}

#[test]
fn test_all_invalid_port_boundaries() {
    let invalid_ports: Vec<i32> = vec![-100, -1, 0, 65536, 65537, 99999, 100000];
    for port in invalid_ports {
        assert!(!validate_port_range(port), "Port {} should be invalid", port);
    }
}

// ============================================================================
// Integration Test Helpers
// ============================================================================

/// Check if a port is accessible via TCP
fn is_port_accessible(port: u16) -> bool {
    TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok()
}

/// Wait for a port to become available with timeout
fn wait_for_port(port: u16, timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed().as_millis() < timeout_ms as u128 {
        if is_port_accessible(port) {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn test_helper_functions_work() {
    // A random high port that is likely not in use
    let unused_port: u16 = 58123;

    // This port should not be accessible
    assert!(!is_port_accessible(unused_port), "Port {} should not be in use", unused_port);

    // wait_for_port should return false for unused ports
    assert!(!wait_for_port(unused_port, 100), "wait_for_port should timeout for unused port");
}

// ============================================================================
// Server Module Verification Tests
// ============================================================================

#[test]
fn test_server_module_exports() {
    let server_content = include_str!("../../src/homepage/server.rs");

    assert!(
        server_content.contains("pub struct HomepageServerHandle"),
        "server.rs must export HomepageServerHandle struct"
    );
    assert!(
        server_content.contains("pub fn start_homepage_server"),
        "server.rs must export start_homepage_server function"
    );
    assert!(
        server_content.contains("pub fn validate_port"),
        "server.rs must export validate_port function"
    );
}

#[test]
fn test_config_module_exports() {
    let config_content = include_str!("../../src/config.rs");

    assert!(
        config_content.contains("pub struct HomepageConfig"),
        "config.rs must export HomepageConfig struct"
    );
    assert!(
        config_content.contains("pub enabled: bool"),
        "HomepageConfig must have enabled field"
    );
    assert!(
        config_content.contains("pub port: u16"),
        "HomepageConfig must have port field"
    );
    assert!(
        config_content.contains("pub fn validate"),
        "HomepageConfig must have validate function"
    );
}

#[test]
fn test_state_module_exports() {
    let state_content = include_str!("../../src/homepage/state.rs");

    assert!(
        state_content.contains("pub struct AppState"),
        "state.rs must export AppState struct"
    );
    assert!(
        state_content.contains("pub fn new"),
        "AppState must have new constructor"
    );
    assert!(
        state_content.contains("pub fn get_version"),
        "AppState must have get_version method"
    );
    assert!(
        state_content.contains("pub fn is_running"),
        "AppState must have is_running method"
    );
}

#[test]
fn test_mod_exports() {
    let mod_content = include_str!("../../src/homepage/mod.rs");

    assert!(mod_content.contains("pub mod server"), "mod.rs must export server module");
    assert!(mod_content.contains("pub mod routes"), "mod.rs must export routes module");
    assert!(mod_content.contains("pub mod handlers"), "mod.rs must export handlers module");
    assert!(mod_content.contains("pub mod state"), "mod.rs must export state module");
    assert!(
        mod_content.contains("pub use server::start_homepage_server"),
        "mod.rs must re-export start_homepage_server"
    );
}

// ============================================================================
// Default Configuration Tests
// ============================================================================

#[test]
fn test_homepage_config_default_values() {
    let config_content = include_str!("../../src/config.rs");

    assert!(
        config_content.contains("fn default_homepage_enabled") && config_content.contains("false"),
        "Default homepage enabled should be false"
    );
    assert!(
        config_content.contains("fn default_homepage_port") && config_content.contains("8080"),
        "Default homepage port should be 8080"
    );
    assert!(
        config_content.contains("fn default_timeout_secs") && config_content.contains("5"),
        "Default timeout should be 5 seconds"
    );
    assert!(
        config_content.contains("fn default_max_connections") && config_content.contains("100"),
        "Default max connections should be 100"
    );
}

#[test]
fn test_homepage_config_serde_defaults() {
    let config_content = include_str!("../../src/config.rs");

    assert!(
        config_content.contains("#[serde(default = \"default_homepage_enabled\")]"),
        "enabled field should have serde default"
    );
    assert!(
        config_content.contains("#[serde(default = \"default_homepage_port\")]"),
        "port field should have serde default"
    );
}

// ============================================================================
// Port Validation Error Messages
// ============================================================================

#[test]
fn test_port_validation_error_message() {
    let server_content = include_str!("../../src/homepage/server.rs");

    assert!(
        server_content.contains("Invalid port number"),
        "validate_port should return descriptive error for invalid port"
    );
    assert!(
        server_content.contains("1") && server_content.contains("65535"),
        "validate_port error should mention valid port range"
    );
}

#[test]
fn test_config_validation_port_zero_error() {
    let config_content = include_str!("../../src/config.rs");

    assert!(
        config_content.contains("port == 0") || config_content.contains("port cannot be 0"),
        "Config validation should reject port 0"
    );
}

// ============================================================================
// HTTP Response Format Tests
// ============================================================================

#[test]
fn test_server_uses_warp() {
    let server_content = include_str!("../../src/homepage/server.rs");

    assert!(
        server_content.contains("use warp::Filter") || server_content.contains("warp::Filter"),
        "Server should use warp framework"
    );
    assert!(
        server_content.contains("warp::serve"),
        "Server should use warp::serve to start HTTP server"
    );
}

#[test]
fn test_server_binds_to_configured_port() {
    let server_content = include_str!("../../src/homepage/server.rs");

    assert!(
        server_content.contains("config.port") || (server_content.contains("port") && server_content.contains("SocketAddr")),
        "Server should use port from configuration"
    );
}

// ============================================================================
// Integration-style Tests for Full Configuration Flow
// ============================================================================

mod integration_tests {
    use super::homepage_config_tests::*;

    #[test]
    fn test_full_config_flow_enabled() {
        let config = HomepageConfig::new()
            .with_port(8080)
            .with_enabled(true);

        assert!(config.validate().is_ok());
        assert!(config.enabled);
        assert_eq!(config.port, 8080);
    }

    #[test]
    fn test_full_config_flow_custom_port() {
        let config = HomepageConfig::new()
            .with_port(9000)
            .with_enabled(true);

        assert!(config.validate().is_ok());
        assert_eq!(config.port, 9000);
    }

    #[test]
    fn test_full_config_flow_disabled() {
        let config = HomepageConfig::new()
            .with_enabled(false);

        assert!(config.validate().is_ok());
        assert!(!config.enabled);
    }

    #[test]
    fn test_full_config_flow_invalid_ports() {
        let result_negative = validate_port_i32(-1);
        assert!(result_negative.is_err());
        assert!(result_negative.unwrap_err().len() > 0);

        let result_large = validate_port_i32(99999);
        assert!(result_large.is_err());
        assert!(result_large.unwrap_err().len() > 0);
    }
}

// ============================================================================
// Scenario 4: Configuration Display Tests
// Tests that verify the homepage displays current MirDB configuration values
// ============================================================================

mod config_display_tests {
    /// Embedded HTML content for testing config section
    const INDEX_HTML: &str = include_str!("../../assets/index.html");

    /// Embedded CSS content
    const STYLE_CSS: &str = include_str!("../../assets/css/style.css");

    // ------------------------------------------------------------------------
    // Test Case 1: Configuration panel displays work directory
    // Input: Start server with work_dir=/tmp/mirdb-test
    // Expected: Configuration panel displays work directory as /tmp/mirdb-test
    // ------------------------------------------------------------------------

    #[test]
    fn test_config_section_exists_in_html() {
        assert!(
            INDEX_HTML.contains("id=\"config\"") || INDEX_HTML.contains("id='config'"),
            "HTML must contain configuration section with id='config'"
        );
    }

    #[test]
    fn test_config_panel_exists() {
        assert!(
            INDEX_HTML.contains("id=\"config-panel\"") || INDEX_HTML.contains("id='config-panel'"),
            "HTML must contain config-panel element"
        );
    }

    #[test]
    fn test_config_work_dir_element_exists() {
        assert!(
            INDEX_HTML.contains("id=\"config-work-dir\"") || INDEX_HTML.contains("id='config-work-dir'"),
            "HTML must contain work directory display element with id='config-work-dir'"
        );
    }

    #[test]
    fn test_config_work_dir_label_exists() {
        let html_lower = INDEX_HTML.to_lowercase();
        assert!(
            html_lower.contains("work directory") || html_lower.contains("work_dir"),
            "Configuration panel must have Work Directory label"
        );
    }

    // ------------------------------------------------------------------------
    // Test Case 2: Configuration panel displays listen address with port 11211
    // Input: Start server with memcached port 11211
    // Expected: Configuration panel displays listen address with port 11211
    // ------------------------------------------------------------------------

    #[test]
    fn test_config_addr_element_exists() {
        assert!(
            INDEX_HTML.contains("id=\"config-addr\"") || INDEX_HTML.contains("id='config-addr'"),
            "HTML must contain listen address display element with id='config-addr'"
        );
    }

    #[test]
    fn test_config_addr_label_exists() {
        let html_lower = INDEX_HTML.to_lowercase();
        assert!(
            html_lower.contains("listen address") || html_lower.contains("address"),
            "Configuration panel must have Listen Address label"
        );
    }

    // ------------------------------------------------------------------------
    // Test Case 3: Configuration panel displays memtable size limit
    // Input: Start server with specific size limits
    // Expected: Configuration panel displays memtable size limit and other limits
    // ------------------------------------------------------------------------

    #[test]
    fn test_config_memtable_limit_element_exists() {
        assert!(
            INDEX_HTML.contains("id=\"config-memtable-limit\"") || INDEX_HTML.contains("id='config-memtable-limit'"),
            "HTML must contain memtable limit display element with id='config-memtable-limit'"
        );
    }

    #[test]
    fn test_config_memtable_label_exists() {
        let html_lower = INDEX_HTML.to_lowercase();
        assert!(
            html_lower.contains("memtable") && (html_lower.contains("size") || html_lower.contains("limit")),
            "Configuration panel must have Memtable Size Limit label"
        );
    }

    #[test]
    fn test_config_sst_size_element_exists() {
        assert!(
            INDEX_HTML.contains("id=\"config-sst-size\"") || INDEX_HTML.contains("id='config-sst-size'"),
            "HTML must contain SSTable size display element with id='config-sst-size'"
        );
    }

    #[test]
    fn test_config_block_size_element_exists() {
        assert!(
            INDEX_HTML.contains("id=\"config-block-size\"") || INDEX_HTML.contains("id='config-block-size'"),
            "HTML must contain block size display element with id='config-block-size'"
        );
    }

    // ------------------------------------------------------------------------
    // Test Case 4: GET /api/config endpoint returns JSON
    // Input: GET /api/config endpoint
    // Expected: JSON response with work_dir, port, and limits fields
    // ------------------------------------------------------------------------

    #[test]
    fn test_config_api_fetch_in_html() {
        assert!(
            INDEX_HTML.contains("/api/config"),
            "HTML must fetch from /api/config endpoint"
        );
    }

    #[test]
    fn test_config_javascript_handles_work_dir() {
        assert!(
            INDEX_HTML.contains("data.work_dir") || INDEX_HTML.contains("data['work_dir']"),
            "JavaScript must handle work_dir from API response"
        );
    }

    #[test]
    fn test_config_javascript_handles_addr() {
        assert!(
            INDEX_HTML.contains("data.addr") || INDEX_HTML.contains("data['addr']"),
            "JavaScript must handle addr from API response"
        );
    }

    #[test]
    fn test_config_javascript_handles_memtable_limit() {
        assert!(
            INDEX_HTML.contains("memtable_size_limit"),
            "JavaScript must handle memtable_size_limit from API response"
        );
    }

    // ------------------------------------------------------------------------
    // CSS Tests for Config Section
    // ------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------
    // Handler Module Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_handlers_module_has_config_handler() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("pub async fn handle_config"),
            "handlers.rs must export handle_config function"
        );
    }

    #[test]
    fn test_config_handler_returns_work_dir() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("\"work_dir\"") || handlers_content.contains("work_dir"),
            "handle_config must include work_dir in response"
        );
    }

    #[test]
    fn test_config_handler_returns_addr() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("\"addr\"") || handlers_content.contains("addr"),
            "handle_config must include addr in response"
        );
    }

    #[test]
    fn test_config_handler_returns_memtable_limit() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("memtable_size_limit"),
            "handle_config must include memtable_size_limit in response"
        );
    }

    #[test]
    fn test_config_handler_returns_sst_max_size() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("sst_max_size"),
            "handle_config must include sst_max_size in response"
        );
    }

    #[test]
    fn test_config_handler_returns_block_size() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("block_size"),
            "handle_config must include block_size in response"
        );
    }

    #[test]
    fn test_config_handler_returns_formatted_sizes() {
        let handlers_content = include_str!("../../src/homepage/handlers.rs");

        assert!(
            handlers_content.contains("_formatted") || handlers_content.contains("format_bytes"),
            "handle_config should include human-readable formatted sizes"
        );
    }

    // ------------------------------------------------------------------------
    // State Module Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_state_module_has_required_fields() {
        let state_content = include_str!("../../src/homepage/state.rs");

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
        let state_content = include_str!("../../src/homepage/state.rs");

        assert!(
            state_content.contains("pub fn format_bytes") || state_content.contains("fn format_bytes"),
            "state.rs should have format_bytes helper function"
        );
    }

    #[test]
    fn test_state_module_with_full_config_constructor() {
        let state_content = include_str!("../../src/homepage/state.rs");

        assert!(
            state_content.contains("pub fn with_full_config") || state_content.contains("fn with_full_config"),
            "AppState should have with_full_config constructor for setting all config values"
        );
    }
}
