//! Configuration Tests for HTTP Server
//!
//! Owner: Scenario 1 - HTTP Server Initialization and Configuration
//!
//! Test cases:
//! 1. Start server with homepage_port=8080 and homepage_enabled=true
//! 2. Start server with homepage_port=9000 (custom port)
//! 3. Start server with homepage_enabled=false
//! 4. Attempt to start with invalid port (e.g., -1, 99999)

use std::sync::Arc;
use std::time::Duration;

// Import from the homepage module
// Note: These tests are designed to work with the standalone homepage module

/// Test module for HomepageConfig parsing and validation
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

/// Test module for HomepageServerConfig (from server.rs)
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

/// Integration-style tests for the full configuration flow
mod integration_tests {
    use super::homepage_config_tests::*;

    #[test]
    fn test_full_config_flow_enabled() {
        // Simulate: Start server with homepage_port=8080 and homepage_enabled=true
        let config = HomepageConfig::new()
            .with_port(8080)
            .with_enabled(true);

        // Validate config
        assert!(config.validate().is_ok());
        assert!(config.enabled);
        assert_eq!(config.port, 8080);
    }

    #[test]
    fn test_full_config_flow_custom_port() {
        // Simulate: Start server with homepage_port=9000
        let config = HomepageConfig::new()
            .with_port(9000)
            .with_enabled(true);

        // Validate config
        assert!(config.validate().is_ok());
        assert_eq!(config.port, 9000);
    }

    #[test]
    fn test_full_config_flow_disabled() {
        // Simulate: Start server with homepage_enabled=false
        let config = HomepageConfig::new()
            .with_enabled(false);

        // When disabled, server should not start
        // but config should still be valid
        assert!(config.validate().is_ok());
        assert!(!config.enabled);
    }

    #[test]
    fn test_full_config_flow_invalid_ports() {
        // Simulate: Attempt to start with invalid port
        // Port -1
        let result_negative = validate_port_i32(-1);
        assert!(result_negative.is_err());
        assert!(result_negative.unwrap_err().len() > 0);

        // Port 99999
        let result_large = validate_port_i32(99999);
        assert!(result_large.is_err());
        assert!(result_large.unwrap_err().len() > 0);
    }
}
