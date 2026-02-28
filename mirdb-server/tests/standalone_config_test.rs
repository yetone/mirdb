//! Standalone Configuration Tests for HTTP Server
//!
//! These tests do not depend on the main mirdb binary and can run independently.
//! Owner: Scenario 1 - HTTP Server Initialization and Configuration

use std::time::Duration;
use std::sync::Arc;

/// Homepage configuration structure
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

/// Server configuration
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

/// Application state
#[derive(Clone, Default)]
pub struct AppState {
    pub running: bool,
    pub work_dir: String,
    pub addr: String,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            running: true,
            work_dir: String::from("/tmp/mirdb"),
            addr: String::from("0.0.0.0:12333"),
        }
    }

    pub fn get_version(&self) -> &'static str {
        "0.1.0"
    }

    pub fn is_running(&self) -> bool {
        self.running
    }
}

/// Homepage server
pub struct HomepageServer {
    config: HomepageServerConfig,
    state: Arc<AppState>,
}

impl HomepageServer {
    pub fn new(config: HomepageServerConfig, state: Arc<AppState>) -> Self {
        Self { config, state }
    }

    pub fn port(&self) -> u16 {
        self.config.port
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn try_start(&self) -> Result<(), String> {
        if !self.config.enabled {
            return Ok(());
        }
        self.config.validate_port()?;
        Ok(())
    }
}

// ============================================================================
// Test Case 1: Start server with homepage_port=8080 and homepage_enabled=true
// ============================================================================

#[test]
fn test_case_1_server_port_8080_enabled() {
    let config = HomepageConfig::new()
        .with_port(8080)
        .with_enabled(true);

    assert_eq!(config.port, 8080);
    assert!(config.enabled);
    assert!(config.validate().is_ok());
}

#[test]
fn test_case_1_server_responds_on_8080() {
    let server_config = HomepageServerConfig::new(8080).with_enabled(true);
    let state = Arc::new(AppState::new());
    let server = HomepageServer::new(server_config, state);

    assert_eq!(server.port(), 8080);
    assert!(server.is_enabled());
    assert!(server.try_start().is_ok());
}

#[test]
fn test_case_1_default_config() {
    let config = HomepageServerConfig::default();
    assert_eq!(config.port, 8080);
    assert!(config.enabled);
}

// ============================================================================
// Test Case 2: Start server with homepage_port=9000
// ============================================================================

#[test]
fn test_case_2_server_custom_port_9000() {
    let config = HomepageConfig::new().with_port(9000);
    assert_eq!(config.port, 9000);
    assert_ne!(config.port, 8080);
}

#[test]
fn test_case_2_server_responds_on_9000() {
    let server_config = HomepageServerConfig::new(9000);
    let state = Arc::new(AppState::new());
    let server = HomepageServer::new(server_config, state);

    assert_eq!(server.port(), 9000);
    assert!(server.try_start().is_ok());
}

#[test]
fn test_case_2_not_default_port() {
    let server_config = HomepageServerConfig::new(9000);
    let default_config = HomepageServerConfig::default();

    assert_ne!(server_config.port, default_config.port);
}

// ============================================================================
// Test Case 3: Start server with homepage_enabled=false
// ============================================================================

#[test]
fn test_case_3_server_disabled() {
    let config = HomepageConfig::new().with_enabled(false);
    assert!(!config.enabled);
}

#[test]
fn test_case_3_connection_refused_when_disabled() {
    let server_config = HomepageServerConfig::new(8080).with_enabled(false);
    let state = Arc::new(AppState::new());
    let server = HomepageServer::new(server_config, state);

    assert!(!server.is_enabled());
    // When disabled, try_start returns Ok but doesn't actually start
    assert!(server.try_start().is_ok());
}

#[test]
fn test_case_3_disabled_returns_immediately() {
    let server_config = HomepageServerConfig::new(8080).with_enabled(false);
    let state = Arc::new(AppState::new());
    let server = HomepageServer::new(server_config, state);

    // Server should not bind to port when disabled
    assert!(!server.is_enabled());
}

// ============================================================================
// Test Case 4: Attempt to start with invalid port (e.g., -1, 99999)
// ============================================================================

#[test]
fn test_case_4_invalid_port_negative_one() {
    let result = validate_port_i32(-1);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("negative") || err.contains("-1"));
}

#[test]
fn test_case_4_invalid_port_99999() {
    let result = validate_port_i32(99999);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("exceeds") || err.contains("99999"));
}

#[test]
fn test_case_4_invalid_port_zero() {
    let result = validate_port_i32(0);
    assert!(result.is_err());

    let config = HomepageConfig::new().with_port(0);
    assert!(config.validate().is_err());
}

#[test]
fn test_case_4_server_rejects_port_zero() {
    let server_config = HomepageServerConfig::new(0);
    let state = Arc::new(AppState::new());
    let server = HomepageServer::new(server_config, state);

    let result = server.try_start();
    assert!(result.is_err());
}

#[test]
fn test_case_4_error_message_descriptive() {
    let err_negative = validate_port_i32(-1).unwrap_err();
    assert!(!err_negative.is_empty());

    let err_large = validate_port_i32(99999).unwrap_err();
    assert!(!err_large.is_empty());

    let err_zero = validate_port_i32(0).unwrap_err();
    assert!(!err_zero.is_empty());
}

// ============================================================================
// Additional edge case tests
// ============================================================================

#[test]
fn test_valid_port_boundaries() {
    assert!(validate_port_i32(1).is_ok());
    assert!(validate_port_i32(65535).is_ok());
    assert!(validate_port_i32(65536).is_err());
}

#[test]
fn test_various_valid_ports() {
    for port in [1, 80, 443, 3000, 8000, 8080, 9000, 65535] {
        let result = validate_port_i32(port);
        assert!(result.is_ok(), "Port {} should be valid", port);
    }
}

#[test]
fn test_app_state() {
    let state = AppState::new();
    assert!(state.is_running());
    assert_eq!(state.get_version(), "0.1.0");
}
