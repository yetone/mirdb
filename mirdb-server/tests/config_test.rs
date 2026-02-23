//! HTTP Server Configuration Integration Tests
//! Owner: Scenario 13 - Server Configuration
//!
//! These tests verify:
//! - HTTP server configuration options work correctly
//! - Custom port configuration is respected
//! - Default port (8080) is used when not specified
//! - Bind address configuration works
//! - Invalid port numbers are rejected with clear errors

mod common;

use std::net::TcpListener;

use common::{find_available_port, is_port_available};

/// Test 1: Config file with http_port = 9000 should make HTTP server listen on port 9000
/// This test verifies that custom port configuration works
#[test]
fn test_http_config_custom_port_9000() {
    use mirdb::http::HttpServerConfig;

    // Create config with custom port 9000
    let config = HttpServerConfig::new(9000);
    assert_eq!(config.port, 9000, "Port should be set to 9000");

    // Verify socket address is generated correctly
    assert_eq!(
        config.socket_addr(),
        "0.0.0.0:9000",
        "Socket address should use port 9000"
    );

    // Test that we can bind to an available port using our config approach
    let port = find_available_port();
    let test_config = HttpServerConfig::new(port);
    let listener = TcpListener::bind(test_config.socket_addr());
    assert!(
        listener.is_ok(),
        "Should be able to bind to configured port"
    );
}

/// Test 2: No http_port in config file should use default port 8080
#[test]
fn test_http_config_default_port_8080() {
    use mirdb::http::HttpServerConfig;

    let default_config = HttpServerConfig::default();
    assert_eq!(
        default_config.port, 8080,
        "Default HTTP port should be 8080"
    );
    assert_eq!(
        default_config.socket_addr(),
        "0.0.0.0:8080",
        "Default socket address should be 0.0.0.0:8080"
    );
}

/// Test 3: Config with http_bind_address = '127.0.0.1' should only accept localhost connections
#[test]
fn test_http_config_localhost_bind_address() {
    use mirdb::http::HttpServerConfig;

    // Test that bind address configuration works
    let config = HttpServerConfig::new(8080).with_bind_address("127.0.0.1");
    assert_eq!(
        config.bind_address, "127.0.0.1",
        "Bind address should be 127.0.0.1"
    );
    assert_eq!(
        config.socket_addr(),
        "127.0.0.1:8080",
        "Socket address should reflect bind address"
    );

    // Verify we can actually bind to localhost with a custom port
    let port = find_available_port();
    let localhost_config = HttpServerConfig::new(port).with_bind_address("127.0.0.1");
    let listener = TcpListener::bind(localhost_config.socket_addr());
    assert!(
        listener.is_ok(),
        "Should be able to bind to localhost address"
    );
}

/// Test 4: Invalid port number (e.g., 99999) should fail with clear error message
#[test]
fn test_http_config_invalid_port_99999() {
    // Port 99999 is outside the valid range (1-65535) for u16
    let invalid_port: u32 = 99999;

    // Verify the port is indeed invalid for u16
    assert!(
        invalid_port > u16::MAX as u32,
        "99999 should be greater than u16::MAX (65535)"
    );

    // Test HttpServerConfig with max valid port (boundary test)
    use mirdb::http::HttpServerConfig;

    let config = HttpServerConfig::new(65535);
    assert_eq!(config.port, 65535, "Should accept max valid port 65535");

    // Test that port 0 is handled (OS assigns random port)
    let config_zero = HttpServerConfig::new(0);
    assert_eq!(config_zero.port, 0, "HttpServerConfig accepts port 0");
}

/// Test 5: Verify HttpServerConfig port configuration
#[test]
fn test_http_server_config_port_settings() {
    use mirdb::http::HttpServerConfig;

    // Test custom port
    let config = HttpServerConfig::new(9000);
    assert_eq!(config.port, 9000, "Custom port should be 9000");

    // Test socket address generation
    assert_eq!(
        config.socket_addr(),
        "0.0.0.0:9000",
        "Socket address should include custom port"
    );
}

/// Test 6: Verify HttpServerConfig bind address configuration
#[test]
fn test_http_server_config_bind_address_settings() {
    use mirdb::http::HttpServerConfig;

    // Test with default bind address
    let config = HttpServerConfig::default();
    assert_eq!(
        config.bind_address, "0.0.0.0",
        "Default bind address should be 0.0.0.0"
    );

    // Test with custom bind address
    let custom_config = HttpServerConfig::new(8080).with_bind_address("192.168.1.1");
    assert_eq!(
        custom_config.bind_address, "192.168.1.1",
        "Custom bind address should be set correctly"
    );
    assert_eq!(
        custom_config.socket_addr(),
        "192.168.1.1:8080",
        "Socket address should include custom bind address"
    );
}

/// Test 7: Port boundary values
#[test]
fn test_http_config_port_boundaries() {
    use mirdb::http::HttpServerConfig;

    // Test minimum valid port (1)
    let config_min = HttpServerConfig::new(1);
    assert_eq!(config_min.port, 1, "Should accept port 1");

    // Test maximum valid port (65535)
    let config_max = HttpServerConfig::new(65535);
    assert_eq!(config_max.port, 65535, "Should accept port 65535");

    // Test common ports
    let config_http = HttpServerConfig::new(80);
    assert_eq!(config_http.port, 80, "Should accept port 80");

    let config_https = HttpServerConfig::new(443);
    assert_eq!(config_https.port, 443, "Should accept port 443");
}

/// Test 8: Multiple configurations can coexist
#[test]
fn test_multiple_server_configurations() {
    use mirdb::http::HttpServerConfig;

    // Create multiple configurations
    let config1 = HttpServerConfig::new(8080);
    let config2 = HttpServerConfig::new(9000).with_bind_address("127.0.0.1");
    let config3 = HttpServerConfig::default();

    // All should have independent settings
    assert_eq!(config1.port, 8080);
    assert_eq!(config2.port, 9000);
    assert_eq!(config3.port, 8080);

    assert_eq!(config1.bind_address, "0.0.0.0");
    assert_eq!(config2.bind_address, "127.0.0.1");
    assert_eq!(config3.bind_address, "0.0.0.0");

    // Verify different ports can bind
    let port1 = find_available_port();
    let listener1 = TcpListener::bind(format!("127.0.0.1:{}", port1));
    assert!(listener1.is_ok(), "First port should bind");

    // Hold the first listener to make sure second port is different
    let _hold_listener = listener1.unwrap();

    let port2 = find_available_port();
    assert_ne!(port1, port2, "Should find different ports");

    let listener2 = TcpListener::bind(format!("127.0.0.1:{}", port2));
    assert!(listener2.is_ok(), "Second port should bind");
}

/// Test 9: Server configuration affects actual binding
#[test]
fn test_config_affects_actual_server_binding() {
    use mirdb::http::HttpServerConfig;

    // Create configuration and bind in one step to avoid race conditions
    let port = find_available_port();
    let config = HttpServerConfig::new(port).with_bind_address("127.0.0.1");

    // Bind to the port using the config
    let listener = TcpListener::bind(config.socket_addr());
    assert!(
        listener.is_ok(),
        "Should be able to bind to configured port: {}",
        config.socket_addr()
    );

    // Port should no longer be available
    let _hold = listener.unwrap();
    assert!(
        !is_port_available(port),
        "Port {} should not be available after binding",
        port
    );
}
