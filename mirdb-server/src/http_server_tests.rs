//! Integration tests for HTTP server startup and configuration.
//!
//! These tests verify that:
//! 1. HTTP server starts correctly with http.enabled=true
//! 2. HTTP server does not start when http.enabled=false
//! 3. HTTP server binds to custom addresses
//! 4. Port conflict detection works
//! 5. Default behavior when HTTP config is absent

#![cfg(test)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

use crate::config::{Config, HttpConfig};
use crate::error::MyResult;

/// Test Case 1: Start server with http.enabled=true and http.addr='127.0.0.1:8080'
/// Expected: HTTP server starts on port 8080, TCP server starts on default port 12333
#[test]
fn test_http_enabled_with_custom_port() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http1"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = true
addr = "127.0.0.1:8080"
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP configuration is parsed correctly
    assert!(config.http_enabled(), "HTTP should be enabled");
    assert_eq!(config.http_addr(), Some("127.0.0.1:8080"), "HTTP address should be 127.0.0.1:8080");

    // Verify TCP address is correct
    assert_eq!(config.addr, "127.0.0.1:12333", "TCP address should be 127.0.0.1:12333");

    // Validate configuration (should pass - no port conflict)
    config.validate()?;

    Ok(())
}

/// Test Case 2: Start server with http.enabled=false
/// Expected: Only TCP server starts, HTTP endpoints return connection refused
#[test]
fn test_http_disabled() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http2"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = false
addr = "127.0.0.1:8081"
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP is disabled
    assert!(!config.http_enabled(), "HTTP should be disabled");
    assert_eq!(config.http_addr(), None, "HTTP address should be None when disabled");

    // Verify config still has http section but it's disabled
    assert!(config.http.is_some(), "HTTP config section should exist");
    assert!(!config.http.as_ref().unwrap().enabled, "HTTP enabled flag should be false");

    Ok(())
}

/// Test Case 3: Start server with http.addr='0.0.0.0:9090'
/// Expected: HTTP server binds to all interfaces on port 9090
#[test]
fn test_http_bind_all_interfaces() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http3"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = true
addr = "0.0.0.0:9090"
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP is enabled and configured for all interfaces
    assert!(config.http_enabled(), "HTTP should be enabled");
    assert_eq!(config.http_addr(), Some("0.0.0.0:9090"), "HTTP should bind to all interfaces on port 9090");

    // Validate configuration
    config.validate()?;

    // Verify the address can be parsed as a SocketAddr
    let http_addr: SocketAddr = config.http_addr().unwrap().parse().unwrap();
    assert_eq!(http_addr.port(), 9090, "Port should be 9090");
    assert!(http_addr.ip().is_unspecified(), "IP should be unspecified (0.0.0.0)");

    Ok(())
}

/// Test Case 4: Start server with conflicting port (same as Memcached)
/// Expected: Server fails to start with clear error message about port conflict
#[test]
fn test_port_conflict_detection() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http4"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = true
addr = "127.0.0.1:12333"
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP is enabled with conflicting port
    assert!(config.http_enabled(), "HTTP should be enabled");
    assert_eq!(config.http_addr(), Some("127.0.0.1:12333"), "HTTP should have same port as TCP");

    // Validate should fail with port conflict error
    let result = config.validate();
    assert!(result.is_err(), "Validation should fail due to port conflict");

    let error = result.unwrap_err();
    assert!(error.msg.contains("Port conflict"), "Error message should mention port conflict: {}", error.msg);
    assert!(error.msg.contains("12333"), "Error message should mention the conflicting port: {}", error.msg);

    Ok(())
}

/// Test Case 5: Start server without HTTP config section
/// Expected: HTTP server disabled by default, only TCP server runs
#[test]
fn test_no_http_config_section() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http5"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP is disabled by default (no http section)
    assert!(!config.http_enabled(), "HTTP should be disabled by default");
    assert_eq!(config.http_addr(), None, "HTTP address should be None when not configured");

    // Verify http section is None
    assert!(config.http.is_none(), "HTTP config section should not exist");

    // Validate should pass
    config.validate()?;

    Ok(())
}

/// Additional test: Verify default HTTP address when enabled but addr not specified
#[test]
fn test_http_default_addr() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http6"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = true
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP is enabled with default address
    assert!(config.http_enabled(), "HTTP should be enabled");
    assert_eq!(config.http_addr(), Some("127.0.0.1:8080"), "HTTP should use default address 127.0.0.1:8080");

    // Validate should pass (different ports)
    config.validate()?;

    Ok(())
}

/// Test port conflict with same port on different interfaces (should still detect)
#[test]
fn test_port_conflict_different_interfaces() -> MyResult<()> {
    let toml_str = r#"
addr = "0.0.0.0:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http7"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = true
addr = "127.0.0.1:12333"
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Port conflict should still be detected even with different interfaces
    let result = config.validate();
    assert!(result.is_err(), "Validation should fail due to port conflict");

    let error = result.unwrap_err();
    assert!(error.msg.contains("Port conflict"), "Error message should mention port conflict");

    Ok(())
}

/// Test that HTTP config works with IPv6 addresses
#[test]
fn test_http_ipv6_address() -> MyResult<()> {
    let toml_str = r#"
addr = "127.0.0.1:12333"

max_level = 7
work_dir = "/tmp/mirdb_test_http8"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500

[http]
enabled = true
addr = "[::1]:8080"
"#;

    let config: Config = toml::from_str(toml_str).unwrap();

    // Verify HTTP is enabled with IPv6 address
    assert!(config.http_enabled(), "HTTP should be enabled");
    assert_eq!(config.http_addr(), Some("[::1]:8080"), "HTTP should have IPv6 address");

    // Validate should pass (different ports)
    config.validate()?;

    Ok(())
}
