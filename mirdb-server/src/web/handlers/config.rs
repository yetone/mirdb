//! Configuration API handler.
//! Owner: Scenario 4 - Configuration API Endpoint
//!
//! This module provides the /api/config endpoint handler that returns
//! current server configuration values. The endpoint is read-only;
//! modification attempts return HTTP 405 Method Not Allowed.
//!
//! Response fields:
//! - listen_addr: Server listen address
//! - max_lsm_levels: Maximum LSM tree levels
//! - work_dir: Working directory path
//! - sstable_max_size: Maximum SSTable size in bytes
//! - memtable_max_size: Maximum memtable size in bytes
//! - block_size: Block size in bytes
//!
//! Note: Read-only endpoint, no modification allowed

use std::sync::Arc;

use serde_json;

use crate::web::types::ConfigResponse;

/// Configuration state that holds the server's configuration values.
/// This state is read-only and initialized at server startup.
pub struct ConfigState {
    /// Server listen address (e.g., "0.0.0.0:12333")
    pub listen_addr: String,
    /// Maximum LSM tree levels (typically 7)
    pub max_lsm_levels: usize,
    /// Working directory for data files
    pub work_dir: String,
    /// Maximum SSTable file size in bytes
    pub sstable_max_size: usize,
    /// Maximum memtable size in bytes
    pub memtable_max_size: usize,
    /// Block size in bytes
    pub block_size: usize,
}

impl ConfigState {
    /// Create a new configuration state with the provided values
    pub fn new(
        listen_addr: String,
        max_lsm_levels: usize,
        work_dir: String,
        sstable_max_size: usize,
        memtable_max_size: usize,
        block_size: usize,
    ) -> Self {
        Self {
            listen_addr,
            max_lsm_levels,
            work_dir,
            sstable_max_size,
            memtable_max_size,
            block_size,
        }
    }

    /// Create a configuration state with default values
    pub fn default_config() -> Self {
        Self {
            listen_addr: "0.0.0.0:12333".to_string(),
            max_lsm_levels: 7,
            work_dir: "/tmp/mirdb".to_string(),
            sstable_max_size: 100 * 1024 * 1024, // 100MB
            memtable_max_size: 4 * 1024 * 1024,  // 4MB
            block_size: 4 * 1024,                 // 4KB
        }
    }

    /// Build a ConfigResponse from the current state
    pub fn get_config(&self) -> ConfigResponse {
        ConfigResponse {
            listen_addr: self.listen_addr.clone(),
            max_lsm_levels: self.max_lsm_levels,
            work_dir: self.work_dir.clone(),
            sstable_max_size: self.sstable_max_size,
            memtable_max_size: self.memtable_max_size,
            block_size: self.block_size,
        }
    }
}

/// Thread-safe shared configuration state
pub type SharedConfigState = Arc<ConfigState>;

/// Create a new shared configuration state with provided values
pub fn create_config_state(
    listen_addr: String,
    max_lsm_levels: usize,
    work_dir: String,
    sstable_max_size: usize,
    memtable_max_size: usize,
    block_size: usize,
) -> SharedConfigState {
    Arc::new(ConfigState::new(
        listen_addr,
        max_lsm_levels,
        work_dir,
        sstable_max_size,
        memtable_max_size,
        block_size,
    ))
}

/// Create a shared configuration state with default values
pub fn create_default_config_state() -> SharedConfigState {
    Arc::new(ConfigState::default_config())
}

/// Get configuration as JSON string
pub fn get_config_json(state: &ConfigState) -> String {
    let config = state.get_config();
    serde_json::to_string(&config).unwrap_or_else(|_| {
        r#"{"error": "Failed to serialize configuration"}"#.to_string()
    })
}

/// Check if a method is allowed for the config endpoint.
/// Only GET is allowed; returns false for POST, PUT, DELETE, etc.
pub fn is_method_allowed(method: &str) -> bool {
    method.eq_ignore_ascii_case("GET")
}

/// Error message for method not allowed
pub fn method_not_allowed_error() -> String {
    serde_json::to_string(&crate::web::types::ApiError::new(
        "Method Not Allowed. Configuration is read-only.",
        405,
    ))
    .unwrap_or_else(|_| r#"{"error": "Method Not Allowed", "code": 405}"#.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_state_creation() {
        let state = ConfigState::new(
            "0.0.0.0:12333".to_string(),
            7,
            "/tmp/mirdb".to_string(),
            100 * 1024 * 1024,
            4 * 1024 * 1024,
            4 * 1024,
        );
        assert_eq!(state.listen_addr, "0.0.0.0:12333");
        assert_eq!(state.max_lsm_levels, 7);
        assert_eq!(state.work_dir, "/tmp/mirdb");
        assert_eq!(state.sstable_max_size, 100 * 1024 * 1024);
        assert_eq!(state.memtable_max_size, 4 * 1024 * 1024);
        assert_eq!(state.block_size, 4 * 1024);
    }

    #[test]
    fn test_default_config() {
        let state = ConfigState::default_config();
        assert_eq!(state.listen_addr, "0.0.0.0:12333");
        assert_eq!(state.max_lsm_levels, 7);
        assert_eq!(state.work_dir, "/tmp/mirdb");
        assert_eq!(state.sstable_max_size, 100 * 1024 * 1024);
        assert_eq!(state.memtable_max_size, 4 * 1024 * 1024);
        assert_eq!(state.block_size, 4 * 1024);
    }

    #[test]
    fn test_get_config_response() {
        let state = ConfigState::new(
            "127.0.0.1:8080".to_string(),
            5,
            "/data/mirdb".to_string(),
            50 * 1024 * 1024,
            2 * 1024 * 1024,
            8 * 1024,
        );

        let config = state.get_config();
        assert_eq!(config.listen_addr, "127.0.0.1:8080");
        assert_eq!(config.max_lsm_levels, 5);
        assert_eq!(config.work_dir, "/data/mirdb");
        assert_eq!(config.sstable_max_size, 50 * 1024 * 1024);
        assert_eq!(config.memtable_max_size, 2 * 1024 * 1024);
        assert_eq!(config.block_size, 8 * 1024);
    }

    #[test]
    fn test_config_json_serialization() {
        let state = ConfigState::default_config();
        let json = get_config_json(&state);

        // Verify JSON can be parsed
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify key values
        assert_eq!(parsed["listen_addr"], "0.0.0.0:12333");
        assert_eq!(parsed["max_lsm_levels"], 7);
        assert_eq!(parsed["work_dir"], "/tmp/mirdb");
        assert_eq!(parsed["sstable_max_size"], 100 * 1024 * 1024);
        assert_eq!(parsed["memtable_max_size"], 4 * 1024 * 1024);
        assert_eq!(parsed["block_size"], 4 * 1024);
    }

    #[test]
    fn test_shared_config_state() {
        let state = create_default_config_state();
        let state_clone = state.clone();

        assert_eq!(state.listen_addr, state_clone.listen_addr);
        assert_eq!(state.max_lsm_levels, state_clone.max_lsm_levels);
    }

    #[test]
    fn test_custom_config_state() {
        let state = create_config_state(
            "192.168.1.1:9000".to_string(),
            10,
            "/custom/path".to_string(),
            200 * 1024 * 1024,
            8 * 1024 * 1024,
            16 * 1024,
        );

        assert_eq!(state.listen_addr, "192.168.1.1:9000");
        assert_eq!(state.max_lsm_levels, 10);
        assert_eq!(state.work_dir, "/custom/path");
    }

    #[test]
    fn test_method_allowed_get() {
        assert!(is_method_allowed("GET"));
        assert!(is_method_allowed("get"));
        assert!(is_method_allowed("Get"));
    }

    #[test]
    fn test_method_not_allowed_post() {
        assert!(!is_method_allowed("POST"));
        assert!(!is_method_allowed("post"));
    }

    #[test]
    fn test_method_not_allowed_put() {
        assert!(!is_method_allowed("PUT"));
    }

    #[test]
    fn test_method_not_allowed_delete() {
        assert!(!is_method_allowed("DELETE"));
    }

    #[test]
    fn test_method_not_allowed_error_response() {
        let error_json = method_not_allowed_error();
        let parsed: serde_json::Value = serde_json::from_str(&error_json).unwrap();

        assert_eq!(parsed["code"], 405);
        assert!(!parsed["success"].as_bool().unwrap_or(true));
        assert!(parsed["error"].as_str().unwrap().contains("Method Not Allowed"));
    }
}
