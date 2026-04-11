//! Configuration API Handler
//! Owner: Scenario 8 - Configuration Display
//!
//! Expected exports:
//! - handle_config(options: &Options, port: u16) -> ConfigResponse
//! - handle_config_json(options: &Options, port: u16) -> String
//! - ConfigResponse struct with all configuration fields

use crate::options::Options;
use serde::{Deserialize, Serialize};

/// Configuration response containing all server configuration fields
///
/// This struct mirrors the Options struct but in a format suitable
/// for JSON serialization and display in the web UI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfigResponse {
    /// Working directory for database files
    pub work_dir: String,
    /// Server port number
    pub port: u16,
    /// Maximum LSM tree levels
    pub max_levels: usize,
    /// Maximum memtable size in bytes
    pub memtable_size: usize,
    /// Maximum memtable height (skip list)
    pub memtable_height: usize,
    /// Maximum number of immutable memtables
    pub imm_memtable_count: usize,
    /// Maximum SSTable file size in bytes
    pub sst_max_size: usize,
    /// L0 compaction trigger threshold
    pub l0_compaction_trigger: usize,
    /// Block size for SSTable blocks
    pub block_size: usize,
    /// Block restart interval
    pub block_restart_interval: usize,
    /// Thread sleep interval in milliseconds
    pub thread_sleep_ms: usize,
}

impl Default for ConfigResponse {
    fn default() -> Self {
        ConfigResponse {
            work_dir: "/tmp/mirdb".to_string(),
            port: 12333,
            max_levels: 7,
            memtable_size: 4 * 1024 * 1024,
            memtable_height: 32,
            imm_memtable_count: 16,
            sst_max_size: 100 * 1024 * 1024,
            l0_compaction_trigger: 4,
            block_size: 4 * 1024,
            block_restart_interval: 16,
            thread_sleep_ms: 500,
        }
    }
}

/// Generate configuration response from Options struct
///
/// # Arguments
/// * `options` - Reference to the server's Options configuration
/// * `port` - Server port number (extracted from address)
///
/// # Returns
/// ConfigResponse with all configuration values from Options
pub fn handle_config(options: &Options, port: u16) -> ConfigResponse {
    ConfigResponse {
        work_dir: options.work_dir.clone(),
        port,
        max_levels: options.max_level,
        memtable_size: options.mem_table_max_size,
        memtable_height: options.mem_table_max_height,
        imm_memtable_count: options.imm_mem_table_max_count,
        sst_max_size: options.sst_max_size,
        l0_compaction_trigger: options.l0_compaction_trigger,
        block_size: options.table_opt.block_size,
        block_restart_interval: options.table_opt.block_restart_interval,
        thread_sleep_ms: options.thread_sleep_ms,
    }
}

/// Generate configuration response as JSON string
///
/// # Arguments
/// * `options` - Reference to the server's Options configuration
/// * `port` - Server port number (extracted from address)
///
/// # Returns
/// JSON string representation of the configuration
pub fn handle_config_json(options: &Options, port: u16) -> String {
    let config = handle_config(options, port);
    serde_json::to_string(&config).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_options() -> Options {
        let mut opts = Options::default();
        opts.work_dir = "/test/mirdb".to_string();
        opts.max_level = 5;
        opts.mem_table_max_size = 8 * 1024 * 1024;
        opts.mem_table_max_height = 16;
        opts.imm_mem_table_max_count = 8;
        opts.sst_max_size = 50 * 1024 * 1024;
        opts.l0_compaction_trigger = 2;
        opts.thread_sleep_ms = 250;
        opts.table_opt.block_size = 8 * 1024;
        opts.table_opt.block_restart_interval = 8;
        opts
    }

    #[test]
    fn test_config_response_default() {
        let config = ConfigResponse::default();
        assert_eq!(config.work_dir, "/tmp/mirdb");
        assert_eq!(config.port, 12333);
        assert_eq!(config.max_levels, 7);
        assert_eq!(config.memtable_size, 4 * 1024 * 1024);
    }

    #[test]
    fn test_handle_config_returns_correct_values() {
        let options = create_test_options();
        let port = 8080;

        let config = handle_config(&options, port);

        assert_eq!(config.work_dir, "/test/mirdb");
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_levels, 5);
        assert_eq!(config.memtable_size, 8 * 1024 * 1024);
        assert_eq!(config.memtable_height, 16);
        assert_eq!(config.imm_memtable_count, 8);
        assert_eq!(config.sst_max_size, 50 * 1024 * 1024);
        assert_eq!(config.l0_compaction_trigger, 2);
        assert_eq!(config.block_size, 8 * 1024);
        assert_eq!(config.block_restart_interval, 8);
        assert_eq!(config.thread_sleep_ms, 250);
    }

    #[test]
    fn test_handle_config_json_valid_json() {
        let options = create_test_options();
        let port = 12333;

        let json = handle_config_json(&options, port);

        // Should be valid JSON
        let parsed: Result<ConfigResponse, _> = serde_json::from_str(&json);
        assert!(parsed.is_ok());

        // Should contain required fields
        assert!(json.contains("work_dir"));
        assert!(json.contains("port"));
        assert!(json.contains("max_levels"));
        assert!(json.contains("memtable_size"));
        assert!(json.contains("sst_max_size"));
        assert!(json.contains("l0_compaction_trigger"));
    }

    #[test]
    fn test_config_response_serialization() {
        let config = ConfigResponse {
            work_dir: "/var/lib/mirdb".to_string(),
            port: 9999,
            max_levels: 10,
            memtable_size: 16 * 1024 * 1024,
            memtable_height: 64,
            imm_memtable_count: 32,
            sst_max_size: 200 * 1024 * 1024,
            l0_compaction_trigger: 8,
            block_size: 16 * 1024,
            block_restart_interval: 32,
            thread_sleep_ms: 1000,
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"work_dir\":\"/var/lib/mirdb\""));
        assert!(json.contains("\"port\":9999"));
        assert!(json.contains("\"max_levels\":10"));
    }

    #[test]
    fn test_config_response_deserialization() {
        let json = r#"{
            "work_dir": "/data/mirdb",
            "port": 5555,
            "max_levels": 3,
            "memtable_size": 1048576,
            "memtable_height": 8,
            "imm_memtable_count": 4,
            "sst_max_size": 10485760,
            "l0_compaction_trigger": 2,
            "block_size": 2048,
            "block_restart_interval": 4,
            "thread_sleep_ms": 100
        }"#;

        let config: ConfigResponse = serde_json::from_str(json).unwrap();
        assert_eq!(config.work_dir, "/data/mirdb");
        assert_eq!(config.port, 5555);
        assert_eq!(config.max_levels, 3);
        assert_eq!(config.memtable_size, 1048576);
    }

    #[test]
    fn test_config_uses_default_options_values() {
        let options = Options::default();
        let port = 12333;

        let config = handle_config(&options, port);

        assert_eq!(config.work_dir, options.work_dir);
        assert_eq!(config.max_levels, options.max_level);
        assert_eq!(config.memtable_size, options.mem_table_max_size);
        assert_eq!(config.sst_max_size, options.sst_max_size);
    }
}
