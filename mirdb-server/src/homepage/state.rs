//! Shared State for HTTP Handlers
//!
//! Contains read-only access to MirDB state for the homepage.
//!
//! Owner: Scenario 1 - HTTP Server Initialization (creates this file)
//! Used by: All scenarios that need MirDB state access
//!
//! Expected exports:
//! - pub struct AppState
//! - impl AppState { pub fn new(mirdb_state: Arc<RwLock<MirDBState>>) -> Self }
//! - impl AppState { pub fn get_config(&self) -> Config }
//! - impl AppState { pub fn get_version(&self) -> &str }
//! - impl AppState { pub fn is_running(&self) -> bool }

use std::sync::Arc;

use crate::config::HomepageConfig;

/// Version of MirDB
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared application state for the homepage HTTP handlers
#[derive(Clone)]
pub struct AppState {
    /// Homepage configuration
    pub config: HomepageConfig,
    /// Server running status
    pub running: bool,
    /// Work directory path
    pub work_dir: String,
    /// Listen address (memcached port)
    pub addr: String,
    /// Memtable size limit in bytes
    pub memtable_size_limit: usize,
    /// SSTable max size in bytes
    pub sst_max_size: usize,
    /// Block size in bytes
    pub block_size: usize,
}

impl AppState {
    /// Create a new AppState with default values
    pub fn new(config: HomepageConfig) -> Self {
        Self {
            config,
            running: true,
            work_dir: String::from("/tmp/mirdb"),
            addr: String::from("0.0.0.0:12333"),
            memtable_size_limit: 4 * 1024 * 1024, // 4MB default
            sst_max_size: 100 * 1024 * 1024,      // 100MB default
            block_size: 4 * 1024,                  // 4KB default
        }
    }

    /// Create AppState with custom configuration
    pub fn with_config(config: HomepageConfig, work_dir: String, addr: String) -> Self {
        Self {
            config,
            running: true,
            work_dir,
            addr,
            memtable_size_limit: 4 * 1024 * 1024,
            sst_max_size: 100 * 1024 * 1024,
            block_size: 4 * 1024,
        }
    }

    /// Create AppState with full configuration including size limits
    pub fn with_full_config(
        config: HomepageConfig,
        work_dir: String,
        addr: String,
        memtable_size_limit: usize,
        sst_max_size: usize,
        block_size: usize,
    ) -> Self {
        Self {
            config,
            running: true,
            work_dir,
            addr,
            memtable_size_limit,
            sst_max_size,
            block_size,
        }
    }

    /// Get the MirDB version
    pub fn get_version(&self) -> &'static str {
        VERSION
    }

    /// Check if server is running
    pub fn is_running(&self) -> bool {
        self.running
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new(HomepageConfig::default())
    }
}

/// Helper to format bytes as human-readable string
pub fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * 1024 * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}
