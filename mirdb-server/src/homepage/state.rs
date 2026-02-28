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
    /// Listen address
    pub addr: String,
}

impl AppState {
    /// Create a new AppState with default values
    pub fn new(config: HomepageConfig) -> Self {
        Self {
            config,
            running: true,
            work_dir: String::from("/tmp/mirdb"),
            addr: String::from("0.0.0.0:12333"),
        }
    }

    /// Create AppState with custom configuration
    pub fn with_config(config: HomepageConfig, work_dir: String, addr: String) -> Self {
        Self {
            config,
            running: true,
            work_dir,
            addr,
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
