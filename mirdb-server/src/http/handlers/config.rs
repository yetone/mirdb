//! Configuration API Handler
//! Owner: Scenario 8 - Configuration Display
//!
//! Expected exports:
//! - handle_config(options: Options) -> Response
//! - ConfigResponse struct with all configuration fields

use serde::Serialize;

/// Configuration response
#[derive(Debug, Serialize)]
pub struct ConfigResponse {
    pub work_dir: String,
    pub port: u16,
    pub max_levels: u32,
    pub memtable_size: u64,
}

impl Default for ConfigResponse {
    fn default() -> Self {
        ConfigResponse {
            work_dir: String::new(),
            port: 11211,
            max_levels: 7,
            memtable_size: 4 * 1024 * 1024,
        }
    }
}
