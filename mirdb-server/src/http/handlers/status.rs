//! Server Status API Handler
//! Owner: Scenario 3 - Server Status Dashboard
//! Co-owner: Scenario 9 - LSM Tree Statistics
//!
//! Expected exports:
//! - handle_status(store: Arc<Store>) -> Response
//! - StatusResponse struct with: memory_usage, active_connections, database_size
//! - LSM stats: memtable_count, sstable_count, total_size

use serde::Serialize;

/// Server status response
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub memory_usage: u64,
    pub active_connections: u32,
    pub database_size: u64,
    pub memtable_count: u32,
    pub sstable_count: u32,
    pub total_size: u64,
}

impl Default for StatusResponse {
    fn default() -> Self {
        StatusResponse {
            memory_usage: 0,
            active_connections: 0,
            database_size: 0,
            memtable_count: 0,
            sstable_count: 0,
            total_size: 0,
        }
    }
}
