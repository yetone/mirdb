//! Server Status API Handler
//! Owner: Scenario 3 - Server Status Dashboard
//! Co-owner: Scenario 9 - LSM Tree Statistics
//!
//! Expected exports:
//! - handle_status() -> StatusResponse
//! - StatusResponse struct with: memory_usage, active_connections, database_size
//! - LSM stats: memtable_count, sstable_count, total_size

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

/// Global counter for active connections (thread-safe)
static ACTIVE_CONNECTIONS: AtomicU32 = AtomicU32::new(0);

/// Increment active connection count
pub fn increment_connections() {
    ACTIVE_CONNECTIONS.fetch_add(1, Ordering::SeqCst);
}

/// Decrement active connection count
pub fn decrement_connections() {
    ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
}

/// Get current active connection count
pub fn get_active_connections() -> u32 {
    ACTIVE_CONNECTIONS.load(Ordering::SeqCst)
}

/// Server status response
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

/// Get current memory usage of the process in bytes
fn get_memory_usage() -> u64 {
    // Try to read from /proc/self/statm on Linux
    if let Ok(content) = fs::read_to_string("/proc/self/statm") {
        let parts: Vec<&str> = content.split_whitespace().collect();
        if parts.len() >= 2 {
            // Second field is RSS (resident set size) in pages
            if let Ok(rss_pages) = parts[1].parse::<u64>() {
                // Multiply by page size (typically 4096)
                return rss_pages * 4096;
            }
        }
    }
    // Fallback: return a reasonable estimate
    0
}

/// Calculate database size from work directory
fn get_database_size(work_dir: &str) -> u64 {
    let path = Path::new(work_dir);
    if !path.exists() {
        return 0;
    }

    calculate_dir_size(path)
}

/// Recursively calculate directory size
fn calculate_dir_size(path: &Path) -> u64 {
    let mut size = 0;

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_file() {
                if let Ok(metadata) = entry.metadata() {
                    size += metadata.len();
                }
            } else if entry_path.is_dir() {
                size += calculate_dir_size(&entry_path);
            }
        }
    }

    size
}

/// Count SSTable files in work directory
fn count_sstables(work_dir: &str) -> u32 {
    let path = Path::new(work_dir);
    if !path.exists() {
        return 0;
    }

    let mut count = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_file() {
                if let Some(ext) = entry_path.extension() {
                    if ext == "sst" {
                        count += 1;
                    }
                }
            }
        }
    }

    count
}

/// Generate server status response
///
/// Returns a StatusResponse with current server metrics including:
/// - memory_usage: Current process memory in bytes
/// - active_connections: Number of active client connections
/// - database_size: Total size of database files in bytes
/// - memtable_count: Number of memtables (placeholder for Scenario 9)
/// - sstable_count: Number of SSTable files
/// - total_size: Total storage size
pub fn handle_status(work_dir: &str) -> StatusResponse {
    let memory_usage = get_memory_usage();
    let active_connections = get_active_connections();
    let database_size = get_database_size(work_dir);
    let sstable_count = count_sstables(work_dir);

    // memtable_count is a placeholder - Scenario 9 will implement proper LSM stats
    let memtable_count = 1; // Default active memtable

    StatusResponse {
        memory_usage,
        active_connections,
        database_size,
        memtable_count,
        sstable_count,
        total_size: database_size,
    }
}

/// Generate status response as JSON string
pub fn handle_status_json(work_dir: &str) -> String {
    let status = handle_status(work_dir);
    serde_json::to_string(&status).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, File};
    use std::io::Write;

    #[test]
    fn test_status_response_default() {
        let status = StatusResponse::default();
        assert_eq!(status.memory_usage, 0);
        assert_eq!(status.active_connections, 0);
        assert_eq!(status.database_size, 0);
        assert_eq!(status.memtable_count, 0);
        assert_eq!(status.sstable_count, 0);
        assert_eq!(status.total_size, 0);
    }

    #[test]
    fn test_status_response_serialization() {
        let status = StatusResponse {
            memory_usage: 1024,
            active_connections: 5,
            database_size: 2048,
            memtable_count: 2,
            sstable_count: 3,
            total_size: 2048,
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"memory_usage\":1024"));
        assert!(json.contains("\"active_connections\":5"));
        assert!(json.contains("\"database_size\":2048"));
    }

    #[test]
    fn test_connection_counting() {
        let initial = get_active_connections();

        increment_connections();
        assert_eq!(get_active_connections(), initial + 1);

        increment_connections();
        assert_eq!(get_active_connections(), initial + 2);

        decrement_connections();
        assert_eq!(get_active_connections(), initial + 1);

        decrement_connections();
        assert_eq!(get_active_connections(), initial);
    }

    #[test]
    fn test_handle_status_returns_valid_response() {
        let status = handle_status("/tmp/mirdb-test-nonexistent");

        // Should return valid response even for non-existent directory
        assert_eq!(status.database_size, 0);
        assert_eq!(status.sstable_count, 0);
        assert_eq!(status.memtable_count, 1); // Default active memtable
    }

    #[test]
    fn test_handle_status_json() {
        let json = handle_status_json("/tmp/mirdb-test-nonexistent");

        // Should be valid JSON
        let parsed: Result<StatusResponse, _> = serde_json::from_str(&json);
        assert!(parsed.is_ok());

        // Should contain required fields
        assert!(json.contains("memory_usage"));
        assert!(json.contains("active_connections"));
        assert!(json.contains("database_size"));
    }

    #[test]
    fn test_database_size_calculation() {
        let test_dir = "/tmp/mirdb-status-test";
        let _ = fs::remove_dir_all(test_dir);
        create_dir_all(test_dir).unwrap();

        // Create a test file
        let mut file = File::create(format!("{}/test.sst", test_dir)).unwrap();
        file.write_all(b"test data 12345").unwrap();

        let size = get_database_size(test_dir);
        assert!(size > 0);

        let count = count_sstables(test_dir);
        assert_eq!(count, 1);

        // Cleanup
        let _ = fs::remove_dir_all(test_dir);
    }
}
