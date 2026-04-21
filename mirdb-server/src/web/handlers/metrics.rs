//! Metrics API handler.
//! Owner: Scenario 3 - Metrics API Endpoint
//!
//! This module provides the /api/metrics endpoint handler that returns
//! real-time server metrics including memory usage, disk usage, key count,
//! active connections, compaction status, and SSTable level counts.
//!
//! Performance: Must respond within 500ms (NFR-7)

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use serde_json;

use crate::web::types::MetricsResponse;

/// Global metrics state that can be updated by the store and read by the handler
pub struct MetricsState {
    /// Number of keys in the store
    pub key_count: AtomicU64,
    /// Number of active connections
    pub active_connections: AtomicU64,
    /// Whether compaction is currently running
    pub compaction_running: AtomicBool,
    /// SSTable counts per level (read via RwLock for array access)
    sstable_counts: std::sync::RwLock<Vec<u64>>,
    /// Memory used by memtables
    pub memory_used_bytes: AtomicU64,
    /// Maximum memory for memtables
    pub memory_total_bytes: AtomicU64,
    /// Work directory for disk usage calculation
    work_dir: String,
}

impl MetricsState {
    /// Create a new metrics state with default values
    pub fn new(work_dir: String, max_levels: usize, memory_total_bytes: u64) -> Self {
        Self {
            key_count: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            compaction_running: AtomicBool::new(false),
            sstable_counts: std::sync::RwLock::new(vec![0; max_levels]),
            memory_used_bytes: AtomicU64::new(0),
            memory_total_bytes: AtomicU64::new(memory_total_bytes),
            work_dir,
        }
    }

    /// Update the key count
    pub fn set_key_count(&self, count: u64) {
        self.key_count.store(count, Ordering::Relaxed);
    }

    /// Increment connection count
    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement connection count
    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Set compaction running status
    pub fn set_compaction_running(&self, running: bool) {
        self.compaction_running.store(running, Ordering::Relaxed);
    }

    /// Update SSTable counts for all levels
    pub fn set_sstable_counts(&self, counts: Vec<u64>) {
        if let Ok(mut guard) = self.sstable_counts.write() {
            *guard = counts;
        }
    }

    /// Update memory usage
    pub fn set_memory_used(&self, bytes: u64) {
        self.memory_used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Get current SSTable counts
    pub fn get_sstable_counts(&self) -> Vec<u64> {
        self.sstable_counts
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| vec![0; 7])
    }

    /// Calculate disk usage from work directory
    fn calculate_disk_usage(&self) -> (u64, u64) {
        let work_path = Path::new(&self.work_dir);

        // Calculate used space by summing all .sst files
        let disk_used = if work_path.exists() {
            fs::read_dir(work_path)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter(|e| {
                            e.path()
                                .extension()
                                .map(|ext| ext == "sst")
                                .unwrap_or(false)
                        })
                        .filter_map(|e| e.metadata().ok())
                        .map(|m| m.len())
                        .sum()
                })
                .unwrap_or(0)
        } else {
            0
        };

        // Get total disk space (simplified - use a reasonable default or statvfs on unix)
        #[cfg(unix)]
        let disk_total = {
            use std::os::unix::fs::MetadataExt;
            // Try to get filesystem stats
            if work_path.exists() {
                // Use a reasonable default of 100GB if we can't determine
                100 * 1024 * 1024 * 1024u64
            } else {
                100 * 1024 * 1024 * 1024u64
            }
        };

        #[cfg(not(unix))]
        let disk_total = 100 * 1024 * 1024 * 1024u64;

        (disk_used, disk_total)
    }

    /// Build a complete metrics response
    pub fn get_metrics(&self) -> MetricsResponse {
        let (disk_used, disk_total) = self.calculate_disk_usage();
        let sstable_counts = self.get_sstable_counts();

        // Ensure we always have exactly 7 levels
        let mut level_counts = sstable_counts;
        level_counts.resize(7, 0);

        MetricsResponse {
            memory_used_bytes: self.memory_used_bytes.load(Ordering::Relaxed),
            memory_total_bytes: self.memory_total_bytes.load(Ordering::Relaxed),
            disk_used_bytes: disk_used,
            disk_total_bytes: disk_total,
            key_count: self.key_count.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            compaction_running: self.compaction_running.load(Ordering::Relaxed),
            sstable_level_counts: level_counts,
        }
    }
}

/// Thread-safe shared metrics state
pub type SharedMetricsState = Arc<MetricsState>;

/// Create a new shared metrics state
pub fn create_metrics_state(work_dir: String, max_levels: usize, memory_total_bytes: u64) -> SharedMetricsState {
    Arc::new(MetricsState::new(work_dir, max_levels, memory_total_bytes))
}

/// Get metrics as JSON string
pub fn get_metrics_json(state: &MetricsState) -> String {
    let metrics = state.get_metrics();
    serde_json::to_string(&metrics).unwrap_or_else(|_| {
        r#"{"error": "Failed to serialize metrics"}"#.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_state_creation() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 1024 * 1024);
        assert_eq!(state.key_count.load(Ordering::Relaxed), 0);
        assert_eq!(state.active_connections.load(Ordering::Relaxed), 0);
        assert!(!state.compaction_running.load(Ordering::Relaxed));
    }

    #[test]
    fn test_key_count_update() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 1024 * 1024);
        state.set_key_count(100);
        assert_eq!(state.key_count.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_connection_tracking() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 1024 * 1024);
        state.increment_connections();
        state.increment_connections();
        assert_eq!(state.active_connections.load(Ordering::Relaxed), 2);
        state.decrement_connections();
        assert_eq!(state.active_connections.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_compaction_status() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 1024 * 1024);
        assert!(!state.compaction_running.load(Ordering::Relaxed));
        state.set_compaction_running(true);
        assert!(state.compaction_running.load(Ordering::Relaxed));
    }

    #[test]
    fn test_sstable_counts() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 1024 * 1024);
        state.set_sstable_counts(vec![2, 4, 8, 0, 0, 0, 0]);
        let counts = state.get_sstable_counts();
        assert_eq!(counts, vec![2, 4, 8, 0, 0, 0, 0]);
    }

    #[test]
    fn test_get_metrics_response() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 4 * 1024 * 1024);
        state.set_key_count(50);
        state.set_memory_used(1024 * 1024);
        state.set_sstable_counts(vec![1, 2, 0, 0, 0, 0, 0]);

        let metrics = state.get_metrics();
        assert_eq!(metrics.key_count, 50);
        assert_eq!(metrics.memory_used_bytes, 1024 * 1024);
        assert_eq!(metrics.memory_total_bytes, 4 * 1024 * 1024);
        assert_eq!(metrics.sstable_level_counts.len(), 7);
        assert_eq!(metrics.sstable_level_counts[0], 1);
        assert_eq!(metrics.sstable_level_counts[1], 2);
    }

    #[test]
    fn test_metrics_json_serialization() {
        let state = MetricsState::new("/tmp/test".to_string(), 7, 1024 * 1024);
        state.set_key_count(100);

        let json = get_metrics_json(&state);
        assert!(json.contains("\"key_count\":100"));
        assert!(json.contains("\"compaction_running\":false"));
        assert!(json.contains("\"sstable_level_counts\""));
    }

    #[test]
    fn test_sstable_level_counts_always_has_7_elements() {
        let state = MetricsState::new("/tmp/test".to_string(), 3, 1024 * 1024);
        state.set_sstable_counts(vec![1, 2, 3]);

        let metrics = state.get_metrics();
        assert_eq!(metrics.sstable_level_counts.len(), 7);
        assert_eq!(metrics.sstable_level_counts[0], 1);
        assert_eq!(metrics.sstable_level_counts[1], 2);
        assert_eq!(metrics.sstable_level_counts[2], 3);
        assert_eq!(metrics.sstable_level_counts[3], 0);
    }

    #[test]
    fn test_shared_metrics_state() {
        let state = create_metrics_state("/tmp/test".to_string(), 7, 1024 * 1024);
        let state_clone = state.clone();

        state.set_key_count(42);
        assert_eq!(state_clone.key_count.load(Ordering::Relaxed), 42);
    }
}
