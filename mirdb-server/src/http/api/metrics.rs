//! Performance metrics API endpoint.
//!
//! Owner: Scenario 3 - Performance Metrics API
//! Co-owner: Scenario 12 - API Endpoint Performance (for testing)
//!
//! Expected exports:
//! - MetricsResponse: Metrics response structure
//! - get_metrics(state): Handler returning performance metrics
//!
//! Response format:
//! {
//!     "total_keys": u64,
//!     "ops_per_second": f64,
//!     "memory_usage_bytes": u64,
//!     "storage_used_bytes": u64,
//!     "uptime_seconds": u64
//! }

use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Metrics response structure for /api/metrics endpoint
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsResponse {
    /// Total number of keys stored
    pub total_keys: u64,
    /// Operations per second (averaged)
    pub ops_per_second: f64,
    /// Current memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Storage space used in bytes
    pub storage_used_bytes: u64,
    /// Server uptime in seconds
    pub uptime_seconds: u64,
}

impl Default for MetricsResponse {
    fn default() -> Self {
        Self {
            total_keys: 0,
            ops_per_second: 0.0,
            memory_usage_bytes: 0,
            storage_used_bytes: 0,
            uptime_seconds: 0,
        }
    }
}

/// Metrics state container for tracking performance metrics
#[derive(Debug)]
pub struct MetricsState {
    start_time: Instant,
    total_keys: u64,
    total_ops: u64,
    memory_usage_bytes: u64,
    storage_used_bytes: u64,
}

impl MetricsState {
    /// Create a new metrics state instance
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_keys: 0,
            total_ops: 0,
            memory_usage_bytes: 0,
            storage_used_bytes: 0,
        }
    }

    /// Get current uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Calculate ops per second
    pub fn ops_per_second(&self) -> f64 {
        let uptime = self.uptime_seconds();
        if uptime == 0 {
            0.0
        } else {
            self.total_ops as f64 / uptime as f64
        }
    }

    /// Update total keys count
    pub fn set_total_keys(&mut self, count: u64) {
        self.total_keys = count;
    }

    /// Increment operation counter
    pub fn increment_ops(&mut self) {
        self.total_ops += 1;
    }

    /// Update memory usage
    pub fn set_memory_usage(&mut self, bytes: u64) {
        self.memory_usage_bytes = bytes;
    }

    /// Update storage usage
    pub fn set_storage_usage(&mut self, bytes: u64) {
        self.storage_used_bytes = bytes;
    }

    /// Get total keys
    pub fn total_keys(&self) -> u64 {
        self.total_keys
    }

    /// Get memory usage
    pub fn memory_usage_bytes(&self) -> u64 {
        self.memory_usage_bytes
    }

    /// Get storage usage
    pub fn storage_used_bytes(&self) -> u64 {
        self.storage_used_bytes
    }
}

impl Default for MetricsState {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the current metrics as a MetricsResponse
///
/// This function generates a metrics response based on the current metrics state.
///
/// # Arguments
/// * `state` - Reference to the MetricsState
///
/// # Returns
/// A MetricsResponse containing current performance metrics
pub fn get_metrics(state: &MetricsState) -> MetricsResponse {
    MetricsResponse {
        total_keys: state.total_keys(),
        ops_per_second: state.ops_per_second(),
        memory_usage_bytes: state.memory_usage_bytes(),
        storage_used_bytes: state.storage_used_bytes(),
        uptime_seconds: state.uptime_seconds(),
    }
}

/// Generate JSON string response for metrics endpoint
///
/// # Arguments
/// * `state` - Reference to the MetricsState
///
/// # Returns
/// JSON string of the metrics response
pub fn get_metrics_json(state: &MetricsState) -> Result<String, serde_json::Error> {
    let response = get_metrics(state);
    serde_json::to_string(&response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_response_default() {
        let response = MetricsResponse::default();
        assert_eq!(response.total_keys, 0);
        assert_eq!(response.ops_per_second, 0.0);
        assert_eq!(response.memory_usage_bytes, 0);
        assert_eq!(response.storage_used_bytes, 0);
        assert_eq!(response.uptime_seconds, 0);
    }

    #[test]
    fn test_metrics_state_new() {
        let state = MetricsState::new();
        assert_eq!(state.total_keys(), 0);
        assert!(state.uptime_seconds() >= 0);
    }

    #[test]
    fn test_metrics_state_setters() {
        let mut state = MetricsState::new();

        state.set_total_keys(100);
        assert_eq!(state.total_keys(), 100);

        state.set_memory_usage(1024);
        assert_eq!(state.memory_usage_bytes(), 1024);

        state.set_storage_usage(2048);
        assert_eq!(state.storage_used_bytes(), 2048);
    }

    #[test]
    fn test_metrics_state_ops() {
        let mut state = MetricsState::new();
        state.increment_ops();
        state.increment_ops();
        // ops_per_second depends on elapsed time
        let response = get_metrics(&state);
        assert!(response.ops_per_second >= 0.0);
    }

    #[test]
    fn test_get_metrics() {
        let mut state = MetricsState::new();
        state.set_total_keys(50);
        state.set_memory_usage(4096);
        state.set_storage_usage(8192);

        let response = get_metrics(&state);
        assert_eq!(response.total_keys, 50);
        assert_eq!(response.memory_usage_bytes, 4096);
        assert_eq!(response.storage_used_bytes, 8192);
    }

    #[test]
    fn test_get_metrics_json() {
        let state = MetricsState::new();
        let json = get_metrics_json(&state).unwrap();

        assert!(json.contains("\"total_keys\":"));
        assert!(json.contains("\"ops_per_second\":"));
        assert!(json.contains("\"memory_usage_bytes\":"));
        assert!(json.contains("\"storage_used_bytes\":"));
        assert!(json.contains("\"uptime_seconds\":"));
    }

    #[test]
    fn test_metrics_response_json_structure() {
        let response = MetricsResponse {
            total_keys: 1000,
            ops_per_second: 150.5,
            memory_usage_bytes: 1048576,
            storage_used_bytes: 2097152,
            uptime_seconds: 3600,
        };

        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["total_keys"], 1000);
        assert_eq!(parsed["ops_per_second"], 150.5);
        assert_eq!(parsed["memory_usage_bytes"], 1048576);
        assert_eq!(parsed["storage_used_bytes"], 2097152);
        assert_eq!(parsed["uptime_seconds"], 3600);
    }
}
