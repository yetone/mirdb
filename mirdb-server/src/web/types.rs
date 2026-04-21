//! Shared types for web API responses.
//!
//! This module contains all response types used by the web API endpoints.
//! All responses serialize to consistent JSON format.

use serde::{Deserialize, Serialize};

/// Generic API response wrapper for successful responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: T,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data,
        }
    }
}

/// Error response with code and message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub success: bool,
    pub error: String,
    pub code: u16,
}

impl ApiError {
    pub fn new(error: impl Into<String>, code: u16) -> Self {
        Self {
            success: false,
            error: error.into(),
            code,
        }
    }

    pub fn internal(error: impl Into<String>) -> Self {
        Self::new(error, 500)
    }

    pub fn not_found(error: impl Into<String>) -> Self {
        Self::new(error, 404)
    }

    pub fn bad_request(error: impl Into<String>) -> Self {
        Self::new(error, 400)
    }
}

/// Server metrics data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    /// Memory currently used by the store in bytes
    pub memory_used_bytes: u64,
    /// Total memory available for the store in bytes
    pub memory_total_bytes: u64,
    /// Disk space used by SSTable files in bytes
    pub disk_used_bytes: u64,
    /// Total disk space available in bytes
    pub disk_total_bytes: u64,
    /// Number of keys stored in the database
    pub key_count: u64,
    /// Number of active client connections
    pub active_connections: u64,
    /// Whether compaction is currently running
    pub compaction_running: bool,
    /// Count of SSTables at each LSM level (7 levels max)
    pub sstable_level_counts: Vec<u64>,
}

impl Default for MetricsResponse {
    fn default() -> Self {
        Self {
            memory_used_bytes: 0,
            memory_total_bytes: 0,
            disk_used_bytes: 0,
            disk_total_bytes: 0,
            key_count: 0,
            active_connections: 0,
            compaction_running: false,
            sstable_level_counts: vec![0; 7],
        }
    }
}

/// Configuration data response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    pub listen_addr: String,
    pub max_lsm_levels: usize,
    pub work_dir: String,
    pub sstable_max_size: usize,
    pub memtable_max_size: usize,
    pub block_size: usize,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub server_running: bool,
    pub compaction_status: String,
}

/// Key-value operation response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResponse {
    pub success: bool,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_response_serialization() {
        let metrics = MetricsResponse {
            memory_used_bytes: 1024,
            memory_total_bytes: 4096,
            disk_used_bytes: 10240,
            disk_total_bytes: 1048576,
            key_count: 100,
            active_connections: 5,
            compaction_running: false,
            sstable_level_counts: vec![2, 4, 0, 0, 0, 0, 0],
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("memory_used_bytes"));
        assert!(json.contains("sstable_level_counts"));
    }

    #[test]
    fn test_api_error_creation() {
        let error = ApiError::internal("test error");
        assert_eq!(error.code, 500);
        assert!(!error.success);
    }

    #[test]
    fn test_default_metrics_response() {
        let metrics = MetricsResponse::default();
        assert_eq!(metrics.sstable_level_counts.len(), 7);
        assert!(!metrics.compaction_running);
    }
}
