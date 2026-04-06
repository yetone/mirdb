//! API request handlers.
//! Owner: Multiple scenarios contribute handlers
//!
//! Note: With tiny_http, handlers are implemented directly in server.rs.
//! This module provides response structures and helper functions for
//! future scenarios to use.
//!
//! Structures:
//! - `StatsResponse`: Statistics response (Scenario 2)
//! - `KeysResponse`: Keys list response (Scenarios 3, 4)
//! - `KeyDetailResponse`: Key detail response (Scenario 5)
//! - `CompactionResponse`: Compaction status response (Scenario 6)

use serde::{Deserialize, Serialize};

/// Stats response structure (Scenario 2 will fully implement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsResponse {
    pub total_keys: u64,
    pub version: String,
    pub uptime_seconds: u64,
}

impl Default for StatsResponse {
    fn default() -> Self {
        StatsResponse {
            total_keys: 0,
            version: "0.1.0".to_string(),
            uptime_seconds: 0,
        }
    }
}

/// Keys list response (Scenario 3 will fully implement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysResponse {
    pub keys: Vec<String>,
    pub total: u64,
    pub offset: u64,
    pub limit: u64,
}

impl Default for KeysResponse {
    fn default() -> Self {
        KeysResponse {
            keys: vec![],
            total: 0,
            offset: 0,
            limit: 20,
        }
    }
}

/// Key detail response (Scenario 5 will fully implement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDetailResponse {
    pub key: String,
    pub value: String,
    pub size: u64,
    pub flags: u32,
}

/// Compaction status response (Scenario 6 will fully implement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionResponse {
    pub status: String,
    pub progress: u8,
}

impl Default for CompactionResponse {
    fn default() -> Self {
        CompactionResponse {
            status: "idle".to_string(),
            progress: 0,
        }
    }
}

/// Error response for API errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl ErrorResponse {
    pub fn new(message: &str) -> Self {
        ErrorResponse {
            error: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_response_default() {
        let stats = StatsResponse::default();
        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.version, "0.1.0");
    }

    #[test]
    fn test_keys_response_default() {
        let keys = KeysResponse::default();
        assert!(keys.keys.is_empty());
        assert_eq!(keys.limit, 20);
    }

    #[test]
    fn test_error_response() {
        let err = ErrorResponse::new("Not found");
        assert_eq!(err.error, "Not found");
    }
}
