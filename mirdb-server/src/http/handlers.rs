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

/// Stats response structure (Scenario 2 - Database Statistics Display)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsResponse {
    pub total_keys: u64,
    pub memory_usage: u64,
    pub storage_size: u64,
    pub version: String,
    pub uptime_seconds: u64,
    pub last_updated: u64, // Unix timestamp for cache validation
}

impl Default for StatsResponse {
    fn default() -> Self {
        StatsResponse {
            total_keys: 0,
            memory_usage: 0,
            storage_size: 0,
            version: "0.1.0".to_string(),
            uptime_seconds: 0,
            last_updated: 0,
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

// ============================================================================
// Scenario 4: Key Search and Filter
// ============================================================================

/// Filter keys by search prefix (Scenario 4: Key Search and Filter)
///
/// Performs case-sensitive prefix matching on keys.
/// An empty search string matches all keys.
///
/// # Arguments
/// * `keys` - Vector of key strings to filter
/// * `search` - Search prefix to match against
///
/// # Returns
/// Filtered vector containing only keys that start with the search prefix
pub fn filter_keys_by_search(keys: Vec<String>, search: &str) -> Vec<String> {
    if search.is_empty() {
        return keys;
    }
    keys.into_iter()
        .filter(|key| key.starts_with(search))
        .collect()
}

/// URL-decode a search parameter (Scenario 4: Key Search and Filter)
///
/// Handles percent-encoding and plus signs in search queries.
///
/// # Arguments
/// * `encoded` - URL-encoded search string
///
/// # Returns
/// Decoded search string, or the original if decoding fails
pub fn decode_search_param(encoded: &str) -> String {
    let mut result = Vec::with_capacity(encoded.len());
    let mut chars = encoded.bytes().peekable();

    while let Some(b) = chars.next() {
        if b == b'%' {
            let high = chars.next();
            let low = chars.next();
            if let (Some(h), Some(l)) = (high, low) {
                let hex_str = format!("{}{}", h as char, l as char);
                if let Ok(decoded) = u8::from_str_radix(&hex_str, 16) {
                    result.push(decoded);
                    continue;
                }
            }
            result.push(b);
        } else if b == b'+' {
            result.push(b' ');
        } else {
            result.push(b);
        }
    }

    String::from_utf8(result).unwrap_or_else(|_| encoded.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_response_default() {
        let stats = StatsResponse::default();
        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.memory_usage, 0);
        assert_eq!(stats.storage_size, 0);
        assert_eq!(stats.version, "0.1.0");
        assert_eq!(stats.uptime_seconds, 0);
        assert_eq!(stats.last_updated, 0);
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

    // =========================================================================
    // Scenario 4: Key Search and Filter Tests
    // =========================================================================

    #[test]
    fn test_filter_keys_by_search_empty_search() {
        let keys = vec![
            "user:1".to_string(),
            "session:abc".to_string(),
            "cache:data".to_string(),
        ];
        let filtered = filter_keys_by_search(keys.clone(), "");
        assert_eq!(filtered, keys);
    }

    #[test]
    fn test_filter_keys_by_search_prefix_match() {
        let keys = vec![
            "user:1".to_string(),
            "user:2".to_string(),
            "session:abc".to_string(),
            "cache:data".to_string(),
        ];
        let filtered = filter_keys_by_search(keys, "user:");
        assert_eq!(filtered, vec!["user:1", "user:2"]);
    }

    #[test]
    fn test_filter_keys_by_search_no_match() {
        let keys = vec![
            "user:1".to_string(),
            "session:abc".to_string(),
            "cache:data".to_string(),
        ];
        let filtered = filter_keys_by_search(keys, "nonexistent");
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_keys_by_search_case_sensitive() {
        let keys = vec![
            "user:1".to_string(),
            "User:2".to_string(),
            "USER:3".to_string(),
        ];
        let filtered = filter_keys_by_search(keys, "user:");
        assert_eq!(filtered, vec!["user:1"]);
    }

    #[test]
    fn test_filter_keys_by_search_partial_prefix() {
        let keys = vec![
            "user:admin".to_string(),
            "user:guest".to_string(),
            "username:test".to_string(),
        ];
        let filtered = filter_keys_by_search(keys, "user:");
        assert_eq!(filtered, vec!["user:admin", "user:guest"]);
    }

    #[test]
    fn test_decode_search_param_simple() {
        assert_eq!(decode_search_param("hello"), "hello");
        assert_eq!(decode_search_param("user:"), "user:");
    }

    #[test]
    fn test_decode_search_param_url_encoded() {
        assert_eq!(decode_search_param("user%3A"), "user:");
        assert_eq!(decode_search_param("hello%20world"), "hello world");
        assert_eq!(decode_search_param("key%2Fvalue"), "key/value");
    }

    #[test]
    fn test_decode_search_param_plus_sign() {
        assert_eq!(decode_search_param("hello+world"), "hello world");
    }
}
