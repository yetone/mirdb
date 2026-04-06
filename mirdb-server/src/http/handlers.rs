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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Compaction status response (Scenario 6: Compaction Status Display)
///
/// Provides comprehensive compaction status information including:
/// - Current status (idle/running)
/// - Compaction type (minor/major) when running
/// - Progress percentage when available
/// - Last compaction timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionResponse {
    /// Current compaction status: "idle" or "running"
    pub status: String,
    /// Type of compaction when running: "minor", "major", or null when idle
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_type: Option<String>,
    /// Progress percentage (0-100) if available, null if not tracking
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<u8>,
    /// Unix timestamp of last completed compaction, null if never compacted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction: Option<u64>,
}

impl Default for CompactionResponse {
    fn default() -> Self {
        CompactionResponse {
            status: "idle".to_string(),
            compaction_type: None,
            progress: None,
            last_compaction: None,
        }
    }
}

impl CompactionResponse {
    /// Create an idle compaction status response
    pub fn idle(last_compaction: Option<u64>) -> Self {
        CompactionResponse {
            status: "idle".to_string(),
            compaction_type: None,
            progress: None,
            last_compaction,
        }
    }

    /// Create a running compaction status response
    pub fn running(compaction_type: &str, progress: Option<u8>, last_compaction: Option<u64>) -> Self {
        CompactionResponse {
            status: "running".to_string(),
            compaction_type: Some(compaction_type.to_string()),
            progress,
            last_compaction,
        }
    }
}

// ============================================================================
// Scenario 6: Compaction Status Tracking
// ============================================================================

/// Compaction type constants
pub const COMPACTION_TYPE_MINOR: u8 = 1;
pub const COMPACTION_TYPE_MAJOR: u8 = 2;

/// Thread-safe compaction status tracker (Scenario 6: Compaction Status Display)
///
/// Tracks the current state of compaction operations without requiring
/// locks, using atomic operations for thread-safety.
pub struct CompactionTracker {
    /// Whether compaction is currently running
    is_running: AtomicBool,
    /// Type of compaction (1=minor, 2=major, 0=none)
    compaction_type: AtomicU8,
    /// Progress percentage (0-100)
    progress: AtomicU8,
    /// Unix timestamp of last completed compaction
    last_compaction: AtomicU64,
}

impl CompactionTracker {
    /// Create a new compaction tracker
    pub const fn new() -> Self {
        CompactionTracker {
            is_running: AtomicBool::new(false),
            compaction_type: AtomicU8::new(0),
            progress: AtomicU8::new(0),
            last_compaction: AtomicU64::new(0),
        }
    }

    /// Start a compaction operation
    pub fn start(&self, compaction_type: u8) {
        self.is_running.store(true, Ordering::SeqCst);
        self.compaction_type.store(compaction_type, Ordering::SeqCst);
        self.progress.store(0, Ordering::SeqCst);
    }

    /// Update compaction progress (0-100)
    pub fn update_progress(&self, progress: u8) {
        self.progress.store(progress.min(100), Ordering::SeqCst);
    }

    /// Complete a compaction operation
    pub fn complete(&self) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_compaction.store(timestamp, Ordering::SeqCst);
        self.is_running.store(false, Ordering::SeqCst);
        self.progress.store(0, Ordering::SeqCst);
        self.compaction_type.store(0, Ordering::SeqCst);
    }

    /// Get current compaction status as a response
    pub fn get_status(&self) -> CompactionResponse {
        let is_running = self.is_running.load(Ordering::SeqCst);
        let last_compaction = self.last_compaction.load(Ordering::SeqCst);
        let last_compaction_opt = if last_compaction > 0 {
            Some(last_compaction)
        } else {
            None
        };

        if is_running {
            let compaction_type = match self.compaction_type.load(Ordering::SeqCst) {
                COMPACTION_TYPE_MINOR => "minor",
                COMPACTION_TYPE_MAJOR => "major",
                _ => "unknown",
            };
            let progress = self.progress.load(Ordering::SeqCst);
            CompactionResponse::running(compaction_type, Some(progress), last_compaction_opt)
        } else {
            CompactionResponse::idle(last_compaction_opt)
        }
    }

    /// Check if compaction is currently running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
}

impl Default for CompactionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Global compaction tracker instance (Scenario 6)
/// This allows the HTTP server to report compaction status without
/// needing direct access to internal DataManager state.
static COMPACTION_TRACKER: CompactionTracker = CompactionTracker::new();

/// Get a reference to the global compaction tracker
pub fn get_compaction_tracker() -> &'static CompactionTracker {
    &COMPACTION_TRACKER
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

    // =========================================================================
    // Scenario 6: Compaction Status Display Tests
    // =========================================================================

    #[test]
    fn test_compaction_response_default() {
        let response = CompactionResponse::default();
        assert_eq!(response.status, "idle");
        assert!(response.compaction_type.is_none());
        assert!(response.progress.is_none());
        assert!(response.last_compaction.is_none());
    }

    #[test]
    fn test_compaction_response_idle() {
        let response = CompactionResponse::idle(Some(1234567890));
        assert_eq!(response.status, "idle");
        assert!(response.compaction_type.is_none());
        assert!(response.progress.is_none());
        assert_eq!(response.last_compaction, Some(1234567890));
    }

    #[test]
    fn test_compaction_response_idle_no_timestamp() {
        let response = CompactionResponse::idle(None);
        assert_eq!(response.status, "idle");
        assert!(response.last_compaction.is_none());
    }

    #[test]
    fn test_compaction_response_running_minor() {
        let response = CompactionResponse::running("minor", Some(50), Some(1234567890));
        assert_eq!(response.status, "running");
        assert_eq!(response.compaction_type, Some("minor".to_string()));
        assert_eq!(response.progress, Some(50));
        assert_eq!(response.last_compaction, Some(1234567890));
    }

    #[test]
    fn test_compaction_response_running_major() {
        let response = CompactionResponse::running("major", Some(75), None);
        assert_eq!(response.status, "running");
        assert_eq!(response.compaction_type, Some("major".to_string()));
        assert_eq!(response.progress, Some(75));
        assert!(response.last_compaction.is_none());
    }

    #[test]
    fn test_compaction_response_serialization_idle() {
        let response = CompactionResponse::idle(Some(1234567890));
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""status":"idle""#));
        assert!(json.contains(r#""last_compaction":1234567890"#));
        // Optional fields with None should not appear
        assert!(!json.contains("compaction_type"));
        assert!(!json.contains("progress"));
    }

    #[test]
    fn test_compaction_response_serialization_running() {
        let response = CompactionResponse::running("minor", Some(50), Some(1234567890));
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""status":"running""#));
        assert!(json.contains(r#""compaction_type":"minor""#));
        assert!(json.contains(r#""progress":50"#));
        assert!(json.contains(r#""last_compaction":1234567890"#));
    }

    #[test]
    fn test_compaction_tracker_initial_state() {
        let tracker = CompactionTracker::new();
        assert!(!tracker.is_running());
        let status = tracker.get_status();
        assert_eq!(status.status, "idle");
        assert!(status.compaction_type.is_none());
    }

    #[test]
    fn test_compaction_tracker_start_minor() {
        let tracker = CompactionTracker::new();
        tracker.start(COMPACTION_TYPE_MINOR);
        assert!(tracker.is_running());
        let status = tracker.get_status();
        assert_eq!(status.status, "running");
        assert_eq!(status.compaction_type, Some("minor".to_string()));
        assert_eq!(status.progress, Some(0));
    }

    #[test]
    fn test_compaction_tracker_start_major() {
        let tracker = CompactionTracker::new();
        tracker.start(COMPACTION_TYPE_MAJOR);
        assert!(tracker.is_running());
        let status = tracker.get_status();
        assert_eq!(status.status, "running");
        assert_eq!(status.compaction_type, Some("major".to_string()));
    }

    #[test]
    fn test_compaction_tracker_update_progress() {
        let tracker = CompactionTracker::new();
        tracker.start(COMPACTION_TYPE_MINOR);
        tracker.update_progress(50);
        let status = tracker.get_status();
        assert_eq!(status.progress, Some(50));

        tracker.update_progress(75);
        let status = tracker.get_status();
        assert_eq!(status.progress, Some(75));
    }

    #[test]
    fn test_compaction_tracker_progress_capped_at_100() {
        let tracker = CompactionTracker::new();
        tracker.start(COMPACTION_TYPE_MINOR);
        tracker.update_progress(150); // Over 100
        let status = tracker.get_status();
        assert_eq!(status.progress, Some(100)); // Should be capped
    }

    #[test]
    fn test_compaction_tracker_complete() {
        let tracker = CompactionTracker::new();
        tracker.start(COMPACTION_TYPE_MINOR);
        tracker.update_progress(50);
        tracker.complete();

        assert!(!tracker.is_running());
        let status = tracker.get_status();
        assert_eq!(status.status, "idle");
        assert!(status.compaction_type.is_none());
        assert!(status.progress.is_none());
        // last_compaction should be set to a timestamp
        assert!(status.last_compaction.is_some());
    }

    #[test]
    fn test_compaction_tracker_lifecycle() {
        let tracker = CompactionTracker::new();

        // Initial state - idle
        let status = tracker.get_status();
        assert_eq!(status.status, "idle");
        assert!(status.last_compaction.is_none());

        // Start minor compaction
        tracker.start(COMPACTION_TYPE_MINOR);
        let status = tracker.get_status();
        assert_eq!(status.status, "running");
        assert_eq!(status.compaction_type, Some("minor".to_string()));

        // Update progress
        tracker.update_progress(30);
        tracker.update_progress(60);
        tracker.update_progress(100);
        let status = tracker.get_status();
        assert_eq!(status.progress, Some(100));

        // Complete compaction
        tracker.complete();
        let status = tracker.get_status();
        assert_eq!(status.status, "idle");
        assert!(status.last_compaction.is_some());

        // Start another compaction (major)
        let prev_timestamp = status.last_compaction;
        tracker.start(COMPACTION_TYPE_MAJOR);
        let status = tracker.get_status();
        assert_eq!(status.status, "running");
        assert_eq!(status.compaction_type, Some("major".to_string()));
        // Last compaction should still be the previous one
        assert_eq!(status.last_compaction, prev_timestamp);
    }

    #[test]
    fn test_get_compaction_tracker() {
        let tracker = get_compaction_tracker();
        // Should return a valid tracker
        let status = tracker.get_status();
        // Status should be either idle or running
        assert!(status.status == "idle" || status.status == "running");
    }
}
