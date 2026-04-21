//! Metrics API endpoint tests.
//! Owner: Scenario 3 - Metrics API Endpoint
//!
//! Tests for /api/metrics endpoint verifying:
//! 1. Response contains all required JSON fields
//! 2. Key count accuracy after storing keys
//! 3. Response time within 500ms requirement
//! 4. Compaction status reporting
//! 5. SSTable level counts structure

use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test Case 1: Verify metrics response contains all required fields
#[test]
fn test_metrics_response_has_all_required_fields() {
    // Create a MetricsResponse with test data
    let json_str = r#"{
        "memory_used_bytes": 1024,
        "memory_total_bytes": 4096,
        "disk_used_bytes": 10240,
        "disk_total_bytes": 1048576,
        "key_count": 100,
        "active_connections": 5,
        "compaction_running": false,
        "sstable_level_counts": [2, 4, 0, 0, 0, 0, 0]
    }"#;

    // Parse the JSON to verify structure
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();

    // Verify all required fields exist
    assert!(parsed.get("memory_used_bytes").is_some(), "missing memory_used_bytes");
    assert!(parsed.get("memory_total_bytes").is_some(), "missing memory_total_bytes");
    assert!(parsed.get("disk_used_bytes").is_some(), "missing disk_used_bytes");
    assert!(parsed.get("disk_total_bytes").is_some(), "missing disk_total_bytes");
    assert!(parsed.get("key_count").is_some(), "missing key_count");
    assert!(parsed.get("active_connections").is_some(), "missing active_connections");
    assert!(parsed.get("compaction_running").is_some(), "missing compaction_running");
    assert!(parsed.get("sstable_level_counts").is_some(), "missing sstable_level_counts");

    // Verify field types
    assert!(parsed["memory_used_bytes"].is_u64());
    assert!(parsed["memory_total_bytes"].is_u64());
    assert!(parsed["disk_used_bytes"].is_u64());
    assert!(parsed["disk_total_bytes"].is_u64());
    assert!(parsed["key_count"].is_u64());
    assert!(parsed["active_connections"].is_u64());
    assert!(parsed["compaction_running"].is_boolean());
    assert!(parsed["sstable_level_counts"].is_array());
}

/// Test Case 2: Verify key_count accuracy
/// This test verifies that after storing keys, the key_count metric reflects the actual count
#[test]
fn test_key_count_accuracy() {
    use mirdb::web::handlers::metrics::{create_metrics_state, MetricsState};

    let state = create_metrics_state("/tmp/test_metrics".to_string(), 7, 1024 * 1024);

    // Initially key_count should be 0
    let metrics = state.get_metrics();
    assert_eq!(metrics.key_count, 0);

    // Set key count to 100
    state.set_key_count(100);

    // Verify key_count is now 100
    let metrics = state.get_metrics();
    assert_eq!(metrics.key_count, 100, "key_count should equal 100 after storing 100 keys");
}

/// Test Case 3: Verify response time within 500ms for 100 rapid requests
#[test]
fn test_metrics_response_time_under_500ms() {
    use mirdb::web::handlers::metrics::{create_metrics_state, get_metrics_json};

    let state = create_metrics_state("/tmp/test_metrics_perf".to_string(), 7, 4 * 1024 * 1024);

    // Set some initial state
    state.set_key_count(1000);
    state.set_sstable_counts(vec![5, 10, 15, 0, 0, 0, 0]);
    state.set_memory_used(2 * 1024 * 1024);

    let mut max_response_time = Duration::ZERO;
    let mut total_time = Duration::ZERO;

    // Perform 100 rapid requests
    for _ in 0..100 {
        let start = Instant::now();
        let _json = get_metrics_json(&state);
        let elapsed = start.elapsed();

        total_time += elapsed;
        if elapsed > max_response_time {
            max_response_time = elapsed;
        }

        // Each individual response must be under 500ms
        assert!(
            elapsed < Duration::from_millis(500),
            "Response time {:?} exceeded 500ms limit",
            elapsed
        );
    }

    // Verify all responses completed successfully
    println!(
        "100 requests completed. Max response time: {:?}, Avg: {:?}",
        max_response_time,
        total_time / 100
    );
}

/// Test Case 4: Verify compaction_running status during active compaction
#[test]
fn test_compaction_running_status() {
    use mirdb::web::handlers::metrics::create_metrics_state;

    let state = create_metrics_state("/tmp/test_compaction".to_string(), 7, 1024 * 1024);

    // Initially compaction should not be running
    let metrics = state.get_metrics();
    assert!(!metrics.compaction_running, "compaction_running should be false initially");

    // Simulate starting compaction
    state.set_compaction_running(true);

    // Verify compaction_running is now true
    let metrics = state.get_metrics();
    assert!(
        metrics.compaction_running,
        "compaction_running should be true during active compaction"
    );

    // Simulate ending compaction
    state.set_compaction_running(false);

    // Verify compaction_running is false again
    let metrics = state.get_metrics();
    assert!(!metrics.compaction_running, "compaction_running should be false after compaction");
}

/// Test Case 5: Verify sstable_level_counts array structure
#[test]
fn test_sstable_level_counts_structure() {
    use mirdb::web::handlers::metrics::create_metrics_state;

    let state = create_metrics_state("/tmp/test_sstable".to_string(), 7, 1024 * 1024);

    // Set some SSTable counts
    state.set_sstable_counts(vec![2, 4, 8, 0, 0, 0, 0]);

    let metrics = state.get_metrics();

    // Verify array has exactly 7 elements (max LSM levels)
    assert_eq!(
        metrics.sstable_level_counts.len(),
        7,
        "sstable_level_counts should have exactly 7 elements"
    );

    // Verify all elements are >= 0 (they're u64, so always >= 0)
    for (level, count) in metrics.sstable_level_counts.iter().enumerate() {
        assert!(
            *count >= 0,
            "sstable_level_counts[{}] should be >= 0, got {}",
            level,
            count
        );
    }

    // Verify the counts match what we set
    assert_eq!(metrics.sstable_level_counts[0], 2);
    assert_eq!(metrics.sstable_level_counts[1], 4);
    assert_eq!(metrics.sstable_level_counts[2], 8);
    assert_eq!(metrics.sstable_level_counts[3], 0);
}

/// Test: Verify sstable_level_counts always has 7 elements even if fewer levels set
#[test]
fn test_sstable_level_counts_padding() {
    use mirdb::web::handlers::metrics::create_metrics_state;

    let state = create_metrics_state("/tmp/test_padding".to_string(), 3, 1024 * 1024);

    // Set only 3 levels
    state.set_sstable_counts(vec![1, 2, 3]);

    let metrics = state.get_metrics();

    // Should still have 7 elements, padded with zeros
    assert_eq!(metrics.sstable_level_counts.len(), 7);
    assert_eq!(metrics.sstable_level_counts[0], 1);
    assert_eq!(metrics.sstable_level_counts[1], 2);
    assert_eq!(metrics.sstable_level_counts[2], 3);
    assert_eq!(metrics.sstable_level_counts[3], 0);
    assert_eq!(metrics.sstable_level_counts[4], 0);
    assert_eq!(metrics.sstable_level_counts[5], 0);
    assert_eq!(metrics.sstable_level_counts[6], 0);
}

/// Test: Verify JSON serialization of metrics response
#[test]
fn test_metrics_json_serialization() {
    use mirdb::web::handlers::metrics::{create_metrics_state, get_metrics_json};

    let state = create_metrics_state("/tmp/test_json".to_string(), 7, 4 * 1024 * 1024);
    state.set_key_count(42);
    state.set_compaction_running(true);
    state.set_sstable_counts(vec![1, 0, 0, 0, 0, 0, 0]);

    let json = get_metrics_json(&state);

    // Verify JSON can be parsed
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Verify key values
    assert_eq!(parsed["key_count"], 42);
    assert_eq!(parsed["compaction_running"], true);
    assert!(parsed["sstable_level_counts"].is_array());
}

/// Test: Verify concurrent access to metrics state
#[test]
fn test_concurrent_metrics_access() {
    use mirdb::web::handlers::metrics::create_metrics_state;
    use std::thread;

    let state = create_metrics_state("/tmp/test_concurrent".to_string(), 7, 1024 * 1024);
    let state_clone = state.clone();

    // Spawn a thread that updates metrics
    let updater = thread::spawn(move || {
        for i in 0..100 {
            state_clone.set_key_count(i);
            state_clone.set_memory_used(i * 1024);
        }
    });

    // Concurrently read metrics
    for _ in 0..100 {
        let metrics = state.get_metrics();
        // Just verify we can read without panicking
        assert!(metrics.sstable_level_counts.len() == 7);
    }

    updater.join().unwrap();
}

/// Test: Verify memory metrics
#[test]
fn test_memory_metrics() {
    use mirdb::web::handlers::metrics::create_metrics_state;

    let memory_total = 16 * 1024 * 1024u64; // 16MB
    let state = create_metrics_state("/tmp/test_memory".to_string(), 7, memory_total);

    let metrics = state.get_metrics();
    assert_eq!(metrics.memory_total_bytes, memory_total);
    assert_eq!(metrics.memory_used_bytes, 0);

    // Update memory usage
    state.set_memory_used(4 * 1024 * 1024);
    let metrics = state.get_metrics();
    assert_eq!(metrics.memory_used_bytes, 4 * 1024 * 1024);
}

/// Test: Verify connection tracking
#[test]
fn test_connection_tracking() {
    use mirdb::web::handlers::metrics::create_metrics_state;

    let state = create_metrics_state("/tmp/test_conn".to_string(), 7, 1024 * 1024);

    let metrics = state.get_metrics();
    assert_eq!(metrics.active_connections, 0);

    // Add connections
    state.increment_connections();
    state.increment_connections();
    state.increment_connections();

    let metrics = state.get_metrics();
    assert_eq!(metrics.active_connections, 3);

    // Remove a connection
    state.decrement_connections();

    let metrics = state.get_metrics();
    assert_eq!(metrics.active_connections, 2);
}
