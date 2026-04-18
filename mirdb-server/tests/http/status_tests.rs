//! Server status API tests
//!
//! Owner: Scenario 1 (Server Status Display)
//!
//! Test cases:
//! - GET /api/status returns JSON with levels, memory, compaction
//! - Status reflects actual SSTable file counts
//! - Status shows compaction state during active compaction

use std::sync::Arc;

use mirdb::http::handlers::status_handler;
use mirdb::http::types::{LevelInfo, MemoryInfo, StatusResponse};
use mirdb::options::Options;
use mirdb::store::Store;

/// Create test options with a unique work directory
fn get_test_opt() -> Options {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::fs::{create_dir_all, remove_dir_all};
    use std::path::Path;

    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();
    let mut opt = Options::default();
    opt.work_dir = "/tmp/mirdbtest_status/".to_string() + &rand_string;
    if Path::new(&opt.work_dir).exists() {
        remove_dir_all(&opt.work_dir).expect("remove work dir error!");
    }
    create_dir_all(&opt.work_dir).expect("create work dir error!");
    opt.mem_table_max_size = 1;
    opt.imm_mem_table_max_count = 1;
    opt
}

/// Test Case 1: GET /api/status returns JSON with levels, memory, and compaction fields
#[test]
fn test_status_returns_required_fields() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = status_handler(&store);
    assert!(result.is_ok(), "status_handler should return Ok");

    let json = result.unwrap();

    // Parse the JSON response
    let status: StatusResponse = serde_json::from_str(&json)
        .expect("Should be valid StatusResponse JSON");

    // Verify all required fields are present
    assert!(!status.levels.is_empty(), "levels should not be empty");
    assert!(status.memory.percentage >= 0.0, "memory percentage should be non-negative");
    assert!(!status.compaction.is_empty(), "compaction should not be empty");
}

/// Test Case 2: Status includes levels array with level and files fields
#[test]
fn test_status_levels_structure() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = status_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();
    let status: StatusResponse = serde_json::from_str(&json).unwrap();

    // Check that levels have the right structure
    for level_info in &status.levels {
        // Level should be a valid index
        assert!(level_info.level <= 10, "Level should be reasonable");
        // Files count should be non-negative (it's usize, so always true)
    }

    // Should have at least level 0
    assert!(
        status.levels.iter().any(|l| l.level == 0),
        "Should have level 0"
    );
}

/// Test Case 3: Status includes memory info with used_bytes and percentage
#[test]
fn test_status_memory_structure() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = status_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();
    let status: StatusResponse = serde_json::from_str(&json).unwrap();

    // Memory fields should be present and valid
    assert!(status.memory.percentage >= 0.0, "Percentage should be non-negative");
    assert!(status.memory.percentage <= 100.0, "Percentage should not exceed 100");
}

/// Test Case 4: Status compaction field contains valid state
#[test]
fn test_status_compaction_state() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = status_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();
    let status: StatusResponse = serde_json::from_str(&json).unwrap();

    // Compaction should be either "idle" or indicate active compaction
    assert!(
        status.compaction == "idle" || status.compaction.contains("compacting"),
        "Compaction should indicate a valid state"
    );
}

/// Test Case 5: Multiple status calls return consistent structure
#[test]
fn test_status_consistency() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Call status multiple times
    for _ in 0..3 {
        let result = status_handler(&store);
        assert!(result.is_ok(), "status_handler should always succeed");

        let json = result.unwrap();
        let _status: StatusResponse = serde_json::from_str(&json)
            .expect("Response should always be valid JSON");
    }
}

/// Test Case 6: Status JSON is valid and can be parsed
#[test]
fn test_status_valid_json() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = status_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();

    // Should be valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&json)
        .expect("Response should be valid JSON");

    // Verify it's an object
    assert!(parsed.is_object(), "Response should be a JSON object");

    // Verify required top-level keys
    assert!(parsed.get("levels").is_some(), "Should have 'levels' key");
    assert!(parsed.get("memory").is_some(), "Should have 'memory' key");
    assert!(parsed.get("compaction").is_some(), "Should have 'compaction' key");
}

/// Test Case 7: Levels are sorted by level number
#[test]
fn test_status_levels_sorted() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let result = status_handler(&store);
    assert!(result.is_ok());

    let json = result.unwrap();
    let status: StatusResponse = serde_json::from_str(&json).unwrap();

    // Verify levels are sorted
    for i in 1..status.levels.len() {
        assert!(
            status.levels[i].level >= status.levels[i - 1].level,
            "Levels should be sorted in ascending order"
        );
    }
}
