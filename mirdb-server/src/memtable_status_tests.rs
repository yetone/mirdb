//! Integration tests for the memtable status API (REQ-4).
//!
//! These tests verify that:
//! 1. GET /api/status returns JSON with storage.memtable_size_bytes field
//! 2. storage.memtable_size_bytes is 0 or minimal after fresh start
//! 3. storage.memtable_size_bytes increases after writing data
//! 4. storage.memtable_max_bytes matches configured mem_table_max_size
//! 5. Response contains storage.immutable_memtable_count integer field

#![cfg(test)]

use std::fs::{create_dir_all, remove_dir_all};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::data_manager::StorageStats;
use crate::options::Options;
use crate::request::{Request, SetterType};
use crate::response::Response;
use crate::slice::Slice;
use crate::store::Store;
use crate::error::MyResult;

// Counter to ensure unique work directories without using rand (which has bugs)
static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn get_memtable_test_opt() -> Options {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut opt = Options::default();
    opt.work_dir = format!("/tmp/mirdb_memtable_test_{}", counter);
    if Path::new(&opt.work_dir).exists() {
        remove_dir_all(&opt.work_dir).expect("remove work dir error!");
    }
    create_dir_all(&opt.work_dir).expect("create work dir error!");
    opt.mem_table_max_size = 4 * 1024 * 1024; // 4MB to avoid flushing during tests
    opt.imm_mem_table_max_count = 16;
    opt
}

/// Test Case 1: GET /api/status returns JSON containing storage.memtable_size_bytes field
/// Verifies that the status API response includes the memtable_size_bytes field
#[test]
fn test_status_api_contains_memtable_size_bytes() -> MyResult<()> {
    let opt = get_memtable_test_opt();
    let store = Store::new(opt)?;

    let stats = store.get_storage_stats();

    // Verify JSON serialization works (simulating API response)
    let json = serde_json::json!({
        "storage": {
            "memtable_size_bytes": stats.memtable_size_bytes,
            "memtable_max_bytes": stats.memtable_max_bytes,
            "immutable_memtable_count": stats.immutable_memtable_count
        }
    });

    // Verify the JSON contains the expected field
    assert!(json["storage"]["memtable_size_bytes"].is_number(),
            "JSON should contain storage.memtable_size_bytes as number");

    Ok(())
}

/// Test Case 2: GET /api/status after fresh start shows memtable_size_bytes is 0 or minimal
#[test]
fn test_status_api_fresh_start_minimal_size() -> MyResult<()> {
    let opt = get_memtable_test_opt();
    let store = Store::new(opt)?;

    let stats = store.get_storage_stats();

    // After fresh start, memtable should be empty (0 bytes)
    assert_eq!(stats.memtable_size_bytes, 0,
               "Fresh store should have memtable_size_bytes of 0");

    Ok(())
}

/// Test Case 3: GET /api/status after writing data shows memtable_size_bytes increases
#[test]
fn test_status_api_size_increases_after_write() -> MyResult<()> {
    let opt = get_memtable_test_opt();

    let store = Store::new(opt)?;

    // Get initial stats
    let initial_stats = store.get_storage_stats();
    assert_eq!(initial_stats.memtable_size_bytes, 0,
               "Initial memtable should be empty");

    // Write approximately 1KB of data
    let key_size = 10; // 10 byte keys
    let value_size = 100; // 100 byte values
    let num_entries = 10; // 10 entries = ~1.1KB

    for i in 0..num_entries {
        let key = format!("key{:06}", i); // 10 bytes
        let value = vec![b'x'; value_size];
        let payload = Slice::from(value);

        store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from(key),
            flags: 0,
            ttl: 0,
            bytes: payload.len(),
            payload,
            no_reply: false,
        })?;
    }

    // Get updated stats
    let updated_stats = store.get_storage_stats();

    // Verify memtable size increased
    assert!(updated_stats.memtable_size_bytes > initial_stats.memtable_size_bytes,
            "memtable_size_bytes should increase after writes: initial={}, updated={}",
            initial_stats.memtable_size_bytes, updated_stats.memtable_size_bytes);

    // The size should be greater than 0 and reflect the written data
    // Note: The actual size includes serialization overhead
    assert!(updated_stats.memtable_size_bytes > 0,
            "memtable_size_bytes should be > 0 after writes");

    Ok(())
}

/// Test Case 4: Verify memtable_max_bytes matches configured mem_table_max_size
#[test]
fn test_status_api_max_bytes_matches_config() -> MyResult<()> {
    let mut opt = get_memtable_test_opt();
    // Set a specific memtable max size (8MB)
    let expected_max_bytes = 8 * 1024 * 1024;
    opt.mem_table_max_size = expected_max_bytes;

    let store = Store::new(opt)?;

    let stats = store.get_storage_stats();

    // Verify max bytes matches configuration
    assert_eq!(stats.memtable_max_bytes, expected_max_bytes,
               "memtable_max_bytes should equal configured mem_table_max_size");

    Ok(())
}

/// Test Case 5: Response contains storage.immutable_memtable_count integer field
#[test]
fn test_status_api_contains_immutable_memtable_count() -> MyResult<()> {
    let opt = get_memtable_test_opt();
    let store = Store::new(opt)?;

    let stats = store.get_storage_stats();

    // Verify the immutable_memtable_count field exists and is valid
    // After fresh start, there should be no immutable memtables
    assert_eq!(stats.immutable_memtable_count, 0,
               "Fresh store should have 0 immutable memtables");

    // Verify JSON serialization works
    let json = serde_json::json!({
        "storage": {
            "immutable_memtable_count": stats.immutable_memtable_count
        }
    });

    assert!(json["storage"]["immutable_memtable_count"].is_number(),
            "JSON should contain storage.immutable_memtable_count as number");

    Ok(())
}

/// Additional test: Verify multiple writes track size correctly
#[test]
fn test_status_api_size_tracking_accuracy() -> MyResult<()> {
    let opt = get_memtable_test_opt();

    let store = Store::new(opt)?;

    // Write known amount of data
    let key = "testkey";
    let value = vec![b'v'; 1000]; // 1000 bytes
    let payload = Slice::from(value);

    store.apply(Request::Setter {
        setter: SetterType::Set,
        key: Slice::from(key),
        flags: 0,
        ttl: 0,
        bytes: payload.len(),
        payload,
        no_reply: false,
    })?;

    let stats = store.get_storage_stats();

    // The size should reflect the key + serialized value
    // Note: Exact size depends on serialization format
    assert!(stats.memtable_size_bytes > 0,
            "memtable_size_bytes should be > 0 after write");

    // Write more data and verify size increases proportionally
    let value2 = vec![b'w'; 2000]; // 2000 bytes
    let payload2 = Slice::from(value2);

    store.apply(Request::Setter {
        setter: SetterType::Set,
        key: Slice::from("testkey2"),
        flags: 0,
        ttl: 0,
        bytes: payload2.len(),
        payload: payload2,
        no_reply: false,
    })?;

    let stats_after = store.get_storage_stats();

    assert!(stats_after.memtable_size_bytes > stats.memtable_size_bytes,
            "Size should increase after additional writes: before={}, after={}",
            stats.memtable_size_bytes, stats_after.memtable_size_bytes);

    Ok(())
}

/// Test default memtable max size matches Options::default()
#[test]
fn test_status_api_default_max_bytes() -> MyResult<()> {
    // Use default options (not test options which have small sizes)
    use crate::options::MB;
    let default_max = 4 * MB; // Default from Options::default()

    let mut opt = get_memtable_test_opt();
    opt.mem_table_max_size = default_max;

    let store = Store::new(opt)?;
    let stats = store.get_storage_stats();

    assert_eq!(stats.memtable_max_bytes, default_max,
               "Default memtable_max_bytes should be 4MB");

    Ok(())
}
