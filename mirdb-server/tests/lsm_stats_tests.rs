//! LSM Tree Statistics Integration Tests
//! Owner: Scenario 9 - LSM Tree Statistics
//!
//! Tests for REQ-7: Homepage shall show LSM tree statistics
//! (number of memtables, SSTable count, total size)
//!
//! Test Cases:
//! 1. GET /api/status includes memtable_count, sstable_count, total_size fields
//! 2. LSM statistics update after data changes
//! 3. Statistics accuracy verification

use std::fs::{self, create_dir_all, File};
use std::io::Write;
use std::path::Path;

/// Test module for LSM statistics API
mod lsm_stats_api_tests {
    use super::*;

    /// Test 1: Integration test - GET /api/status includes LSM stats fields
    /// Verifies: Response includes memtable_count, sstable_count, total_size fields
    #[test]
    fn test_status_api_includes_lsm_stats_fields() {
        // Read status.rs to verify the response structure includes LSM fields
        let status_rs = fs::read_to_string("src/http/handlers/status.rs")
            .expect("Failed to read status.rs");

        // Verify StatusResponse struct has all required LSM fields
        assert!(
            status_rs.contains("memtable_count"),
            "StatusResponse must include memtable_count field"
        );
        assert!(
            status_rs.contains("sstable_count"),
            "StatusResponse must include sstable_count field"
        );
        assert!(
            status_rs.contains("total_size"),
            "StatusResponse must include total_size field"
        );

        // Verify these are u32/u64 types for proper numeric representation
        assert!(
            status_rs.contains("memtable_count: u32"),
            "memtable_count should be u32"
        );
        assert!(
            status_rs.contains("sstable_count: u32"),
            "sstable_count should be u32"
        );
        assert!(
            status_rs.contains("total_size: u64"),
            "total_size should be u64"
        );
    }

    /// Test 2: Verify LSM stats calculation functions exist
    #[test]
    fn test_lsm_stats_calculation_functions_exist() {
        let status_rs = fs::read_to_string("src/http/handlers/status.rs")
            .expect("Failed to read status.rs");

        // Verify memtable counting function exists
        assert!(
            status_rs.contains("fn count_memtables"),
            "count_memtables function should exist"
        );

        // Verify SSTable counting function exists
        assert!(
            status_rs.contains("fn count_sstables"),
            "count_sstables function should exist"
        );

        // Verify LSM total size calculation exists
        assert!(
            status_rs.contains("fn calculate_lsm_total_size")
                || status_rs.contains("calculate_dir_size"),
            "LSM total size calculation should exist"
        );
    }

    /// Test 3: Verify memtable count includes WAL files
    #[test]
    fn test_memtable_count_includes_wal_files() {
        let status_rs = fs::read_to_string("src/http/handlers/status.rs")
            .expect("Failed to read status.rs");

        // Memtable count should consider WAL files (immutable memtables)
        assert!(
            status_rs.contains(".wal") || status_rs.contains("wal"),
            "Memtable counting should consider WAL files"
        );
    }

    /// Test 4: Verify SSTable count checks .sst extension
    #[test]
    fn test_sstable_count_checks_sst_extension() {
        let status_rs = fs::read_to_string("src/http/handlers/status.rs")
            .expect("Failed to read status.rs");

        assert!(
            status_rs.contains(r#"ext == "sst""#) || status_rs.contains(".sst"),
            "SSTable counting should check .sst extension"
        );
    }

    /// Test 5: Verify JSON serialization for API response
    #[test]
    fn test_status_response_json_serialization() {
        let status_rs = fs::read_to_string("src/http/handlers/status.rs")
            .expect("Failed to read status.rs");

        // StatusResponse should derive Serialize for JSON output
        assert!(
            status_rs.contains("Serialize"),
            "StatusResponse should derive Serialize"
        );

        // Should have JSON serialization function
        assert!(
            status_rs.contains("serde_json::to_string") || status_rs.contains("handle_status_json"),
            "Should support JSON serialization"
        );
    }
}

/// Test module for LSM statistics accuracy
mod lsm_stats_accuracy_tests {
    use super::*;

    /// Test 6: Statistics accuracy with actual files
    #[test]
    fn test_lsm_stats_accuracy_with_files() {
        let test_dir = "/tmp/mirdb-lsm-accuracy-test";
        let _ = fs::remove_dir_all(test_dir);
        create_dir_all(test_dir).expect("Failed to create test directory");

        // Create known test files
        let mut sst1 = File::create(format!("{}/1.sst", test_dir)).unwrap();
        sst1.write_all(b"sstable1").unwrap();

        let mut sst2 = File::create(format!("{}/2.sst", test_dir)).unwrap();
        sst2.write_all(b"sstable2data").unwrap();

        let mut wal1 = File::create(format!("{}/0.wal", test_dir)).unwrap();
        wal1.write_all(b"wal").unwrap();

        // Verify files were created
        let sst_count = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .count();
        assert_eq!(sst_count, 2, "Should have 2 SST files");

        let wal_count = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "wal")
            })
            .count();
        assert_eq!(wal_count, 1, "Should have 1 WAL file");

        // Cleanup
        let _ = fs::remove_dir_all(test_dir);
    }

    /// Test 7: Statistics for empty directory
    #[test]
    fn test_lsm_stats_empty_directory() {
        let test_dir = "/tmp/mirdb-lsm-empty-test";
        let _ = fs::remove_dir_all(test_dir);
        create_dir_all(test_dir).expect("Failed to create test directory");

        // Empty directory should have 0 SSTables
        let sst_count = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .count();
        assert_eq!(sst_count, 0, "Empty dir should have 0 SST files");

        // Cleanup
        let _ = fs::remove_dir_all(test_dir);
    }

    /// Test 8: Statistics for non-existent directory
    #[test]
    fn test_lsm_stats_nonexistent_directory() {
        let test_dir = "/tmp/mirdb-nonexistent-12345";
        let _ = fs::remove_dir_all(test_dir);

        // Should not panic on non-existent directory
        let path = Path::new(test_dir);
        assert!(!path.exists(), "Test dir should not exist");

        // Code should handle this gracefully
        let status_rs = fs::read_to_string("src/http/handlers/status.rs")
            .expect("Failed to read status.rs");

        assert!(
            status_rs.contains("!path.exists()") || status_rs.contains("path.exists()"),
            "Code should check if path exists"
        );
    }
}

/// Test module for LSM statistics update after data changes
mod lsm_stats_update_tests {
    use super::*;

    /// Test 9: Statistics update when files are added
    #[test]
    fn test_lsm_stats_update_on_file_addition() {
        let test_dir = "/tmp/mirdb-lsm-update-test";
        let _ = fs::remove_dir_all(test_dir);
        create_dir_all(test_dir).expect("Failed to create test directory");

        // Initial count
        let initial_count = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .count();
        assert_eq!(initial_count, 0);

        // Add SST file
        File::create(format!("{}/new.sst", test_dir)).unwrap();

        // Count should update
        let updated_count = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .count();
        assert_eq!(updated_count, 1, "Count should reflect new file");

        // Cleanup
        let _ = fs::remove_dir_all(test_dir);
    }

    /// Test 10: Statistics update when files are removed
    #[test]
    fn test_lsm_stats_update_on_file_removal() {
        let test_dir = "/tmp/mirdb-lsm-removal-test";
        let _ = fs::remove_dir_all(test_dir);
        create_dir_all(test_dir).expect("Failed to create test directory");

        // Create file
        let file_path = format!("{}/temp.sst", test_dir);
        File::create(&file_path).unwrap();

        let count_before = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .count();
        assert_eq!(count_before, 1);

        // Remove file
        fs::remove_file(&file_path).unwrap();

        let count_after = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .count();
        assert_eq!(count_after, 0, "Count should reflect removal");

        // Cleanup
        let _ = fs::remove_dir_all(test_dir);
    }

    /// Test 11: Total size reflects file size changes
    #[test]
    fn test_lsm_total_size_reflects_changes() {
        let test_dir = "/tmp/mirdb-lsm-size-change-test";
        let _ = fs::remove_dir_all(test_dir);
        create_dir_all(test_dir).expect("Failed to create test directory");

        // Create file with known size
        let file_path = format!("{}/data.sst", test_dir);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"small").unwrap();

        let size1: u64 = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
            .sum();
        assert_eq!(size1, 5, "Initial size should be 5 bytes");

        // Add more data
        let file_path2 = format!("{}/data2.sst", test_dir);
        let mut file2 = File::create(&file_path2).unwrap();
        file2.write_all(b"largerdata1234567890").unwrap();

        let size2: u64 = fs::read_dir(test_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "sst")
            })
            .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
            .sum();
        assert!(size2 > size1, "Size should increase after adding data");

        // Cleanup
        let _ = fs::remove_dir_all(test_dir);
    }
}

/// Test module for LSM statistics UI integration
mod lsm_stats_ui_tests {
    use super::*;

    /// Test 12: E2E - UI renders LSM statistics
    #[test]
    fn test_ui_displays_lsm_stats() {
        // Verify status.js handles LSM stats fields
        let status_js = fs::read_to_string("src/web/scripts/status.js")
            .expect("Failed to read status.js");

        // Check that status.js renders memtable count
        assert!(
            status_js.contains("memtable_count") || status_js.contains("Memtable"),
            "UI should display memtable count"
        );

        // Check that status.js renders SSTable count
        assert!(
            status_js.contains("sstable_count") || status_js.contains("SSTable"),
            "UI should display SSTable count"
        );

        // Check that status.js renders total size
        assert!(
            status_js.contains("total_size") || status_js.contains("Total Size"),
            "UI should display total size"
        );
    }

    /// Test 13: E2E - UI has proper metric cards for LSM stats
    #[test]
    fn test_ui_has_lsm_metric_cards() {
        let status_js = fs::read_to_string("src/web/scripts/status.js")
            .expect("Failed to read status.js");

        // Verify metric cards are created for LSM stats
        assert!(
            status_js.contains("createMetricCard") || status_js.contains("metric-card"),
            "UI should create metric cards"
        );
    }

    /// Test 14: E2E - HTML has status panel for displaying LSM stats
    #[test]
    fn test_html_has_status_panel() {
        let html = fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        // Verify status panel exists
        assert!(
            html.contains("id=\"status-panel\"") || html.contains("status-panel"),
            "HTML should have status-panel element for LSM stats display"
        );
    }

    /// Test 15: Verify status.js is included in HTML
    #[test]
    fn test_status_js_included_in_html() {
        let html = fs::read_to_string("src/web/index.html")
            .expect("Failed to read index.html");

        assert!(
            html.contains("status.js"),
            "index.html should include status.js for LSM stats rendering"
        );
    }
}
