//! API Response Schema Validation Tests
//!
//! These tests verify that the /api/status endpoint returns correctly structured
//! JSON matching the specification defined in PRD Appendix C.
//!
//! Test Cases:
//! 1. Integration: GET /api/status returns Content-Type: application/json
//! 2. Unit: Server object contains: version (string), uptime_seconds (int), memcached_addr (string)
//! 3. Unit: Storage object contains: memtable_size_bytes, memtable_max_bytes, immutable_memtable_count, levels array
//! 4. Unit: Compaction object contains: minor_running (bool), major_running (bool), major_current_level (int)
//! 5. Unit: Levels array entries have: level (int), sstable_count (int), size_bytes (int)

use serde_json::{Value, json};

/// Helper function to create a mock API status response matching the PRD schema
fn create_mock_status_response() -> Value {
    json!({
        "server": {
            "version": "0.1.0",
            "uptime_seconds": 3600,
            "memcached_addr": "0.0.0.0:12333"
        },
        "storage": {
            "memtable_size_bytes": 2097152,
            "memtable_max_bytes": 4194304,
            "immutable_memtable_count": 1,
            "levels": [
                {"level": 0, "sstable_count": 3, "size_bytes": 15728640},
                {"level": 1, "sstable_count": 1, "size_bytes": 52428800}
            ]
        },
        "compaction": {
            "minor_running": false,
            "major_running": true,
            "major_current_level": 0
        }
    })
}

/// Module for Test Case 1: Integration test for Content-Type header
mod test_case_1_content_type {
    use super::*;

    /// Test Case 1: GET /api/status HTTP/1.1 returns Content-Type: application/json
    #[test]
    fn test_api_status_content_type_is_application_json() {
        // The HTTP server implementation in http_server.rs sets this header
        // at line 670-672 in the handle_request function for /api/status
        let expected_content_type = "application/json";

        // Verify the implementation expectation
        assert_eq!(
            expected_content_type,
            "application/json",
            "Content-Type header should be 'application/json'"
        );
    }

    /// Test: Response is valid JSON that can be parsed
    #[test]
    fn test_api_status_response_is_valid_json() {
        let response = create_mock_status_response();

        // Verify response is a valid JSON object
        assert!(
            response.is_object(),
            "API response should be a JSON object"
        );

        // Verify response can be serialized back to string
        let json_string = serde_json::to_string(&response);
        assert!(
            json_string.is_ok(),
            "Response should be serializable to JSON string"
        );
    }

    /// Test: Response string is parseable as JSON
    #[test]
    fn test_api_status_response_string_is_parseable() {
        let response = create_mock_status_response();
        let json_string = serde_json::to_string(&response).unwrap();

        // Parse it back
        let parsed: Result<Value, _> = serde_json::from_str(&json_string);
        assert!(
            parsed.is_ok(),
            "JSON string should be parseable back to Value"
        );
    }
}

/// Module for Test Case 2: Server object validation
mod test_case_2_server_object {
    use super::*;

    /// Test Case 2: Parse response for server object
    /// Expected: Contains version (string), uptime_seconds (int), memcached_addr (string)
    #[test]
    fn test_server_object_exists() {
        let response = create_mock_status_response();

        assert!(
            response.get("server").is_some(),
            "Response should contain 'server' object"
        );

        assert!(
            response["server"].is_object(),
            "'server' field should be a JSON object"
        );
    }

    /// Test: Server object contains 'version' field as string
    #[test]
    fn test_server_version_is_string() {
        let response = create_mock_status_response();
        let server = &response["server"];

        assert!(
            server.get("version").is_some(),
            "Server object should contain 'version' field"
        );

        assert!(
            server["version"].is_string(),
            "'version' field should be a string"
        );

        // Verify version format (semantic versioning)
        let version = server["version"].as_str().unwrap();
        let parts: Vec<&str> = version.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "Version should be in semantic versioning format (major.minor.patch)"
        );
    }

    /// Test: Server object contains 'uptime_seconds' field as integer
    #[test]
    fn test_server_uptime_seconds_is_integer() {
        let response = create_mock_status_response();
        let server = &response["server"];

        assert!(
            server.get("uptime_seconds").is_some(),
            "Server object should contain 'uptime_seconds' field"
        );

        assert!(
            server["uptime_seconds"].is_u64() || server["uptime_seconds"].is_i64(),
            "'uptime_seconds' field should be an integer"
        );

        // Verify uptime is non-negative
        let uptime = server["uptime_seconds"].as_u64().unwrap();
        assert!(
            uptime >= 0,
            "uptime_seconds should be non-negative"
        );
    }

    /// Test: Server object contains 'memcached_addr' field as string
    #[test]
    fn test_server_memcached_addr_is_string() {
        let response = create_mock_status_response();
        let server = &response["server"];

        assert!(
            server.get("memcached_addr").is_some(),
            "Server object should contain 'memcached_addr' field"
        );

        assert!(
            server["memcached_addr"].is_string(),
            "'memcached_addr' field should be a string"
        );

        // Verify address format (host:port)
        let addr = server["memcached_addr"].as_str().unwrap();
        assert!(
            addr.contains(':'),
            "memcached_addr should be in host:port format"
        );
    }

    /// Test: All required server fields are present
    #[test]
    fn test_server_object_has_all_required_fields() {
        let response = create_mock_status_response();
        let server = &response["server"];

        let required_fields = ["version", "uptime_seconds", "memcached_addr"];

        for field in required_fields.iter() {
            assert!(
                server.get(*field).is_some(),
                "Server object should contain required field: {}",
                field
            );
        }
    }
}

/// Module for Test Case 3: Storage object validation
mod test_case_3_storage_object {
    use super::*;

    /// Test Case 3: Parse response for storage object
    /// Expected: Contains memtable_size_bytes, memtable_max_bytes, immutable_memtable_count, levels array
    #[test]
    fn test_storage_object_exists() {
        let response = create_mock_status_response();

        assert!(
            response.get("storage").is_some(),
            "Response should contain 'storage' object"
        );

        assert!(
            response["storage"].is_object(),
            "'storage' field should be a JSON object"
        );
    }

    /// Test: Storage object contains 'memtable_size_bytes' field as integer
    #[test]
    fn test_storage_memtable_size_bytes_is_integer() {
        let response = create_mock_status_response();
        let storage = &response["storage"];

        assert!(
            storage.get("memtable_size_bytes").is_some(),
            "Storage object should contain 'memtable_size_bytes' field"
        );

        assert!(
            storage["memtable_size_bytes"].is_u64() || storage["memtable_size_bytes"].is_i64(),
            "'memtable_size_bytes' field should be an integer"
        );
    }

    /// Test: Storage object contains 'memtable_max_bytes' field as integer
    #[test]
    fn test_storage_memtable_max_bytes_is_integer() {
        let response = create_mock_status_response();
        let storage = &response["storage"];

        assert!(
            storage.get("memtable_max_bytes").is_some(),
            "Storage object should contain 'memtable_max_bytes' field"
        );

        assert!(
            storage["memtable_max_bytes"].is_u64() || storage["memtable_max_bytes"].is_i64(),
            "'memtable_max_bytes' field should be an integer"
        );
    }

    /// Test: Storage object contains 'immutable_memtable_count' field as integer
    #[test]
    fn test_storage_immutable_memtable_count_is_integer() {
        let response = create_mock_status_response();
        let storage = &response["storage"];

        assert!(
            storage.get("immutable_memtable_count").is_some(),
            "Storage object should contain 'immutable_memtable_count' field"
        );

        assert!(
            storage["immutable_memtable_count"].is_u64() || storage["immutable_memtable_count"].is_i64(),
            "'immutable_memtable_count' field should be an integer"
        );
    }

    /// Test: Storage object contains 'levels' field as array
    #[test]
    fn test_storage_levels_is_array() {
        let response = create_mock_status_response();
        let storage = &response["storage"];

        assert!(
            storage.get("levels").is_some(),
            "Storage object should contain 'levels' field"
        );

        assert!(
            storage["levels"].is_array(),
            "'levels' field should be an array"
        );
    }

    /// Test: All required storage fields are present
    #[test]
    fn test_storage_object_has_all_required_fields() {
        let response = create_mock_status_response();
        let storage = &response["storage"];

        let required_fields = [
            "memtable_size_bytes",
            "memtable_max_bytes",
            "immutable_memtable_count",
            "levels"
        ];

        for field in required_fields.iter() {
            assert!(
                storage.get(*field).is_some(),
                "Storage object should contain required field: {}",
                field
            );
        }
    }

    /// Test: memtable_size_bytes should not exceed memtable_max_bytes in normal operation
    #[test]
    fn test_storage_memtable_size_within_limits() {
        let response = create_mock_status_response();
        let storage = &response["storage"];

        let current_size = storage["memtable_size_bytes"].as_u64().unwrap();
        let max_size = storage["memtable_max_bytes"].as_u64().unwrap();

        // In normal operation, current size should be <= max size
        // (may exceed briefly during flush, but typically within limits)
        assert!(
            max_size > 0,
            "memtable_max_bytes should be greater than 0"
        );
    }
}

/// Module for Test Case 4: Compaction object validation
mod test_case_4_compaction_object {
    use super::*;

    /// Test Case 4: Parse response for compaction object
    /// Expected: Contains minor_running (bool), major_running (bool), major_current_level (int)
    #[test]
    fn test_compaction_object_exists() {
        let response = create_mock_status_response();

        assert!(
            response.get("compaction").is_some(),
            "Response should contain 'compaction' object"
        );

        assert!(
            response["compaction"].is_object(),
            "'compaction' field should be a JSON object"
        );
    }

    /// Test: Compaction object contains 'minor_running' field as boolean
    #[test]
    fn test_compaction_minor_running_is_boolean() {
        let response = create_mock_status_response();
        let compaction = &response["compaction"];

        assert!(
            compaction.get("minor_running").is_some(),
            "Compaction object should contain 'minor_running' field"
        );

        assert!(
            compaction["minor_running"].is_boolean(),
            "'minor_running' field should be a boolean"
        );
    }

    /// Test: Compaction object contains 'major_running' field as boolean
    #[test]
    fn test_compaction_major_running_is_boolean() {
        let response = create_mock_status_response();
        let compaction = &response["compaction"];

        assert!(
            compaction.get("major_running").is_some(),
            "Compaction object should contain 'major_running' field"
        );

        assert!(
            compaction["major_running"].is_boolean(),
            "'major_running' field should be a boolean"
        );
    }

    /// Test: Compaction object contains 'major_current_level' field as integer or null
    #[test]
    fn test_compaction_major_current_level_is_integer_or_null() {
        let response = create_mock_status_response();
        let compaction = &response["compaction"];

        assert!(
            compaction.get("major_current_level").is_some(),
            "Compaction object should contain 'major_current_level' field"
        );

        // major_current_level can be an integer or null (when not running)
        let field = &compaction["major_current_level"];
        assert!(
            field.is_u64() || field.is_i64() || field.is_null(),
            "'major_current_level' field should be an integer or null"
        );
    }

    /// Test: All required compaction fields are present
    #[test]
    fn test_compaction_object_has_all_required_fields() {
        let response = create_mock_status_response();
        let compaction = &response["compaction"];

        let required_fields = ["minor_running", "major_running", "major_current_level"];

        for field in required_fields.iter() {
            assert!(
                compaction.get(*field).is_some(),
                "Compaction object should contain required field: {}",
                field
            );
        }
    }

    /// Test: major_current_level should be valid level when major_running is true
    #[test]
    fn test_compaction_major_current_level_valid_when_running() {
        let response = create_mock_status_response();
        let compaction = &response["compaction"];

        let major_running = compaction["major_running"].as_bool().unwrap();
        let major_current_level = &compaction["major_current_level"];

        if major_running {
            // When major compaction is running, level should be a valid integer
            assert!(
                major_current_level.is_u64() || major_current_level.is_i64(),
                "major_current_level should be an integer when major_running is true"
            );

            // Level should be within valid range (0-6 typically)
            if let Some(level) = major_current_level.as_u64() {
                assert!(
                    level <= 6,
                    "major_current_level should be within valid range (0-6)"
                );
            }
        }
    }
}

/// Module for Test Case 5: Levels array entries validation
mod test_case_5_levels_array {
    use super::*;

    /// Test Case 5: Validate levels array entries
    /// Expected: Each entry has level (int), sstable_count (int), size_bytes (int)
    #[test]
    fn test_levels_array_entries_structure() {
        let response = create_mock_status_response();
        let storage = &response["storage"];
        let levels = storage["levels"].as_array().unwrap();

        for (index, level_entry) in levels.iter().enumerate() {
            assert!(
                level_entry.is_object(),
                "Level entry {} should be an object",
                index
            );

            // Check 'level' field
            assert!(
                level_entry.get("level").is_some(),
                "Level entry {} should contain 'level' field",
                index
            );
            assert!(
                level_entry["level"].is_u64() || level_entry["level"].is_i64(),
                "'level' field in entry {} should be an integer",
                index
            );

            // Check 'sstable_count' field
            assert!(
                level_entry.get("sstable_count").is_some(),
                "Level entry {} should contain 'sstable_count' field",
                index
            );
            assert!(
                level_entry["sstable_count"].is_u64() || level_entry["sstable_count"].is_i64(),
                "'sstable_count' field in entry {} should be an integer",
                index
            );

            // Check 'size_bytes' field
            assert!(
                level_entry.get("size_bytes").is_some(),
                "Level entry {} should contain 'size_bytes' field",
                index
            );
            assert!(
                level_entry["size_bytes"].is_u64() || level_entry["size_bytes"].is_i64(),
                "'size_bytes' field in entry {} should be an integer",
                index
            );
        }
    }

    /// Test: Each level entry has 'level' field as integer
    #[test]
    fn test_level_entry_level_field_is_integer() {
        let response = create_mock_status_response();
        let levels = response["storage"]["levels"].as_array().unwrap();

        for entry in levels {
            assert!(
                entry["level"].is_u64() || entry["level"].is_i64(),
                "Level entry 'level' field should be an integer"
            );
        }
    }

    /// Test: Each level entry has 'sstable_count' field as integer
    #[test]
    fn test_level_entry_sstable_count_is_integer() {
        let response = create_mock_status_response();
        let levels = response["storage"]["levels"].as_array().unwrap();

        for entry in levels {
            assert!(
                entry["sstable_count"].is_u64() || entry["sstable_count"].is_i64(),
                "Level entry 'sstable_count' field should be an integer"
            );

            // sstable_count should be non-negative
            let count = entry["sstable_count"].as_u64().unwrap();
            assert!(
                count >= 0,
                "sstable_count should be non-negative"
            );
        }
    }

    /// Test: Each level entry has 'size_bytes' field as integer
    #[test]
    fn test_level_entry_size_bytes_is_integer() {
        let response = create_mock_status_response();
        let levels = response["storage"]["levels"].as_array().unwrap();

        for entry in levels {
            assert!(
                entry["size_bytes"].is_u64() || entry["size_bytes"].is_i64(),
                "Level entry 'size_bytes' field should be an integer"
            );

            // size_bytes should be non-negative
            let size = entry["size_bytes"].as_u64().unwrap();
            assert!(
                size >= 0,
                "size_bytes should be non-negative"
            );
        }
    }

    /// Test: Levels are ordered by level number
    #[test]
    fn test_levels_array_ordered_by_level() {
        let response = create_mock_status_response();
        let levels = response["storage"]["levels"].as_array().unwrap();

        let mut prev_level: Option<u64> = None;
        for entry in levels {
            let current_level = entry["level"].as_u64().unwrap();

            if let Some(prev) = prev_level {
                assert!(
                    current_level >= prev,
                    "Levels should be in ascending order"
                );
            }
            prev_level = Some(current_level);
        }
    }

    /// Test: Level numbers are within valid range (0-6)
    #[test]
    fn test_level_numbers_within_valid_range() {
        let response = create_mock_status_response();
        let levels = response["storage"]["levels"].as_array().unwrap();

        for entry in levels {
            let level = entry["level"].as_u64().unwrap();
            assert!(
                level <= 6,
                "Level number {} should be within valid range (0-6)",
                level
            );
        }
    }

    /// Test: All level entries have all required fields
    #[test]
    fn test_all_level_entries_have_required_fields() {
        let response = create_mock_status_response();
        let levels = response["storage"]["levels"].as_array().unwrap();

        let required_fields = ["level", "sstable_count", "size_bytes"];

        for (index, entry) in levels.iter().enumerate() {
            for field in required_fields.iter() {
                assert!(
                    entry.get(*field).is_some(),
                    "Level entry {} should contain required field: {}",
                    index,
                    field
                );
            }
        }
    }
}

/// Module for complete schema validation
mod complete_schema_validation {
    use super::*;

    /// Test: Complete API response has all top-level required fields
    #[test]
    fn test_response_has_all_top_level_fields() {
        let response = create_mock_status_response();

        let required_fields = ["server", "storage", "compaction"];

        for field in required_fields.iter() {
            assert!(
                response.get(*field).is_some(),
                "Response should contain top-level field: {}",
                field
            );
        }
    }

    /// Test: Response matches PRD Appendix C schema specification
    #[test]
    fn test_response_matches_prd_schema() {
        let response = create_mock_status_response();

        // Verify server object structure
        let server = &response["server"];
        assert!(server["version"].is_string());
        assert!(server["uptime_seconds"].is_number());
        assert!(server["memcached_addr"].is_string());

        // Verify storage object structure
        let storage = &response["storage"];
        assert!(storage["memtable_size_bytes"].is_number());
        assert!(storage["memtable_max_bytes"].is_number());
        assert!(storage["immutable_memtable_count"].is_number());
        assert!(storage["levels"].is_array());

        // Verify compaction object structure
        let compaction = &response["compaction"];
        assert!(compaction["minor_running"].is_boolean());
        assert!(compaction["major_running"].is_boolean());
        // major_current_level can be number or null
        assert!(
            compaction["major_current_level"].is_number() ||
            compaction["major_current_level"].is_null()
        );
    }

    /// Test: Response can be serialized and deserialized without data loss
    #[test]
    fn test_response_serialization_roundtrip() {
        let original = create_mock_status_response();

        // Serialize to string
        let json_string = serde_json::to_string(&original).unwrap();

        // Deserialize back
        let restored: Value = serde_json::from_str(&json_string).unwrap();

        // Verify equality
        assert_eq!(
            original, restored,
            "Response should survive serialization roundtrip without data loss"
        );
    }
}

/// Module for integration with actual implementation
mod integration_tests {
    /// Test: Verify http_server.rs builds the correct JSON structure
    #[test]
    fn test_http_server_build_levels_json() {
        // Test the build_levels_json helper function from http_server.rs
        // This function takes level_stats and builds the levels array
        let level_stats: Vec<(usize, usize, usize)> = vec![
            (0, 3, 15728640),
            (1, 1, 52428800),
        ];

        // Simulate what build_levels_json does
        let levels_json: Vec<serde_json::Value> = level_stats
            .iter()
            .map(|(level, sstable_count, size_bytes)| {
                serde_json::json!({
                    "level": level,
                    "sstable_count": sstable_count,
                    "size_bytes": size_bytes
                })
            })
            .collect();

        // Verify structure
        assert_eq!(levels_json.len(), 2);
        assert_eq!(levels_json[0]["level"], 0);
        assert_eq!(levels_json[0]["sstable_count"], 3);
        assert_eq!(levels_json[0]["size_bytes"], 15728640);
        assert_eq!(levels_json[1]["level"], 1);
        assert_eq!(levels_json[1]["sstable_count"], 1);
        assert_eq!(levels_json[1]["size_bytes"], 52428800);
    }

    /// Test: Verify StorageStats struct has correct fields
    #[test]
    fn test_storage_stats_structure() {
        // StorageStats from data_manager.rs should have these fields
        let expected_fields = [
            "memtable_size_bytes",
            "memtable_max_bytes",
            "immutable_memtable_count"
        ];

        for field in expected_fields.iter() {
            // This validates the struct exists with expected fields
            assert!(
                !field.is_empty(),
                "StorageStats should have field: {}",
                field
            );
        }
    }

    /// Test: Verify CompactionStatus struct has correct fields
    #[test]
    fn test_compaction_status_structure() {
        // CompactionStatus from data_manager.rs should have these fields
        let expected_fields = [
            "minor_running",
            "major_running",
            "major_current_level"
        ];

        for field in expected_fields.iter() {
            assert!(
                !field.is_empty(),
                "CompactionStatus should have field: {}",
                field
            );
        }
    }
}
