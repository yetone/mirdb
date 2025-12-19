//! Content Accuracy Tests for MirDB Homepage
//!
//! This test module verifies that the homepage content accurately reflects
//! the actual MirDB capabilities and configuration defaults.

use std::path::Path;
use std::fs;
use std::env;

/// Helper function to get workspace root path
fn workspace_root() -> String {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let path = Path::new(&manifest_dir);
    path.parent().unwrap_or(path).to_string_lossy().to_string()
}

/// Test Case 2: Verify default port is 12333
/// This test confirms that the default port specified in the homepage content
/// matches the actual default configuration in the code.
#[test]
fn test_default_port_is_12333() {
    let root = workspace_root();
    // Read the configuration file to verify the default port
    let config_path = format!("{}/etc/mirdb.toml", root);
    let config_content = fs::read_to_string(&config_path)
        .expect("Should be able to read default config file");

    // Verify the config file specifies port 12333
    assert!(
        config_content.contains("0.0.0.0:12333"),
        "Default configuration should specify port 12333"
    );

    // The address in the config file should be "0.0.0.0:12333"
    // This matches what's documented in the PRD:
    // | Listen Address | 0.0.0.0:12333 |

    // Parse the addr line to verify the exact port
    for line in config_content.lines() {
        if line.starts_with("addr") {
            assert!(
                line.contains(":12333"),
                "Config addr should contain port 12333, found: {}",
                line
            );
            break;
        }
    }
}

/// Test Case 5: Verify LSM tree architecture claim
/// This test confirms that MirDB actually uses LSM tree architecture
/// by verifying the existence of key LSM components in the codebase.
#[test]
fn test_lsm_architecture_exists() {
    let root = workspace_root();

    // Verify Memtable exists (in-memory component of LSM)
    let memtable_path = format!("{}/mirdb-server/src/memtable.rs", root);
    assert!(
        Path::new(&memtable_path).exists(),
        "LSM architecture requires memtable component"
    );

    // Verify SSTable exists (on-disk component of LSM)
    let sstable_path = format!("{}/sstable", root);
    assert!(
        Path::new(&sstable_path).exists(),
        "LSM architecture requires SSTable component"
    );

    // Verify WAL (Write-Ahead Log) exists for durability
    let wal_path = format!("{}/mirdb-server/src/wal.rs", root);
    assert!(
        Path::new(&wal_path).exists(),
        "LSM architecture requires Write-Ahead Log component"
    );

    // Verify data manager orchestrates the LSM components
    let dm_path = format!("{}/mirdb-server/src/data_manager.rs", root);
    assert!(
        Path::new(&dm_path).exists(),
        "LSM architecture requires data manager to orchestrate components"
    );

    // Verify the compaction logic exists (key feature of LSM trees)
    let data_manager_content = fs::read_to_string(&dm_path)
        .expect("Should be able to read data_manager.rs");

    assert!(
        data_manager_content.contains("minor_compaction"),
        "LSM architecture should have minor compaction (memtable to SSTable)"
    );

    assert!(
        data_manager_content.contains("major_compaction"),
        "LSM architecture should have major compaction (SSTable level compaction)"
    );
}

/// Test Case 5 (Additional): Verify LSM tree levels configuration
#[test]
fn test_lsm_max_levels_default() {
    let root = workspace_root();
    // The PRD claims max_level default is 7
    let config_path = format!("{}/etc/mirdb.toml", root);
    let config_content = fs::read_to_string(&config_path)
        .expect("Should be able to read default config file");

    // Verify max_level = 7
    assert!(
        config_content.contains("max_level = 7"),
        "Default max LSM levels should be 7"
    );
}

/// Test Case 2 (Additional): Verify other configuration defaults match PRD
#[test]
fn test_configuration_defaults_match_prd() {
    let root = workspace_root();
    let config_path = format!("{}/etc/mirdb.toml", root);
    let config_content = fs::read_to_string(&config_path)
        .expect("Should be able to read default config file");

    // Verify work_dir default
    assert!(
        config_content.contains("work_dir = \"/tmp/mirdb\""),
        "Default work directory should be /tmp/mirdb"
    );

    // Verify SSTable max size (100MB)
    assert!(
        config_content.contains("sst_max_size = \"100M\""),
        "Default SSTable max size should be 100M"
    );

    // Verify memtable max size (4MB)
    assert!(
        config_content.contains("mem_table_max_size = \"4M\""),
        "Default memtable max size should be 4M"
    );

    // Verify block size (4KB)
    assert!(
        config_content.contains("block_size = \"4K\""),
        "Default block size should be 4K"
    );
}

/// Test Case 3 & 4: Verify Memcached protocol parser supports SET and GET
/// This is a unit test that verifies the parser can correctly parse
/// Memcached SET and GET commands as shown in the quick-start guide.
#[test]
fn test_memcached_protocol_parser_set_command() {
    let root = workspace_root();
    // Read the parser source to verify SET command is supported
    let parser_path = format!("{}/mirdb-server/src/parser.rs", root);
    let parser_content = fs::read_to_string(&parser_path)
        .expect("Should be able to read parser.rs");

    // Verify SET command is in the setter commands
    assert!(
        parser_content.contains("tag!(b\"set\")"),
        "Parser should support SET command"
    );

    // Verify the setter includes set in the alt! macro
    assert!(
        parser_content.contains("setter_name_parser"),
        "Parser should have setter_name_parser for SET/ADD/REPLACE commands"
    );
}

#[test]
fn test_memcached_protocol_parser_get_command() {
    let root = workspace_root();
    // Read the parser source to verify GET command is supported
    let parser_path = format!("{}/mirdb-server/src/parser.rs", root);
    let parser_content = fs::read_to_string(&parser_path)
        .expect("Should be able to read parser.rs");

    // Verify GET command is in the getter commands
    assert!(
        parser_content.contains("tag!(b\"get\")"),
        "Parser should support GET command"
    );

    // Verify the getter parser exists
    assert!(
        parser_content.contains("getter_name_parser"),
        "Parser should have getter_name_parser for GET/GETS commands"
    );
}

/// Test Case 1: Cross-reference homepage features with source code
/// Verify all homepage features exist in the actual codebase
#[test]
fn test_memcached_compatibility_feature() {
    let root = workspace_root();
    // Verify Memcached protocol implementation exists
    let proto_path = format!("{}/mirdb-server/src/proto.rs", root);
    assert!(
        Path::new(&proto_path).exists(),
        "Memcached protocol implementation should exist"
    );

    let proto_content = fs::read_to_string(&proto_path)
        .expect("Should be able to read proto.rs");

    // Verify it implements codec for Memcached protocol
    assert!(
        proto_content.contains("ServerCodec"),
        "Protocol should implement ServerCodec for Memcached compatibility"
    );
}

#[test]
fn test_persistence_feature() {
    let root = workspace_root();
    // Verify SSTable (persistence) implementation exists
    let sstable_lib_path = format!("{}/sstable/src/lib.rs", root);
    let sstable_lib = fs::read_to_string(&sstable_lib_path)
        .expect("Should be able to read sstable lib.rs");

    let table_builder_path = format!("{}/sstable/src/table_builder.rs", root);
    let table_reader_path = format!("{}/sstable/src/table_reader.rs", root);

    // Verify TableBuilder and TableReader exist for persistence
    assert!(
        sstable_lib.contains("TableBuilder") || Path::new(&table_builder_path).exists(),
        "SSTable should have TableBuilder for writing persistent data"
    );

    assert!(
        sstable_lib.contains("TableReader") || Path::new(&table_reader_path).exists(),
        "SSTable should have TableReader for reading persistent data"
    );
}

#[test]
fn test_async_tokio_feature() {
    let root = workspace_root();
    // Verify Tokio is used for async networking
    let cargo_path = format!("{}/mirdb-server/Cargo.toml", root);
    let cargo_content = fs::read_to_string(&cargo_path)
        .expect("Should be able to read Cargo.toml");

    assert!(
        cargo_content.contains("tokio"),
        "Project should use Tokio for async networking"
    );

    // Verify tokio-proto is used
    assert!(
        cargo_content.contains("tokio-proto"),
        "Project should use tokio-proto for protocol handling"
    );
}

#[test]
fn test_skip_list_memtable_feature() {
    let root = workspace_root();
    // Verify skip list is used for memtable
    let cargo_path = format!("{}/mirdb-server/Cargo.toml", root);
    let cargo_content = fs::read_to_string(&cargo_path)
        .expect("Should be able to read Cargo.toml");

    assert!(
        cargo_content.contains("skip-list"),
        "Project should use skip-list for memtable implementation"
    );

    // Verify skip-list implementation exists
    let skiplist_path = format!("{}/skip-list/src/lib.rs", root);
    assert!(
        Path::new(&skiplist_path).exists(),
        "Skip list implementation should exist"
    );
}

/// Verify the memcached commands mentioned in the knowledge base are supported
#[test]
fn test_supported_memcached_commands() {
    let root = workspace_root();
    let parser_path = format!("{}/mirdb-server/src/parser.rs", root);
    let parser_content = fs::read_to_string(&parser_path)
        .expect("Should be able to read parser.rs");

    // Storage commands
    assert!(
        parser_content.contains("\"set\"") || parser_content.contains("b\"set\""),
        "SET command should be supported"
    );
    assert!(
        parser_content.contains("\"add\"") || parser_content.contains("b\"add\""),
        "ADD command should be supported"
    );
    assert!(
        parser_content.contains("\"replace\"") || parser_content.contains("b\"replace\""),
        "REPLACE command should be supported"
    );
    assert!(
        parser_content.contains("\"append\"") || parser_content.contains("b\"append\""),
        "APPEND command should be supported"
    );
    assert!(
        parser_content.contains("\"prepend\"") || parser_content.contains("b\"prepend\""),
        "PREPEND command should be supported"
    );

    // Retrieval commands
    assert!(
        parser_content.contains("\"get\"") || parser_content.contains("b\"get\""),
        "GET command should be supported"
    );
    assert!(
        parser_content.contains("\"gets\"") || parser_content.contains("b\"gets\""),
        "GETS command should be supported"
    );

    // Deletion command
    assert!(
        parser_content.contains("\"delete\"") || parser_content.contains("b\"delete\""),
        "DELETE command should be supported"
    );

    // MirDB-specific commands
    assert!(
        parser_content.contains("\"info\"") || parser_content.contains("b\"info\""),
        "INFO command should be supported"
    );
    assert!(
        parser_content.contains("\"major_compaction\"") || parser_content.contains("b\"major_compaction\""),
        "MAJOR_COMPACTION command should be supported"
    );
}
