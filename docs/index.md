# MirDB Documentation

Welcome to the MirDB documentation hub. This site provides comprehensive information about MirDB's features, architecture, and usage.

## Quick Navigation

- [Commands Documentation](./commands.md) - Complete reference for all Memcached commands supported by MirDB

## About MirDB

MirDB is a persistent key-value store with LSM tree architecture and Memcached protocol compatibility. It provides the simplicity of Memcached with the durability of persistent storage.

### Key Features

- **Memcached Protocol Compatibility**: Use existing Memcached clients without modification
- **Persistent Storage**: Data survives restarts, unlike pure Memcached
- **LSM Tree Architecture**: Efficient write-optimized storage with LevelDB-like performance characteristics
- **Configurable**: Tunable parameters for different workloads

## Getting Started

### Installation

```bash
cargo install mirdb
```

### Basic Usage

1. Start the server:
   ```bash
   mirdb-server
   ```

2. Connect with any Memcached client:
   ```bash
   telnet localhost 12333
   ```

3. Try basic commands:
   ```bash
   SET mykey 0 3600 5
   hello
   GET mykey
   ```

## Architecture

MirDB uses an LSM (Log-Structured Merge) tree storage engine:

1. **Write-Ahead Log (WAL)**: Ensures durability by logging all writes
2. **MemTable**: In-memory sorted structure (skip list) for fast writes
3. **Immutable MemTables**: Frozen memtables ready for flushing
4. **SSTables**: Sorted String Tables stored on disk in levels
5. **Compaction**: Periodically merges SSTables to optimize storage

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/yetone/mirdb).
