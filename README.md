## MirDB: A Persistent Key-Value Store with Memcached Protocol

![logo](https://github.com/yetone/mirdb/raw/master/assets/logo.gif)

### STATUS BADGES

[![CircleCI][cc-badge]][cc-url]

 [cc-url]: https://circleci.com/gh/yetone/mirdb
 [cc-badge]: https://atompunk.yetone.fun/github/yetone/mirdb?v=2

## Project Status

### Currently Implemented

* **Server**: Full async server implementation using Tokio runtime
* **Persistence**: Write-ahead logging (WAL) and SSTable-based storage
* **LSM Tree Architecture**: Complete LSM tree implementation with multi-level structure
* **Commands**: Full memcached protocol compatibility (SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND)
* **Compaction**: Both minor and major compaction strategies implemented
* **Memtable**: High-performance skip list implementation for in-memory data
* **Storage Engine**: Efficient SSTable format with block compression

### Roadmap

* [ ] **Raft Consensus**: Distributed deployment with leader election and log replication
* [ ] **Replication**: Master-slave replication for high availability
* [ ] **Monitoring**: Built-in metrics and health check endpoints
* [ ] **Configuration Hot Reload**: Runtime configuration updates without restart
* [ ] **Backup & Restore**: Automated backup tools and point-in-time recovery

### Contributing

We welcome contributions! Please see our [GitHub Issues](https://github.com/yetone/mirdb/issues) for:

* **Bug Reports**: Report any issues you encounter
* **Feature Requests**: Suggest new features or improvements
* **Pull Requests**: Submit code changes and enhancements
* **Development Setup**: Instructions in the `mirdb-server` crate

### USAGE

It is painless as using [memcached](https://github.com/memcached/memcached/blob/master/doc/protocol.txt).

![usage](https://github.com/yetone/mirdb/raw/master/assets/usage.gif)
