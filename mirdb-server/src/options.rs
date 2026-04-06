use sstable::Options as TableOptions;
use std::path::Path;

pub const KB: usize = 1 << 10;
pub const MB: usize = KB * KB;
pub const GB: usize = KB * MB;
pub const TB: usize = KB * GB;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;

/// HTTP server configuration options
/// Scenario 8: HTTP Server Port Configuration
/// NFR-1: HTTP server must run on a separate port from Memcached protocol (configurable)
#[derive(Clone, Debug)]
pub struct HttpOptions {
    /// Enable or disable the HTTP server (default: true)
    pub enable: bool,
    /// HTTP server port (default: 8080)
    pub port: u16,
    /// HTTP server host/address to bind to (default: "0.0.0.0")
    pub host: String,
    /// Statistics cache TTL in seconds (default: 5)
    pub stats_cache_ttl_seconds: u64,
}

impl Default for HttpOptions {
    fn default() -> Self {
        HttpOptions {
            enable: true,
            port: 8080,
            host: "0.0.0.0".to_string(),
            stats_cache_ttl_seconds: 5,
        }
    }
}

#[derive(Clone)]
pub struct Options {
    pub max_level: usize,
    pub work_dir: String,
    pub sst_max_size: usize,
    pub mem_table_max_size: usize,
    pub mem_table_max_height: usize,
    pub imm_mem_table_max_count: usize,

    pub l0_compaction_trigger: usize,

    pub thread_sleep_ms: usize,

    pub table_opt: TableOptions,

    /// HTTP server options (Scenario 8: HTTP Server Port Configuration)
    pub http: HttpOptions,
}

impl Options {
    pub fn get_table_opt(&self) -> &TableOptions {
        &self.table_opt
    }
}

impl Default for Options {
    fn default() -> Self {
        let mut table_opt = TableOptions::default();
        table_opt.block_size = BLOCK_MAX_SIZE;
        table_opt.block_restart_interval = 16;

        Options {
            max_level: 7,
            work_dir: "/tmp/mirdb".into(),
            sst_max_size: MB * 100,
            mem_table_max_size: MB * 4,
            mem_table_max_height: 1 << 5,
            imm_mem_table_max_count: 1 << 4,

            l0_compaction_trigger: 4,

            thread_sleep_ms: 500,

            table_opt,

            http: HttpOptions::default(),
        }
    }
}
