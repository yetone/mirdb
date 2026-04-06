use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use toml;

use crate::error::err;
use crate::error::MyResult;
use crate::error::StatusCode;
use crate::options::{HttpOptions, Options, GB, KB, MB, TB};
use crate::parser_util::macros::{digit, space, usize_parser, IRResult};

/// HTTP server configuration section
/// NFR-1: HTTP server must run on a separate port from Memcached protocol (configurable)
#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    /// Enable or disable the HTTP server (default: true)
    #[serde(default = "default_http_enable")]
    pub enable: bool,
    /// HTTP server port (default: 8080)
    #[serde(default = "default_http_port")]
    pub port: u16,
    /// HTTP server host/address to bind to (default: "0.0.0.0")
    #[serde(default = "default_http_host")]
    pub host: String,
    /// Statistics cache TTL in seconds (default: 5)
    #[serde(default = "default_stats_cache_ttl")]
    pub stats_cache_ttl_seconds: u64,
}

fn default_http_enable() -> bool {
    true
}

fn default_http_port() -> u16 {
    8080
}

fn default_http_host() -> String {
    "0.0.0.0".to_string()
}

fn default_stats_cache_ttl() -> u64 {
    5
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            enable: default_http_enable(),
            port: default_http_port(),
            host: default_http_host(),
            stats_cache_ttl_seconds: default_stats_cache_ttl(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub addr: String,

    pub max_level: usize,
    pub work_dir: String,
    pub sst_max_size: String,
    pub mem_table_max_size: String,
    pub mem_table_max_height: usize,
    pub imm_mem_table_max_count: usize,
    pub block_size: String,
    pub block_restart_interval: usize,

    pub l0_compaction_trigger: usize,

    pub thread_sleep_ms: usize,

    /// HTTP server configuration (optional, uses defaults if not specified)
    #[serde(default)]
    pub http: HttpConfig,
}

impl Config {
    pub fn to_options(&self) -> MyResult<Options> {
        let mut opt = Options::default();
        opt.max_level = self.max_level;
        opt.work_dir = self.work_dir.clone();
        opt.sst_max_size = parse_size(self.sst_max_size.as_bytes())?;
        opt.mem_table_max_size = parse_size(self.mem_table_max_size.as_bytes())?;
        opt.mem_table_max_height = self.mem_table_max_height;
        opt.imm_mem_table_max_count = self.imm_mem_table_max_count;
        opt.table_opt.block_size = parse_size(self.block_size.as_bytes())?;
        opt.table_opt.block_restart_interval = self.block_restart_interval;
        opt.l0_compaction_trigger = self.l0_compaction_trigger;
        opt.thread_sleep_ms = self.thread_sleep_ms;
        opt.http = HttpOptions {
            enable: self.http.enable,
            port: self.http.port,
            host: self.http.host.clone(),
            stats_cache_ttl_seconds: self.http.stats_cache_ttl_seconds,
        };
        Ok(opt)
    }

    /// Extract the Memcached TCP port from the addr string (e.g., "0.0.0.0:12333" -> 12333)
    pub fn get_memcached_port(&self) -> Option<u16> {
        self.addr.split(':').last().and_then(|p| p.parse().ok())
    }

    /// Validate that HTTP and Memcached ports are different (Scenario 8 Test Case 4)
    /// Returns an error if the same port is used for both protocols
    pub fn validate_port_conflict(&self) -> MyResult<()> {
        if !self.http.enable {
            // No conflict if HTTP is disabled
            return Ok(());
        }

        if let Some(memcached_port) = self.get_memcached_port() {
            if self.http.port == memcached_port {
                return err(
                    StatusCode::ConfigError,
                    format!(
                        "HTTP port {} conflicts with Memcached port {}. They must be different.",
                        self.http.port, memcached_port
                    ),
                );
            }
        }
        Ok(())
    }
}

fn to_size_unit(x: &[u8]) -> usize {
    match x {
        b"K" => KB,
        b"M" => MB,
        b"G" => GB,
        b"T" => TB,
        _ => panic!(format!("unknown size unit {:?}", x)),
    }
}

gen_parser!(
    size_unit_parser<&[u8]>,
    alt!(tag!(b"K") | tag!(b"M") | tag!(b"G") | tag!(b"T"))
);

gen_parser!(
    size_parser<usize>,
    chain!(size: usize_parser >> unit: size_unit_parser >> (to_size_unit(unit) * size))
);

fn parse_size(a: &[u8]) -> MyResult<usize> {
    match size_parser(a) {
        IRResult::Ok(v) => Ok(v.1),
        IRResult::Err(e) => err(StatusCode::ConfigError, e.to_owned()),
        IRResult::Incomplete(_) => err(StatusCode::ConfigError, "incomplete!"),
    }
}

pub fn from_path<T: AsRef<Path>>(path: T) -> MyResult<Config> {
    if !path.as_ref().exists() {
        return err(StatusCode::IOError, "cannot found the config file");
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(path.as_ref())?;

    let mut config_str = String::new();
    file.read_to_string(&mut config_str)?;

    let config: Config = toml::from_str(&config_str).unwrap();

    Ok(config)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:12333"

max_level = 7
work_dir = "/tmp/mirdbs"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        println!("{:#?}", config);
        let opt = config.to_options()?;
        assert_eq!(7, opt.max_level);
        assert_eq!("/tmp/mirdbs", opt.work_dir);
        assert_eq!(100 * MB, opt.sst_max_size);
        assert_eq!(4 * MB, opt.mem_table_max_size);
        assert_eq!(32, opt.mem_table_max_height);
        assert_eq!(16, opt.imm_mem_table_max_count);
        assert_eq!(4 * KB, opt.table_opt.block_size);
        assert_eq!(16, opt.table_opt.block_restart_interval);
        assert_eq!(4, opt.l0_compaction_trigger);
        assert_eq!(500, opt.thread_sleep_ms);

        // HTTP defaults should be applied when [http] section is missing
        assert!(opt.http.enable);
        assert_eq!(8080, opt.http.port);
        assert_eq!("0.0.0.0", opt.http.host);
        assert_eq!(5, opt.http.stats_cache_ttl_seconds);

        Ok(())
    }

    // =========================================================================
    // Scenario 8: HTTP Server Port Configuration Tests
    // =========================================================================

    /// Test Case 1: HTTP server uses default port 8080 when not configured
    #[test]
    fn test_http_default_port_8080() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        let opt = config.to_options()?;

        // Verify default HTTP configuration
        assert!(opt.http.enable, "HTTP should be enabled by default");
        assert_eq!(8080, opt.http.port, "HTTP port should default to 8080");
        assert_eq!("0.0.0.0", opt.http.host, "HTTP host should default to 0.0.0.0");

        Ok(())
    }

    /// Test Case 2: HTTP server uses custom port when configured
    #[test]
    fn test_http_custom_port_9000() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[http]
enable = true
port = 9000
host = "127.0.0.1"
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        let opt = config.to_options()?;

        assert!(opt.http.enable, "HTTP should be enabled");
        assert_eq!(9000, opt.http.port, "HTTP port should be 9000");
        assert_eq!("127.0.0.1", opt.http.host, "HTTP host should be 127.0.0.1");

        Ok(())
    }

    /// Test Case 3: HTTP server does not start when enable = false
    #[test]
    fn test_http_disabled() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[http]
enable = false
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        let opt = config.to_options()?;

        assert!(!opt.http.enable, "HTTP should be disabled when enable = false");

        Ok(())
    }

    /// Test Case 4: Port conflict detection - same port for HTTP and Memcached
    #[test]
    fn test_http_port_conflict_detection() {
        let toml_str = r#"
addr = "0.0.0.0:8080"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[http]
enable = true
port = 8080
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        let result = config.validate_port_conflict();

        assert!(result.is_err(), "Should detect port conflict when HTTP and Memcached use same port");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("conflict"), "Error message should mention conflict");
    }

    /// Test that port conflict is not triggered when HTTP is disabled
    #[test]
    fn test_http_port_conflict_ignored_when_disabled() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:8080"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[http]
enable = false
port = 8080
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        // Should not error because HTTP is disabled
        config.validate_port_conflict()?;

        Ok(())
    }

    /// Test that no conflict when ports are different
    #[test]
    fn test_http_no_port_conflict() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[http]
enable = true
port = 8080
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        // Should not error because ports are different
        config.validate_port_conflict()?;

        Ok(())
    }

    /// Test parsing HTTP with stats_cache_ttl_seconds
    #[test]
    fn test_http_stats_cache_ttl() -> MyResult<()> {
        let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500

[http]
enable = true
port = 8080
stats_cache_ttl_seconds = 10
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        let opt = config.to_options()?;

        assert_eq!(10, opt.http.stats_cache_ttl_seconds);

        Ok(())
    }

    /// Test get_memcached_port extraction
    #[test]
    fn test_get_memcached_port() {
        let toml_str = r#"
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(Some(12333), config.get_memcached_port());
    }
}
