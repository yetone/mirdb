use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use toml;

use crate::error::err;
use crate::error::MyResult;
use crate::error::StatusCode;
use crate::options::{Options, GB, KB, MB, TB};
use crate::parser_util::macros::{digit, space, usize_parser, IRResult};

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

    // HTTP server configuration
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_http_bind_address")]
    pub http_bind_address: String,
}

fn default_http_port() -> u16 {
    8080
}

fn default_http_bind_address() -> String {
    "0.0.0.0".to_string()
}

impl Config {
    /// Validate HTTP configuration options
    pub fn validate_http_config(&self) -> MyResult<()> {
        // Validate port range (1-65535)
        if self.http_port == 0 {
            return err(StatusCode::ConfigError, "http_port cannot be 0");
        }
        // Note: u16 max is 65535, so values above that are handled by TOML parsing
        // but we document this for clarity
        Ok(())
    }

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
        Ok(opt)
    }
}

fn to_size_unit(x: &[u8]) -> usize {
    match x {
        b"K" => KB,
        b"M" => MB,
        b"G" => GB,
        b"T" => TB,
        _ => panic!("unknown size unit {:?}", x),
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

/// Parse configuration from a TOML string
pub fn from_str(config_str: &str) -> MyResult<Config> {
    let config: Config = toml::from_str(config_str).map_err(|e| {
        let msg = e.to_string();
        // Provide clear error messages for common configuration errors
        if msg.contains("http_port") && (msg.contains("out of range") || msg.contains("invalid")) {
            crate::error::Status::new(
                StatusCode::ConfigError,
                "Invalid http_port: port must be between 1 and 65535",
            )
        } else {
            crate::error::Status::new(
                StatusCode::ConfigError,
                &format!("Configuration error: {}", msg),
            )
        }
    })?;

    // Validate HTTP configuration
    config.validate_http_config()?;

    Ok(config)
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

    from_str(&config_str)
}

#[cfg(test)]
mod test {
    use super::*;

    /// Base TOML configuration string for testing
    fn base_toml_config() -> String {
        r#"
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
"#.to_string()
    }

    #[test]
    fn test_parse() -> MyResult<()> {
        let toml_str = base_toml_config();

        let config: Config = toml::from_str(&toml_str).unwrap();
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

        Ok(())
    }

    // HTTP Configuration Tests

    #[test]
    fn test_http_config_default_port() -> MyResult<()> {
        // When no http_port is specified, it should default to 8080
        let toml_str = base_toml_config();
        let config = from_str(&toml_str)?;

        assert_eq!(config.http_port, 8080);
        Ok(())
    }

    #[test]
    fn test_http_config_default_bind_address() -> MyResult<()> {
        // When no http_bind_address is specified, it should default to "0.0.0.0"
        let toml_str = base_toml_config();
        let config = from_str(&toml_str)?;

        assert_eq!(config.http_bind_address, "0.0.0.0");
        Ok(())
    }

    #[test]
    fn test_http_config_custom_port() -> MyResult<()> {
        // Config with http_port = 9000
        let toml_str = format!("{}\nhttp_port = 9000", base_toml_config());
        let config = from_str(&toml_str)?;

        assert_eq!(config.http_port, 9000);
        Ok(())
    }

    #[test]
    fn test_http_config_custom_bind_address() -> MyResult<()> {
        // Config with http_bind_address = "127.0.0.1"
        let toml_str = format!("{}\nhttp_bind_address = \"127.0.0.1\"", base_toml_config());
        let config = from_str(&toml_str)?;

        assert_eq!(config.http_bind_address, "127.0.0.1");
        Ok(())
    }

    #[test]
    fn test_http_config_invalid_port_out_of_range() {
        // Config with http_port = 99999 (out of u16 range)
        let toml_str = format!("{}\nhttp_port = 99999", base_toml_config());
        let result = from_str(&toml_str);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, StatusCode::ConfigError);
        // The error message should indicate the port is invalid
        assert!(err.msg.contains("port") || err.msg.contains("http_port") || err.msg.contains("65535"));
    }

    #[test]
    fn test_http_config_port_zero_validation() {
        // Config with http_port = 0 (invalid port)
        let toml_str = format!("{}\nhttp_port = 0", base_toml_config());
        let result = from_str(&toml_str);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, StatusCode::ConfigError);
        assert!(err.msg.contains("http_port cannot be 0") || err.msg.contains("port"));
    }

    #[test]
    fn test_http_config_valid_port_boundary_min() -> MyResult<()> {
        // Config with http_port = 1 (minimum valid port)
        let toml_str = format!("{}\nhttp_port = 1", base_toml_config());
        let config = from_str(&toml_str)?;

        assert_eq!(config.http_port, 1);
        Ok(())
    }

    #[test]
    fn test_http_config_valid_port_boundary_max() -> MyResult<()> {
        // Config with http_port = 65535 (maximum valid port)
        let toml_str = format!("{}\nhttp_port = 65535", base_toml_config());
        let config = from_str(&toml_str)?;

        assert_eq!(config.http_port, 65535);
        Ok(())
    }

    #[test]
    fn test_validate_http_config_zero_port() {
        // Direct validation test for zero port
        let config = Config {
            addr: "0.0.0.0:12333".to_string(),
            max_level: 7,
            work_dir: "/tmp/mirdbs".to_string(),
            sst_max_size: "100M".to_string(),
            mem_table_max_size: "4M".to_string(),
            mem_table_max_height: 32,
            imm_mem_table_max_count: 16,
            block_size: "4K".to_string(),
            block_restart_interval: 16,
            l0_compaction_trigger: 4,
            thread_sleep_ms: 500,
            http_port: 0,
            http_bind_address: "0.0.0.0".to_string(),
        };

        let result = config.validate_http_config();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.msg.contains("http_port cannot be 0"));
    }

    #[test]
    fn test_validate_http_config_valid_port() -> MyResult<()> {
        // Direct validation test for valid port
        let config = Config {
            addr: "0.0.0.0:12333".to_string(),
            max_level: 7,
            work_dir: "/tmp/mirdbs".to_string(),
            sst_max_size: "100M".to_string(),
            mem_table_max_size: "4M".to_string(),
            mem_table_max_height: 32,
            imm_mem_table_max_count: 16,
            block_size: "4K".to_string(),
            block_restart_interval: 16,
            l0_compaction_trigger: 4,
            thread_sleep_ms: 500,
            http_port: 8080,
            http_bind_address: "0.0.0.0".to_string(),
        };

        config.validate_http_config()?;
        Ok(())
    }
}
