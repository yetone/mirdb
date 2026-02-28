use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use toml;

use crate::error::err;
use crate::error::MyResult;
use crate::error::StatusCode;
use crate::options::{Options, GB, KB, MB, TB};
use crate::parser_util::macros::{digit, space, usize_parser, IRResult};

/// Homepage HTTP server configuration
///
/// NFR-5: enable/disable flag
/// REQ-1: configurable port
#[derive(Debug, Clone, Deserialize)]
pub struct HomepageConfig {
    /// Whether the homepage server is enabled
    #[serde(default = "default_homepage_enabled")]
    pub enabled: bool,
    /// Port for the homepage HTTP server
    #[serde(default = "default_homepage_port")]
    pub port: u16,
    /// Optional filesystem asset override path
    #[serde(default)]
    pub assets_path: Option<PathBuf>,
    /// HTTP request timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Maximum concurrent connections
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_homepage_enabled() -> bool {
    false
}

fn default_homepage_port() -> u16 {
    8080
}

fn default_timeout_secs() -> u64 {
    5
}

fn default_max_connections() -> usize {
    100
}

impl Default for HomepageConfig {
    fn default() -> Self {
        HomepageConfig {
            enabled: default_homepage_enabled(),
            port: default_homepage_port(),
            assets_path: None,
            timeout_secs: default_timeout_secs(),
            max_connections: default_max_connections(),
        }
    }
}

impl HomepageConfig {
    /// Validates the homepage configuration
    pub fn validate(&self) -> MyResult<()> {
        if self.port == 0 {
            return err(StatusCode::ConfigError, "homepage port cannot be 0");
        }
        // Port validation: valid range is 1-65535
        // Since u16 max is 65535, we just check for 0 above
        Ok(())
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

    /// Homepage HTTP server configuration (optional)
    #[serde(default)]
    pub homepage: HomepageConfig,
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

        Ok(())
    }

    #[test]
    fn test_homepage_config_defaults() {
        let homepage = HomepageConfig::default();
        assert_eq!(false, homepage.enabled);
        assert_eq!(8080, homepage.port);
        assert_eq!(5, homepage.timeout_secs);
        assert_eq!(100, homepage.max_connections);
    }

    #[test]
    fn test_homepage_config_enabled() -> MyResult<()> {
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

[homepage]
enabled = true
port = 8080
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(true, config.homepage.enabled);
        assert_eq!(8080, config.homepage.port);
        config.homepage.validate()?;
        Ok(())
    }

    #[test]
    fn test_homepage_config_custom_port() -> MyResult<()> {
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

[homepage]
enabled = true
port = 9000
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(true, config.homepage.enabled);
        assert_eq!(9000, config.homepage.port);
        config.homepage.validate()?;
        Ok(())
    }

    #[test]
    fn test_homepage_config_disabled() -> MyResult<()> {
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

[homepage]
enabled = false
port = 8080
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(false, config.homepage.enabled);
        Ok(())
    }

    #[test]
    fn test_homepage_config_invalid_port_zero() {
        let homepage = HomepageConfig {
            enabled: true,
            port: 0,
            assets_path: None,
            timeout_secs: 5,
            max_connections: 100,
        };
        let result = homepage.validate();
        assert!(result.is_err());
        if let Err(status) = result {
            assert!(status.msg.contains("port"));
        }
    }

    #[test]
    fn test_homepage_config_without_section() -> MyResult<()> {
        // When homepage section is missing, should use defaults
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
        // Should default to disabled
        assert_eq!(false, config.homepage.enabled);
        assert_eq!(8080, config.homepage.port);
        Ok(())
    }
}
