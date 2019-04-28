use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use toml;

use crate::error::err;
use crate::error::MyResult;
use crate::error::StatusCode;
use crate::options::{GB, KB, MB, Options, TB};
use crate::parser_util::macros::{digit, IRResult, space, usize_parser};

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
        _ => panic!(format!("unknown size unit {:?}", x)),
    }
}

gen_parser!(
    size_unit_parser<&[u8]>,
    alt!(tag!(b"K") | tag!(b"M") | tag!(b"G") | tag!(b"T"))
);

gen_parser!(
    size_parser<usize>,
    chain!(
        size: usize_parser
            >> unit: size_unit_parser
            >> (to_size_unit(unit) * size)
    )
);

fn parse_size(a: &[u8]) -> MyResult<usize> {
    match size_parser(a) {
        IRResult::Ok(v) => Ok(v.1),
        IRResult::Err(e) => err(StatusCode::ConfigError, e.to_owned()),
        IRResult::Incomplete(_) => err(StatusCode::ConfigError, "incomplete!")
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
}
