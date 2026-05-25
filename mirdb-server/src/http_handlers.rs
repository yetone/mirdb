use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::options::Options;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ServerConfig {
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

impl From<&Options> for ServerConfig {
    fn from(opt: &Options) -> Self {
        ServerConfig {
            addr: "0.0.0.0:12333".to_string(),
            max_level: opt.max_level,
            work_dir: opt.work_dir.clone(),
            sst_max_size: format_size(opt.sst_max_size),
            mem_table_max_size: format_size(opt.mem_table_max_size),
            mem_table_max_height: opt.mem_table_max_height,
            imm_mem_table_max_count: opt.imm_mem_table_max_count,
            block_size: format_size(opt.table_opt.block_size),
            block_restart_interval: opt.table_opt.block_restart_interval,
            l0_compaction_trigger: opt.l0_compaction_trigger,
            thread_sleep_ms: opt.thread_sleep_ms,
        }
    }
}

fn format_size(size: usize) -> String {
    const KB: usize = 1 << 10;
    const MB: usize = KB * KB;
    const GB: usize = KB * MB;

    if size >= GB && size % GB == 0 {
        format!("{}G", size / GB)
    } else if size >= MB && size % MB == 0 {
        format!("{}M", size / MB)
    } else if size >= KB && size % KB == 0 {
        format!("{}K", size / KB)
    } else {
        format!("{}", size)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct HealthStatus {
    pub status: String,
}

pub fn get_config_handler(opt: &Options) -> (u16, String) {
    let config = ServerConfig::from(opt);
    match serde_json::to_string(&config) {
        Ok(json) => (200, json),
        Err(e) => (500, format!("{{\"error\":\"{}\"}}", e)),
    }
}

pub fn get_health_handler(shutting_down: &AtomicBool) -> (u16, String) {
    if shutting_down.load(Ordering::Relaxed) {
        let status = HealthStatus {
            status: "unhealthy".to_string(),
        };
        match serde_json::to_string(&status) {
            Ok(json) => (503, json),
            Err(e) => (503, format!("{{\"error\":\"{}\"}}", e)),
        }
    } else {
        let status = HealthStatus {
            status: "healthy".to_string(),
        };
        match serde_json::to_string(&status) {
            Ok(json) => (200, json),
            Err(e) => (500, format!("{{\"error\":\"{}\"}}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::get_test_opt;

    #[test]
    fn test_get_config_handler() {
        let opt = get_test_opt();
        let (status, body) = get_config_handler(&opt);
        assert_eq!(status, 200);
        let config: ServerConfig = serde_json::from_str(&body).unwrap();
        assert_eq!(config.max_level, opt.max_level);
        assert_eq!(config.work_dir, opt.work_dir);
        assert_eq!(config.mem_table_max_height, opt.mem_table_max_height);
        assert_eq!(config.imm_mem_table_max_count, opt.imm_mem_table_max_count);
        assert_eq!(config.l0_compaction_trigger, opt.l0_compaction_trigger);
        assert_eq!(config.thread_sleep_ms, opt.thread_sleep_ms);
    }

    #[test]
    fn test_get_health_handler_healthy() {
        let flag = AtomicBool::new(false);
        let (status, body) = get_health_handler(&flag);
        assert_eq!(status, 200);
        let health: HealthStatus = serde_json::from_str(&body).unwrap();
        assert_eq!(health.status, "healthy");
    }

    #[test]
    fn test_get_health_handler_unhealthy() {
        let flag = AtomicBool::new(true);
        let (status, body) = get_health_handler(&flag);
        assert_eq!(status, 503);
        let health: HealthStatus = serde_json::from_str(&body).unwrap();
        assert_eq!(health.status, "unhealthy");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(4096), "4K");
        assert_eq!(format_size(4 * 1024 * 1024), "4M");
        assert_eq!(format_size(100 * 1024 * 1024), "100M");
        assert_eq!(format_size(1024 * 1024 * 1024), "1G");
        assert_eq!(format_size(500), "500");
    }
}
