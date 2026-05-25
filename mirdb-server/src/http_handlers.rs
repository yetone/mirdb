use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::options::Options;
use crate::request::{GetterType, Request, SetterType};
use crate::response::Response;
use crate::slice::Slice;
use crate::store::Store;

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

// ---- Key-Value Operation Handlers (Scenario 16) ----

#[derive(Debug, Deserialize)]
pub struct OpRequest {
    pub op: String,
    pub key: Option<String>,
    pub value: Option<String>,
    #[serde(default)]
    pub flags: Option<u32>,
    #[serde(default)]
    pub exptime: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct OpResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl OpResponse {
    fn ok(value: String, flags: u32) -> Self {
        OpResponse {
            status: "ok".to_string(),
            value: Some(value),
            flags: Some(flags),
            message: None,
        }
    }

    fn stored() -> Self {
        OpResponse {
            status: "stored".to_string(),
            value: None,
            flags: None,
            message: None,
        }
    }

    fn deleted() -> Self {
        OpResponse {
            status: "deleted".to_string(),
            value: None,
            flags: None,
            message: None,
        }
    }

    fn not_found() -> Self {
        OpResponse {
            status: "not_found".to_string(),
            value: None,
            flags: None,
            message: None,
        }
    }

    fn error(message: &str) -> Self {
        OpResponse {
            status: "error".to_string(),
            value: None,
            flags: None,
            message: Some(message.to_string()),
        }
    }
}

pub fn post_operation_handler(store: Arc<Store>, body: &str) -> (u16, String) {
    let req: OpRequest = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => {
            let resp = OpResponse::error(&format!("Invalid JSON: {}", e));
            return (400, serde_json::to_string(&resp).unwrap());
        }
    };

    match req.op.as_str() {
        "set" => handle_set(store, req),
        "get" => handle_get(store, req),
        "delete" => handle_delete(store, req),
        "flush_all" => handle_flush_all(store, req),
        _ => {
            let resp = OpResponse::error("Invalid operation");
            (400, serde_json::to_string(&resp).unwrap())
        }
    }
}

fn handle_set(store: Arc<Store>, req: OpRequest) -> (u16, String) {
    let key = match req.key {
        Some(k) => k,
        None => {
            let resp = OpResponse::error("Missing required field: key");
            return (400, serde_json::to_string(&resp).unwrap());
        }
    };
    let value = match req.value {
        Some(v) => v,
        None => {
            let resp = OpResponse::error("Missing required field: value");
            return (400, serde_json::to_string(&resp).unwrap());
        }
    };
    let flags = req.flags.unwrap_or(0);
    let exptime = req.exptime.unwrap_or(0);
    let payload = Slice::from(value);
    let bytes = payload.len();

    let request = Request::Setter {
        setter: SetterType::Set,
        key: Slice::from(key),
        flags,
        ttl: exptime,
        bytes,
        payload,
        no_reply: false,
    };

    match store.apply(request) {
        Ok(Response::Stored) => {
            let resp = OpResponse::stored();
            (200, serde_json::to_string(&resp).unwrap())
        }
        Ok(_) => {
            let resp = OpResponse::error("Unexpected response from store");
            (500, serde_json::to_string(&resp).unwrap())
        }
        Err(e) => {
            let resp = OpResponse::error(&e.msg);
            (500, serde_json::to_string(&resp).unwrap())
        }
    }
}

fn handle_get(store: Arc<Store>, req: OpRequest) -> (u16, String) {
    let key = match req.key {
        Some(k) => k,
        None => {
            let resp = OpResponse::error("Missing required field: key");
            return (400, serde_json::to_string(&resp).unwrap());
        }
    };

    let request = Request::Getter {
        getter: GetterType::Get,
        keys: vec![Slice::from(key)],
    };

    match store.apply(request) {
        Ok(Response::Get(items)) => {
            if items.is_empty() {
                let resp = OpResponse::not_found();
                (200, serde_json::to_string(&resp).unwrap())
            } else {
                let item = &items[0];
                let value_str = String::from_utf8_lossy(item.data.as_ref()).to_string();
                let resp = OpResponse::ok(value_str, item.flags);
                (200, serde_json::to_string(&resp).unwrap())
            }
        }
        Ok(_) => {
            let resp = OpResponse::error("Unexpected response from store");
            (500, serde_json::to_string(&resp).unwrap())
        }
        Err(e) => {
            let resp = OpResponse::error(&e.msg);
            (500, serde_json::to_string(&resp).unwrap())
        }
    }
}

fn handle_delete(store: Arc<Store>, req: OpRequest) -> (u16, String) {
    let key = match req.key {
        Some(k) => k,
        None => {
            let resp = OpResponse::error("Missing required field: key");
            return (400, serde_json::to_string(&resp).unwrap());
        }
    };

    let request = Request::Deleter {
        key: Slice::from(key),
        no_reply: false,
    };

    match store.apply(request) {
        Ok(Response::Deleted) => {
            let resp = OpResponse::deleted();
            (200, serde_json::to_string(&resp).unwrap())
        }
        Ok(Response::NotFound) => {
            let resp = OpResponse::not_found();
            (200, serde_json::to_string(&resp).unwrap())
        }
        Ok(_) => {
            let resp = OpResponse::error("Unexpected response from store");
            (500, serde_json::to_string(&resp).unwrap())
        }
        Err(e) => {
            let resp = OpResponse::error(&e.msg);
            (500, serde_json::to_string(&resp).unwrap())
        }
    }
}

fn handle_flush_all(store: Arc<Store>, _req: OpRequest) -> (u16, String) {
    match store.flush_all() {
        Ok(()) => {
            let resp = OpResponse {
                status: "ok".to_string(),
                value: None,
                flags: None,
                message: None,
            };
            (200, serde_json::to_string(&resp).unwrap())
        }
        Err(e) => {
            let resp = OpResponse::error(&e.msg);
            (500, serde_json::to_string(&resp).unwrap())
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

    // ---- Integration tests for POST /api/operation (Scenario 16) ----

    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    use crate::http_adapter::start_http_server_on_random_port;
    use crate::options::Options;

    fn make_test_store() -> Arc<Store> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut opt = Options::default();
        opt.work_dir = format!("/tmp/mirdb_http_test_{}", ts);
        let path = std::path::Path::new(&opt.work_dir);
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        std::fs::create_dir_all(path).unwrap();
        // Use a large memtable size to prevent background flushing to SSTables
        // during our tests, so flush_all only needs to clear memtables.
        opt.mem_table_max_size = 1024 * 1024 * 100; // 100MB
        opt.imm_mem_table_max_count = 100;
        opt.thread_sleep_ms = 10000; // Very slow background thread
        Arc::new(Store::new(opt).unwrap())
    }

    fn http_post(port: u16, path: &str, body: &str) -> (u16, String) {
        let addr = format!("127.0.0.1:{}", port);
        let mut stream = TcpStream::connect(&addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

        let request = format!(
            "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            path,
            body.len(),
            body
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(&stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).unwrap();

        let parts: Vec<&str> = status_line.split_whitespace().collect();
        let status_code: u16 = parts.get(1).unwrap_or(&"0").parse().unwrap_or(0);

        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.trim().is_empty() {
                break;
            }
        }

        let mut body = String::new();
        reader.read_to_string(&mut body).unwrap();

        (status_code, body)
    }

    fn http_get(port: u16, path: &str) -> (u16, String) {
        let addr = format!("127.0.0.1:{}", port);
        let mut stream = TcpStream::connect(&addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

        let request = format!(
            "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            path
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(&stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).unwrap();

        let parts: Vec<&str> = status_line.split_whitespace().collect();
        let status_code: u16 = parts.get(1).unwrap_or(&"0").parse().unwrap_or(0);

        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.trim().is_empty() {
                break;
            }
        }

        let mut body = String::new();
        reader.read_to_string(&mut body).unwrap();

        (status_code, body)
    }

    #[test]
    fn test_health_endpoint() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/health");
        assert_eq!(status, 200);
        assert!(body.contains("healthy"), "Expected healthy status, got: {}", body);
    }

    #[test]
    fn test_operation_set_stores_value() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store.clone()).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"set","key":"test","value":"value","flags":0,"exptime":0}"#,
        );
        assert_eq!(status, 200, "SET failed: {}", body);
        assert!(body.contains("stored"), "Expected stored status, got: {}", body);

        // Verify value is retrievable
        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"get","key":"test"}"#,
        );
        assert_eq!(status, 200, "GET failed: {}", body);
        assert!(body.contains("ok"), "Expected ok status, got: {}", body);
        assert!(body.contains("value"), "Expected value in response, got: {}", body);
    }

    #[test]
    fn test_operation_get_nonexistent() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"get","key":"nonexistent"}"#,
        );
        assert_eq!(status, 200, "GET nonexistent failed: {}", body);
        assert!(body.contains("not_found"), "Expected not_found status, got: {}", body);
    }

    #[test]
    fn test_operation_delete_removes_key() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store.clone()).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        // Set a key
        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"set","key":"test","value":"value","flags":0,"exptime":0}"#,
        );
        assert_eq!(status, 200, "SET failed: {}", body);

        // Delete it
        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"delete","key":"test"}"#,
        );
        assert_eq!(status, 200, "DELETE failed: {}", body);
        assert!(body.contains("deleted"), "Expected deleted status, got: {}", body);

        // Verify it's gone
        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"get","key":"test"}"#,
        );
        assert_eq!(status, 200, "GET after delete failed: {}", body);
        assert!(body.contains("not_found"), "Expected not_found after delete, got: {}", body);
    }

    #[test]
    fn test_operation_flush_all_clears_data() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store.clone()).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        // Set multiple keys
        for i in 0..3 {
            let body = format!(
                r#"{{"op":"set","key":"key{}","value":"val{}","flags":0,"exptime":0}}"#,
                i, i
            );
            let (status, resp) = http_post(port, "/api/operation", &body);
            assert_eq!(status, 200, "SET key{} failed: {}", i, resp);
        }

        // Flush all
        let (status, body) = http_post(port, "/api/operation", r#"{"op":"flush_all"}"#);
        assert_eq!(status, 200, "FLUSH_ALL failed: {}", body);
        assert!(body.contains("ok"), "Expected ok status, got: {}", body);

        // Verify all keys are gone
        for i in 0..3 {
            let body = format!(r#"{{"op":"get","key":"key{}"}}"#, i);
            let (status, resp) = http_post(port, "/api/operation", &body);
            assert_eq!(status, 200, "GET key{} after flush failed: {}", i, resp);
            assert!(resp.contains("not_found"), "Expected not_found for key{} after flush, got: {}", i, resp);
        }
    }

    #[test]
    fn test_operation_invalid_op() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"invalid_op","key":"test"}"#,
        );
        assert_eq!(status, 400, "Expected 400 for invalid op, got: {}", status);
        assert!(body.contains("Invalid operation"), "Expected Invalid operation message, got: {}", body);
    }

    #[test]
    fn test_operation_set_missing_value() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"set","key":"test"}"#,
        );
        assert_eq!(status, 400, "Expected 400 for missing value, got: {}", status);
        assert!(body.contains("Missing required field: value"), "Expected missing value message, got: {}", body);
    }

    #[test]
    fn test_operation_get_returns_flags() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store.clone()).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"set","key":"test","value":"value","flags":42,"exptime":0}"#,
        );
        assert_eq!(status, 200, "SET failed: {}", body);

        let (status, body) = http_post(
            port,
            "/api/operation",
            r#"{"op":"get","key":"test"}"#,
        );
        assert_eq!(status, 200, "GET failed: {}", body);
        assert!(body.contains("42"), "Expected flags=42 in response, got: {}", body);
    }

    #[test]
    fn test_404_for_unknown_route() {
        let store = make_test_store();
        let port = start_http_server_on_random_port(store).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/unknown");
        assert_eq!(status, 404, "Expected 404, got: {}", status);
        assert!(body.contains("Not Found"), "Expected Not Found, got: {}", body);
    }
}
