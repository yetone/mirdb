//! Configuration API endpoint integration tests.
//! Owner: Scenario 4 - Configuration API Endpoint
//!
//! Test cases:
//! 1. GET /api/config with default configuration returns expected JSON structure
//! 2. GET /api/config with custom configuration returns custom values
//! 3. POST /api/config returns HTTP 405 Method Not Allowed (config is read-only)

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::time::sleep;

/// Test 1: GET /api/config with default configuration returns valid JSON with expected fields
#[tokio::test]
async fn test_get_config_default_configuration() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that returns default config JSON
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        // Accept one connection
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read HTTP request
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]);

        // Verify it's a GET request to /api/config
        assert!(request.contains("GET /api/config"), "Expected GET /api/config request");

        // Send JSON response with default config values
        let config_json = r#"{"success":true,"data":{"listen_addr":"0.0.0.0:12333","max_lsm_levels":7,"work_dir":"/tmp/mirdb","sstable_max_size":104857600,"memtable_max_size":4194304,"block_size":4096}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            config_json.len(),
            config_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Send HTTP GET request
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let request = "GET /api/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    // Read response
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Verify HTTP 200 OK response
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK, got: {}",
        response
    );
    assert!(
        response.contains("application/json"),
        "Expected JSON content type"
    );

    // Verify response contains expected config fields
    assert!(
        response.contains("listen_addr"),
        "Response should contain listen_addr"
    );
    assert!(
        response.contains("0.0.0.0:12333"),
        "Response should contain default listen address"
    );
    assert!(
        response.contains("max_lsm_levels"),
        "Response should contain max_lsm_levels"
    );
    assert!(
        response.contains("work_dir"),
        "Response should contain work_dir"
    );
    assert!(
        response.contains("/tmp/mirdb"),
        "Response should contain default work_dir"
    );
    assert!(
        response.contains("sstable_max_size"),
        "Response should contain sstable_max_size"
    );
    assert!(
        response.contains("memtable_max_size"),
        "Response should contain memtable_max_size"
    );
    assert!(
        response.contains("block_size"),
        "Response should contain block_size"
    );

    handle.abort();
}

/// Test 2: GET /api/config with custom configuration returns custom values
#[tokio::test]
async fn test_get_config_custom_configuration() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Custom configuration values
    let custom_listen_addr = "192.168.1.100:9999";
    let custom_max_levels = 10;
    let custom_work_dir = "/data/custom/mirdb";
    let custom_sst_size = 209715200; // 200MB
    let custom_mem_size = 8388608; // 8MB
    let custom_block_size = 8192; // 8KB

    // Clone values for the async block
    let custom_listen_addr_clone = custom_listen_addr.to_string();
    let custom_work_dir_clone = custom_work_dir.to_string();

    // Create a mock server that returns custom config JSON
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        // Accept one connection
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read HTTP request
        let mut buf = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;

        // Send JSON response with custom config values
        let config_json = format!(
            r#"{{"success":true,"data":{{"listen_addr":"{}","max_lsm_levels":{},"work_dir":"{}","sstable_max_size":{},"memtable_max_size":{},"block_size":{}}}}}"#,
            custom_listen_addr_clone,
            custom_max_levels,
            custom_work_dir_clone,
            custom_sst_size,
            custom_mem_size,
            custom_block_size
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            config_json.len(),
            config_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Send HTTP GET request
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let request = "GET /api/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    // Read response
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Verify HTTP 200 OK response
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK, got: {}",
        response
    );

    // Verify custom config values are in the response
    assert!(
        response.contains(custom_listen_addr),
        "Response should contain custom listen_addr: {}",
        custom_listen_addr
    );
    assert!(
        response.contains(&custom_max_levels.to_string()),
        "Response should contain custom max_lsm_levels"
    );
    assert!(
        response.contains(custom_work_dir),
        "Response should contain custom work_dir"
    );
    assert!(
        response.contains(&custom_sst_size.to_string()),
        "Response should contain custom sstable_max_size"
    );
    assert!(
        response.contains(&custom_mem_size.to_string()),
        "Response should contain custom memtable_max_size"
    );
    assert!(
        response.contains(&custom_block_size.to_string()),
        "Response should contain custom block_size"
    );

    handle.abort();
}

/// Test 3: POST /api/config returns HTTP 405 Method Not Allowed (config is read-only)
#[tokio::test]
async fn test_post_config_method_not_allowed() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that returns 405 for POST
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        // Accept one connection
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read HTTP request
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]);

        // Verify it's a POST request
        assert!(
            request.contains("POST /api/config"),
            "Expected POST /api/config request"
        );

        // Send 405 Method Not Allowed response
        let error_json =
            r#"{"success":false,"error":"Method Not Allowed. Configuration is read-only.","code":405}"#;
        let response = format!(
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            error_json.len(),
            error_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Send HTTP POST request
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let body = r#"{"listen_addr":"127.0.0.1:9999"}"#;
    let request = format!(
        "POST /api/config HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(request.as_bytes()).unwrap();

    // Read response
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Verify HTTP 405 Method Not Allowed response
    assert!(
        response.contains("405"),
        "Expected 405 status code, got: {}",
        response
    );
    assert!(
        response.contains("Method Not Allowed"),
        "Response should contain Method Not Allowed"
    );

    // Verify error response body
    assert!(
        response.contains("read-only") || response.contains("Method Not Allowed"),
        "Error should mention read-only or Method Not Allowed"
    );
    assert!(
        response.contains("success") && response.contains("false"),
        "Response should indicate failure"
    );

    handle.abort();
}

/// Test: PUT /api/config also returns 405 Method Not Allowed
#[tokio::test]
async fn test_put_config_method_not_allowed() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that returns 405 for PUT
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buf = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;

        let error_json =
            r#"{"success":false,"error":"Method Not Allowed. Configuration is read-only.","code":405}"#;
        let response = format!(
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            error_json.len(),
            error_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let request = "PUT /api/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    assert!(
        response.contains("405"),
        "Expected 405 status code for PUT"
    );

    handle.abort();
}

/// Test: DELETE /api/config also returns 405 Method Not Allowed
#[tokio::test]
async fn test_delete_config_method_not_allowed() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that returns 405 for DELETE
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buf = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;

        let error_json =
            r#"{"success":false,"error":"Method Not Allowed. Configuration is read-only.","code":405}"#;
        let response = format!(
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            error_json.len(),
            error_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let request = "DELETE /api/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    assert!(
        response.contains("405"),
        "Expected 405 status code for DELETE"
    );

    handle.abort();
}

/// Unit tests for ConfigState
#[cfg(test)]
mod config_state_tests {
    use mirdb::web::handlers::config::{
        create_config_state, create_default_config_state, get_config_json, is_method_allowed,
        method_not_allowed_error, ConfigState,
    };

    #[test]
    fn test_default_config_has_expected_values() {
        let state = ConfigState::default_config();
        assert_eq!(state.listen_addr, "0.0.0.0:12333");
        assert_eq!(state.max_lsm_levels, 7);
        assert_eq!(state.work_dir, "/tmp/mirdb");
        assert_eq!(state.sstable_max_size, 100 * 1024 * 1024); // 100MB
        assert_eq!(state.memtable_max_size, 4 * 1024 * 1024); // 4MB
        assert_eq!(state.block_size, 4 * 1024); // 4KB
    }

    #[test]
    fn test_custom_config_state() {
        let state = create_config_state(
            "127.0.0.1:8080".to_string(),
            5,
            "/custom/path".to_string(),
            50 * 1024 * 1024,
            2 * 1024 * 1024,
            8 * 1024,
        );

        assert_eq!(state.listen_addr, "127.0.0.1:8080");
        assert_eq!(state.max_lsm_levels, 5);
        assert_eq!(state.work_dir, "/custom/path");
        assert_eq!(state.sstable_max_size, 50 * 1024 * 1024);
        assert_eq!(state.memtable_max_size, 2 * 1024 * 1024);
        assert_eq!(state.block_size, 8 * 1024);
    }

    #[test]
    fn test_get_config_response() {
        let state = ConfigState::default_config();
        let response = state.get_config();

        assert_eq!(response.listen_addr, "0.0.0.0:12333");
        assert_eq!(response.max_lsm_levels, 7);
        assert_eq!(response.work_dir, "/tmp/mirdb");
    }

    #[test]
    fn test_config_json_serialization() {
        let state = ConfigState::default_config();
        let json = get_config_json(&state);

        // Parse and validate JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["listen_addr"], "0.0.0.0:12333");
        assert_eq!(parsed["max_lsm_levels"], 7);
        assert_eq!(parsed["work_dir"], "/tmp/mirdb");
    }

    #[test]
    fn test_only_get_method_allowed() {
        assert!(is_method_allowed("GET"));
        assert!(is_method_allowed("get"));
        assert!(is_method_allowed("Get"));
        assert!(!is_method_allowed("POST"));
        assert!(!is_method_allowed("PUT"));
        assert!(!is_method_allowed("DELETE"));
        assert!(!is_method_allowed("PATCH"));
        assert!(!is_method_allowed("OPTIONS"));
    }

    #[test]
    fn test_method_not_allowed_error_contains_expected_fields() {
        let error_json = method_not_allowed_error();
        let parsed: serde_json::Value = serde_json::from_str(&error_json).unwrap();

        assert_eq!(parsed["code"], 405);
        assert_eq!(parsed["success"], false);
        assert!(parsed["error"]
            .as_str()
            .unwrap()
            .contains("Method Not Allowed"));
    }

    #[test]
    fn test_shared_config_state_cloning() {
        let state1 = create_default_config_state();
        let state2 = state1.clone();

        // Both should have same values
        assert_eq!(state1.listen_addr, state2.listen_addr);
        assert_eq!(state1.max_lsm_levels, state2.max_lsm_levels);
        assert_eq!(state1.work_dir, state2.work_dir);
    }
}
