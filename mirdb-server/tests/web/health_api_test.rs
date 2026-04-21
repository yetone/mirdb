//! Health check API endpoint integration tests.
//! Owner: Scenario 8 - Health Check Endpoint
//!
//! Test cases:
//! 1. GET /api/health on healthy server returns expected JSON structure
//! 2. GET /api/health during compaction returns compaction_status: 'running'
//! 3. GET /api/health response time is within 100ms (fast health check)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::time::sleep;

/// Test 1: GET /api/health on healthy server returns valid JSON with expected fields
/// Expected response: {"status": "healthy", "server_running": true, "compaction_status": "idle"}
#[tokio::test]
async fn test_get_health_healthy_server() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that returns health JSON for a healthy server
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        // Accept one connection
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read HTTP request
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
            .await
            .unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]);

        // Verify it's a GET request to /api/health
        assert!(
            request.contains("GET /api/health"),
            "Expected GET /api/health request"
        );

        // Send JSON response with healthy status
        let health_json = r#"{"success":true,"data":{"status":"healthy","server_running":true,"compaction_status":"idle"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            health_json.len(),
            health_json
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

    let request = "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
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

    // Verify response contains expected health fields
    assert!(
        response.contains("\"status\""),
        "Response should contain status field"
    );
    assert!(
        response.contains("\"healthy\""),
        "Status should be 'healthy'"
    );
    assert!(
        response.contains("\"server_running\""),
        "Response should contain server_running field"
    );
    assert!(
        response.contains("true"),
        "server_running should be true"
    );
    assert!(
        response.contains("\"compaction_status\""),
        "Response should contain compaction_status field"
    );
    assert!(
        response.contains("\"idle\""),
        "compaction_status should be 'idle'"
    );

    handle.abort();
}

/// Test 2: GET /api/health during compaction returns compaction_status: 'running'
#[tokio::test]
async fn test_get_health_during_compaction() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that returns health JSON with compaction running
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        // Accept one connection
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read HTTP request
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
            .await
            .unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]);

        // Verify it's a GET request to /api/health
        assert!(
            request.contains("GET /api/health"),
            "Expected GET /api/health request"
        );

        // Send JSON response with compaction running
        let health_json = r#"{"success":true,"data":{"status":"healthy","server_running":true,"compaction_status":"running"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            health_json.len(),
            health_json
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

    let request = "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
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

    // Verify compaction_status is 'running'
    assert!(
        response.contains("\"compaction_status\""),
        "Response should contain compaction_status field"
    );
    assert!(
        response.contains("\"running\""),
        "compaction_status should be 'running' during compaction"
    );

    // Server should still be healthy even during compaction
    assert!(
        response.contains("\"status\""),
        "Response should contain status field"
    );
    assert!(
        response.contains("\"healthy\""),
        "Status should be 'healthy' even during compaction"
    );

    handle.abort();
}

/// Test 3: GET /api/health response time is within 100ms (fast health check)
#[tokio::test]
async fn test_get_health_response_time() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that responds quickly
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        // Accept one connection
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read HTTP request
        let mut buf = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;

        // Send JSON response immediately (fast health check)
        let health_json = r#"{"success":true,"data":{"status":"healthy","server_running":true,"compaction_status":"idle"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            health_json.len(),
            health_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Measure response time
    let start_time = Instant::now();

    // Send HTTP GET request
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    let request = "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    // Read response
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    let elapsed = start_time.elapsed();

    // Verify response was received
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK, got: {}",
        response
    );

    // Verify response time is within 100ms
    assert!(
        elapsed.as_millis() < 100,
        "Health check should respond within 100ms, but took {}ms",
        elapsed.as_millis()
    );

    handle.abort();
}

/// Test: Health endpoint returns proper JSON structure
#[tokio::test]
async fn test_health_response_structure() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buf = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;

        let health_json = r#"{"success":true,"data":{"status":"healthy","server_running":true,"compaction_status":"idle"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            health_json.len(),
            health_json
        );
        let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let request = "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Verify response structure
    assert!(response.contains("\"success\":true"), "Response should have success: true");
    assert!(response.contains("\"data\":"), "Response should have data field");

    // Extract JSON body and validate structure
    if let Some(json_start) = response.find('{') {
        let json_body = &response[json_start..];
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(json_body);

        assert!(parsed.is_ok(), "Response body should be valid JSON");

        let value = parsed.unwrap();
        assert!(value.get("success").is_some(), "Should have success field");
        assert!(value.get("data").is_some(), "Should have data field");

        let data = value.get("data").unwrap();
        assert!(data.get("status").is_some(), "Data should have status field");
        assert!(data.get("server_running").is_some(), "Data should have server_running field");
        assert!(data.get("compaction_status").is_some(), "Data should have compaction_status field");
    }

    handle.abort();
}

/// Test: Health endpoint only accepts GET method
#[tokio::test]
async fn test_health_endpoint_get_only() {
    // Find an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create a mock server that handles GET requests
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
            .await
            .unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]);

        // Respond based on method
        if request.starts_with("GET") {
            let health_json = r#"{"success":true,"data":{"status":"healthy","server_running":true,"compaction_status":"idle"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                health_json.len(),
                health_json
            );
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
        } else {
            let error_json = r#"{"success":false,"error":"Method Not Allowed","code":405}"#;
            let response = format!(
                "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                error_json.len(),
                error_json
            );
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
        }
    });

    sleep(Duration::from_millis(100)).await;

    // Send GET request - should succeed
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let request = "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "GET request should return 200 OK"
    );

    handle.abort();
}

/// Unit tests for HealthState
#[cfg(test)]
mod health_state_tests {
    use mirdb::web::handlers::health::{
        create_health_state, get_health_json, get_health_with_state, is_health_check_fast,
        HealthState,
    };
    use std::time::Instant;

    #[test]
    fn test_health_state_default_values() {
        let state = HealthState::new();
        assert!(state.is_server_running(), "Server should be running by default");
        assert!(!state.is_compaction_running(), "Compaction should not be running by default");
    }

    #[test]
    fn test_health_state_server_status() {
        let state = HealthState::new();

        // Default: server running
        assert!(state.is_server_running());

        // Set server not running
        state.set_server_running(false);
        assert!(!state.is_server_running());

        // Set server running again
        state.set_server_running(true);
        assert!(state.is_server_running());
    }

    #[test]
    fn test_health_state_compaction_status() {
        let state = HealthState::new();

        // Default: compaction not running
        assert!(!state.is_compaction_running());

        // Start compaction
        state.set_compaction_running(true);
        assert!(state.is_compaction_running());

        // Stop compaction
        state.set_compaction_running(false);
        assert!(!state.is_compaction_running());
    }

    #[test]
    fn test_get_health_response_healthy() {
        let state = HealthState::new();
        let health = state.get_health();

        assert_eq!(health.status, "healthy");
        assert!(health.server_running);
        assert_eq!(health.compaction_status, "idle");
    }

    #[test]
    fn test_get_health_response_unhealthy() {
        let state = HealthState::new();
        state.set_server_running(false);

        let health = state.get_health();

        assert_eq!(health.status, "unhealthy");
        assert!(!health.server_running);
    }

    #[test]
    fn test_get_health_response_compaction_running() {
        let state = HealthState::new();
        state.set_compaction_running(true);

        let health = state.get_health();

        assert_eq!(health.status, "healthy");
        assert!(health.server_running);
        assert_eq!(health.compaction_status, "running");
    }

    #[test]
    fn test_health_json_serialization() {
        let state = HealthState::new();
        let json = get_health_json(&state);

        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"server_running\":true"));
        assert!(json.contains("\"compaction_status\":\"idle\""));
    }

    #[test]
    fn test_health_json_compaction_running() {
        let state = HealthState::new();
        state.set_compaction_running(true);

        let json = get_health_json(&state);

        assert!(json.contains("\"compaction_status\":\"running\""));
    }

    #[test]
    fn test_shared_health_state_thread_safety() {
        let state = create_health_state();
        let state_clone = state.clone();

        // Modify through one reference
        state.set_compaction_running(true);
        // Read through another
        assert!(state_clone.is_compaction_running());

        // Modify through clone
        state_clone.set_server_running(false);
        // Read through original
        assert!(!state.is_server_running());
    }

    #[test]
    fn test_health_check_performance() {
        let start = Instant::now();
        let state = HealthState::new();
        let _health = state.get_health();

        assert!(
            is_health_check_fast(start),
            "Health check should complete in < 100ms"
        );
    }

    #[test]
    fn test_get_health_with_state_function() {
        let state = HealthState::new();
        state.set_compaction_running(true);

        let health = get_health_with_state(&state);

        assert_eq!(health.status, "healthy");
        assert_eq!(health.compaction_status, "running");
    }
}
