//! Integration tests for Error Handling and Edge Cases (Scenario 12)
//!
//! These tests verify that the system handles errors and edge cases gracefully:
//! - Malformed HTTP requests
//! - Invalid console commands
//! - Resource limits (large keys, large values)

use std::fs::{create_dir_all, remove_dir_all};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::distributions::Alphanumeric;
use rand::Rng;

use mirdb::http::server::{HttpServer, HttpServerConfig};
use mirdb::options::Options;
use mirdb::store::Store;

/// Get test options with a unique work directory
fn get_test_opt() -> Options {
    let rand_string: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();
    let mut opt = Options::default();
    opt.work_dir = "/tmp/mirdbtest_errhandling/".to_string() + &rand_string;
    if Path::new(&opt.work_dir).exists() {
        remove_dir_all(&opt.work_dir).expect("remove work dir error!");
    }
    create_dir_all(&opt.work_dir).expect("create work dir error!");
    opt.mem_table_max_size = 1;
    opt.imm_mem_table_max_count = 1;
    opt
}

/// Find an available port for testing
fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Send a raw HTTP request and return status code and body
fn send_raw_request(host: &str, port: u16, raw_request: &str) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    stream.write_all(raw_request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);

    // Read status line
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;

    // Parse status code from status line (e.g., "HTTP/1.1 400 Bad Request")
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Read headers
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(len_str) = line.split(':').nth(1) {
                content_length = len_str.trim().parse().unwrap_or(0);
            }
        }
    }

    // Read body
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok((status_code, String::from_utf8_lossy(&body).to_string()))
}

/// Send an HTTP POST request and return status code and response body
fn send_http_post(host: &str, port: u16, path: &str, body: &str) -> std::io::Result<(u16, String)> {
    let request = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}:{}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        path,
        host,
        port,
        body.len(),
        body
    );
    send_raw_request(host, port, &request)
}

/// Send an HTTP POST request with raw bytes and return status code and response body
fn send_http_post_bytes(host: &str, port: u16, path: &str, body: &[u8]) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    let headers = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}:{}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        path,
        host,
        port,
        body.len()
    );

    stream.write_all(headers.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);

    // Read status line
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;

    // Parse status code
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Read headers
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(len_str) = line.split(':').nth(1) {
                content_length = len_str.trim().parse().unwrap_or(0);
            }
        }
    }

    // Read body
    let mut resp_body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut resp_body)?;
    }

    Ok((status_code, String::from_utf8_lossy(&resp_body).to_string()))
}

/// Test harness that sets up the HTTP server and provides request helpers
struct TestHarness {
    port: u16,
    _store: Arc<Store>,
}

impl TestHarness {
    fn new() -> Self {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).expect("Failed to create store"));
        let port = find_available_port();

        let server_store = store.clone();
        let config = HttpServerConfig::new(port).with_bind_address("127.0.0.1");
        let server = HttpServer::new(config, server_store);

        // Start server in a background thread
        thread::spawn(move || {
            let _ = server.run();
        });

        // Give the server time to start
        thread::sleep(Duration::from_millis(100));

        TestHarness {
            port,
            _store: store,
        }
    }

    fn post_console(&self, command: &str) -> std::io::Result<(u16, String)> {
        send_http_post("127.0.0.1", self.port, "/api/console", command)
    }

    fn post_console_bytes(&self, command: &[u8]) -> std::io::Result<(u16, String)> {
        send_http_post_bytes("127.0.0.1", self.port, "/api/console", command)
    }

    fn send_raw(&self, raw_request: &str) -> std::io::Result<(u16, String)> {
        send_raw_request("127.0.0.1", self.port, raw_request)
    }
}

// ============================================================================
// Test Case 1: Malformed HTTP request (invalid headers)
// Input: Malformed HTTP request (invalid headers)
// Expected: HTTP 400 Bad Request response
// ============================================================================

#[test]
fn test_malformed_request_no_path() {
    let harness = TestHarness::new();

    // Send a request with no path (just the method)
    let raw_request = "GET\r\n\r\n";
    let (status_code, _body) = harness
        .send_raw(raw_request)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 400,
        "Malformed request with no path should return 400 Bad Request. Got: {}",
        status_code
    );
}

#[test]
fn test_malformed_request_invalid_method() {
    let harness = TestHarness::new();

    // Send a request with a single word (invalid request line)
    let raw_request = "INVALID\r\n\r\n";
    let (status_code, _body) = harness
        .send_raw(raw_request)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 400,
        "Request with invalid method should return 400 Bad Request. Got: {}",
        status_code
    );
}

#[test]
fn test_malformed_request_empty_request_line() {
    let harness = TestHarness::new();

    // Send just headers with no request line
    let raw_request = "\r\nHost: localhost\r\n\r\n";

    // This might close connection immediately, so we accept either 400 or connection error
    let result = harness.send_raw(raw_request);

    match result {
        Ok((status_code, _)) => {
            // Server responded with an error status
            assert!(
                status_code == 400 || status_code == 0,
                "Empty request line should return 400 or close connection. Got: {}",
                status_code
            );
        }
        Err(_) => {
            // Connection was closed, which is acceptable for empty request
        }
    }
}

#[test]
fn test_malformed_request_truncated_headers() {
    let harness = TestHarness::new();

    // Send request with truncated content-length header
    let raw_request = "POST /api/console HTTP/1.1\r\nContent-Length: abc\r\n\r\ntest";

    let result = harness.send_raw(raw_request);

    // Server should handle gracefully - either return error or process with 0 content-length
    match result {
        Ok((status_code, body)) => {
            // 200 is acceptable if server treats invalid content-length as 0
            // 400 is acceptable if server rejects the malformed header
            assert!(
                status_code == 200 || status_code == 400,
                "Malformed Content-Length should be handled. Got status: {}, body: {}",
                status_code, body
            );
        }
        Err(_) => {
            // Connection error is acceptable for malformed request
        }
    }
}

// ============================================================================
// Test Case 2: POST /api/console with empty body
// Input: POST /api/console with empty body
// Expected: Error response indicating invalid command
// ============================================================================

#[test]
fn test_console_empty_body_returns_error() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Empty body POST should still return 200 with JSON response. Got: {}",
        status_code
    );

    // The response should indicate an error
    assert!(
        body.contains("CLIENT_ERROR") || body.contains("empty command") || body.contains("invalid"),
        "Empty body should return error message indicating invalid command. Got: {}",
        body
    );
}

#[test]
fn test_console_whitespace_only_body() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("   \r\n\t   ")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Whitespace-only POST should return 200 with JSON. Got: {}",
        status_code
    );

    // Should indicate error for empty/whitespace command
    assert!(
        body.contains("CLIENT_ERROR") || body.contains("empty") || body.contains("ERROR"),
        "Whitespace-only body should return error. Got: {}",
        body
    );
}

// ============================================================================
// Test Case 3: POST /api/console with unsupported command (e.g., flush_all)
// Input: POST /api/console with unsupported command (e.g., flush_all)
// Expected: Error response indicating command not supported
// ============================================================================

#[test]
fn test_console_unsupported_command_flush_all() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("flush_all")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Unsupported command should return 200 with JSON error. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "flush_all should return error indicating unsupported command. Got: {}",
        body
    );
}

#[test]
fn test_console_unsupported_command_stats() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("stats")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Unsupported stats command should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "stats command should return error indicating unsupported. Got: {}",
        body
    );
}

#[test]
fn test_console_unsupported_command_version() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("version")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Unsupported version command should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "version command should return error indicating unsupported. Got: {}",
        body
    );
}

#[test]
fn test_console_unsupported_command_quit() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("quit")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Unsupported quit command should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "quit command should return error indicating unsupported. Got: {}",
        body
    );
}

#[test]
fn test_console_unsupported_command_incr() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("incr mykey 1")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Unsupported incr command should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "incr command should return error indicating unsupported. Got: {}",
        body
    );
}

#[test]
fn test_console_unsupported_command_decr() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("decr mykey 1")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Unsupported decr command should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "decr command should return error indicating unsupported. Got: {}",
        body
    );
}

#[test]
fn test_console_random_garbage_command() {
    let harness = TestHarness::new();

    let (status_code, body) = harness
        .post_console("xyznotarealcommand 123 abc")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Random garbage command should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("unsupported"),
        "Unknown command should return error. Got: {}",
        body
    );
}

// ============================================================================
// Test Case 4: Very long key name (> 250 characters)
// Input: Very long key name (> 250 characters)
// Expected: Error response or appropriate handling per memcached spec
// ============================================================================

#[test]
fn test_console_very_long_key_name_251_chars() {
    let harness = TestHarness::new();

    // Memcached spec: keys must be less than 250 characters
    let long_key: String = std::iter::repeat('a').take(251).collect();
    let command = format!("set {} 0 0 5\r\nhello\r\n", long_key);

    let (status_code, body) = harness
        .post_console(&command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Long key request should return 200 with JSON response. Got: {}",
        status_code
    );

    // Should either succeed (implementation-specific) or return an error
    // Both are acceptable behaviors
    assert!(
        body.contains("STORED") || body.contains("CLIENT_ERROR") || body.contains("ERROR"),
        "Long key should either be stored or return error. Got: {}",
        body
    );
}

#[test]
fn test_console_very_long_key_name_500_chars() {
    let harness = TestHarness::new();

    let long_key: String = std::iter::repeat('k').take(500).collect();
    let command = format!("set {} 0 0 5\r\nhello\r\n", long_key);

    let (status_code, body) = harness
        .post_console(&command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Very long key request should return 200 with JSON. Got: {}",
        status_code
    );

    // Either stored or error is acceptable
    assert!(
        body.contains("STORED") || body.contains("CLIENT_ERROR") || body.contains("ERROR"),
        "Very long key should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_very_long_key_name_1000_chars() {
    let harness = TestHarness::new();

    let long_key: String = std::iter::repeat('x').take(1000).collect();
    let command = format!("set {} 0 0 5\r\nhello\r\n", long_key);

    let (status_code, body) = harness
        .post_console(&command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Very long key request should return 200 with JSON. Got: {}",
        status_code
    );

    // Either stored or error is acceptable
    assert!(
        body.contains("STORED") || body.contains("CLIENT_ERROR") || body.contains("ERROR"),
        "Extremely long key should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_get_with_very_long_key() {
    let harness = TestHarness::new();

    let long_key: String = std::iter::repeat('g').take(300).collect();
    let command = format!("get {}", long_key);

    let (status_code, body) = harness
        .post_console(&command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "GET with long key should return 200. Got: {}",
        status_code
    );

    // Should return END (key not found) or error
    assert!(
        body.contains("END") || body.contains("ERROR") || body.contains("CLIENT_ERROR"),
        "GET with long key should return END or error. Got: {}",
        body
    );
}

#[test]
fn test_console_delete_with_very_long_key() {
    let harness = TestHarness::new();

    let long_key: String = std::iter::repeat('d').take(300).collect();
    let command = format!("delete {}", long_key);

    let (status_code, body) = harness
        .post_console(&command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "DELETE with long key should return 200. Got: {}",
        status_code
    );

    // Should return NOT_FOUND (key doesn't exist) or error
    assert!(
        body.contains("NOT_FOUND") || body.contains("ERROR") || body.contains("CLIENT_ERROR"),
        "DELETE with long key should return NOT_FOUND or error. Got: {}",
        body
    );
}

// ============================================================================
// Test Case 5: Request with very large value (> 1MB)
// Input: Request with very large value (> 1MB)
// Expected: Error response or appropriate size limit handling
// ============================================================================

#[test]
fn test_console_large_value_100kb() {
    let harness = TestHarness::new();

    // 100KB value
    let value_size = 100 * 1024;
    let large_value: Vec<u8> = std::iter::repeat(b'v').take(value_size).collect();
    let command = format!("set largekey 0 0 {}\r\n", value_size);

    let mut full_command: Vec<u8> = command.into_bytes();
    full_command.extend_from_slice(&large_value);
    full_command.extend_from_slice(b"\r\n");

    let (status_code, body) = harness
        .post_console_bytes(&full_command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Large value (100KB) request should return 200 with JSON. Got: {}",
        status_code
    );

    // Should either store the value or return an appropriate error
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR"),
        "Large value should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_large_value_500kb() {
    let harness = TestHarness::new();

    // 500KB value
    let value_size = 500 * 1024;
    let large_value: Vec<u8> = std::iter::repeat(b'w').take(value_size).collect();
    let command = format!("set largekey500 0 0 {}\r\n", value_size);

    let mut full_command: Vec<u8> = command.into_bytes();
    full_command.extend_from_slice(&large_value);
    full_command.extend_from_slice(b"\r\n");

    let (status_code, body) = harness
        .post_console_bytes(&full_command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Large value (500KB) request should return 200 with JSON. Got: {}",
        status_code
    );

    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR"),
        "Large value (500KB) should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_large_value_1mb() {
    let harness = TestHarness::new();

    // 1MB value - exactly at common memcached limit
    let value_size = 1024 * 1024;
    let large_value: Vec<u8> = std::iter::repeat(b'x').take(value_size).collect();
    let command = format!("set largekey1mb 0 0 {}\r\n", value_size);

    let mut full_command: Vec<u8> = command.into_bytes();
    full_command.extend_from_slice(&large_value);
    full_command.extend_from_slice(b"\r\n");

    let (status_code, body) = harness
        .post_console_bytes(&full_command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "1MB value request should return 200 with JSON. Got: {}",
        status_code
    );

    // Memcached typically limits values to 1MB, so error is expected
    // But some implementations allow larger values
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR") || body.contains("SERVER_ERROR"),
        "1MB value should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_large_value_over_1mb() {
    let harness = TestHarness::new();

    // 1.5MB value - over common memcached limit
    let value_size = (1024 * 1024) + (512 * 1024);
    let large_value: Vec<u8> = std::iter::repeat(b'y').take(value_size).collect();
    let command = format!("set largekeyover1mb 0 0 {}\r\n", value_size);

    let mut full_command: Vec<u8> = command.into_bytes();
    full_command.extend_from_slice(&large_value);
    full_command.extend_from_slice(b"\r\n");

    let (status_code, body) = harness
        .post_console_bytes(&full_command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Over 1MB value request should return 200 with JSON. Got: {}",
        status_code
    );

    // Either stored or error is acceptable
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR") || body.contains("SERVER_ERROR"),
        "Over 1MB value should be handled appropriately. Got: {}",
        body
    );
}

// ============================================================================
// Additional Error Handling Tests
// ============================================================================

#[test]
fn test_404_for_unknown_path() {
    let harness = TestHarness::new();

    let raw_request = "GET /unknown/path HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let (status_code, body) = harness
        .send_raw(raw_request)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 404,
        "Unknown path should return 404 Not Found. Got: {}",
        status_code
    );

    assert!(
        body.contains("404") || body.contains("Not Found"),
        "404 response should indicate not found. Got: {}",
        body
    );
}

#[test]
fn test_405_method_not_allowed_for_post_on_homepage() {
    let harness = TestHarness::new();

    let raw_request = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
    let (status_code, body) = harness
        .send_raw(raw_request)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 405,
        "POST on homepage should return 405 Method Not Allowed. Got: {}",
        status_code
    );

    assert!(
        body.contains("405") || body.contains("Method Not Allowed"),
        "405 response should indicate method not allowed. Got: {}",
        body
    );
}

#[test]
fn test_405_method_not_allowed_for_delete_on_api() {
    let harness = TestHarness::new();

    let raw_request = "DELETE /api/console HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let (status_code, body) = harness
        .send_raw(raw_request)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 405,
        "DELETE on /api/console should return 405. Got: {}",
        status_code
    );

    assert!(
        body.contains("405") || body.contains("Method Not Allowed"),
        "Response should indicate method not allowed. Got: {}",
        body
    );
}

#[test]
fn test_console_special_characters_in_key() {
    let harness = TestHarness::new();

    // Test key with special characters (some may be disallowed)
    let command = "set my-key_with.special!chars 0 0 5\r\nhello\r\n";
    let (status_code, body) = harness
        .post_console(command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Special char key should return 200 with JSON. Got: {}",
        status_code
    );

    // Either stored or error depending on key validation rules
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR"),
        "Special char key should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_binary_data_in_value() {
    let harness = TestHarness::new();

    // Binary data including null bytes
    let binary_value: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];
    let value_size = binary_value.len();
    let command = format!("set binarykey 0 0 {}\r\n", value_size);

    let mut full_command: Vec<u8> = command.into_bytes();
    full_command.extend_from_slice(&binary_value);
    full_command.extend_from_slice(b"\r\n");

    let (status_code, body) = harness
        .post_console_bytes(&full_command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Binary data should return 200 with JSON. Got: {}",
        status_code
    );

    // Either stored or error - both are valid behaviors
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR") || body.contains("invalid"),
        "Binary data should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_negative_byte_count() {
    let harness = TestHarness::new();

    // Negative byte count should be rejected
    let command = "set negkey 0 0 -5\r\nhello\r\n";
    let (status_code, body) = harness
        .post_console(command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Negative byte count should return 200 with JSON error. Got: {}",
        status_code
    );

    assert!(
        body.contains("ERROR") || body.contains("CLIENT_ERROR") || body.contains("invalid"),
        "Negative byte count should return error. Got: {}",
        body
    );
}

#[test]
fn test_console_very_large_flags_value() {
    let harness = TestHarness::new();

    // Very large flags value (might overflow u32)
    let command = "set flagkey 9999999999 0 5\r\nhello\r\n";
    let (status_code, body) = harness
        .post_console(command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Large flags should return 200 with JSON. Got: {}",
        status_code
    );

    // Should either parse within u32 limits or return error
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR") || body.contains("invalid"),
        "Large flags value should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_very_large_exptime_value() {
    let harness = TestHarness::new();

    // Very large exptime value
    let command = "set exptimekey 0 9999999999 5\r\nhello\r\n";
    let (status_code, body) = harness
        .post_console(command)
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Large exptime should return 200 with JSON. Got: {}",
        status_code
    );

    // Should either handle or return error
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("CLIENT_ERROR") || body.contains("invalid"),
        "Large exptime value should be handled appropriately. Got: {}",
        body
    );
}

#[test]
fn test_console_command_case_sensitivity() {
    let harness = TestHarness::new();

    // Test uppercase command
    let (status_code, body) = harness
        .post_console("SET mykey 0 0 5\r\nhello\r\n")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Uppercase SET should return 200. Got: {}",
        status_code
    );

    // Should work regardless of case (or return consistent error)
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("unsupported"),
        "Uppercase SET should be handled. Got: {}",
        body
    );
}

#[test]
fn test_console_mixed_case_command() {
    let harness = TestHarness::new();

    // Test mixed case command
    let (status_code, body) = harness
        .post_console("SeT mykey 0 0 5\r\nhello\r\n")
        .expect("Failed to send request");

    assert_eq!(
        status_code, 200,
        "Mixed case SeT should return 200. Got: {}",
        status_code
    );

    // Should work if case-insensitive, or error if case-sensitive
    assert!(
        body.contains("STORED") || body.contains("ERROR") || body.contains("unsupported"),
        "Mixed case command should be handled. Got: {}",
        body
    );
}
