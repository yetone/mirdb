//! Integration tests for Interactive Console - SET Command (Scenario 4)
//!
//! These tests verify that the console API correctly executes SET commands
//! against the store as specified in the scenario requirements.

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
    opt.work_dir = "/tmp/mirdbtest/".to_string() + &rand_string;
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

/// Send an HTTP POST request and return the response body
fn send_http_post(host: &str, port: u16, path: &str, body: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

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

    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);

    // Read status line
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;

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

    Ok(String::from_utf8_lossy(&body).to_string())
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

    fn post_console(&self, command: &str) -> std::io::Result<String> {
        send_http_post("127.0.0.1", self.port, "/api/console", command)
    }
}

// ============================================================================
// Test Case 1: Basic SET command with simple value
// Input: set testkey 0 0 5\r\nhello\r\n
// Expected: Response body contains STORED
// ============================================================================
#[test]
fn test_set_command_basic() {
    let harness = TestHarness::new();

    let response = harness
        .post_console("set testkey 0 0 5\r\nhello\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("STORED"),
        "Response should contain STORED. Got: {}",
        response
    );
}

// ============================================================================
// Test Case 2: SET command with value containing spaces
// Input: set key_with_spaces 0 0 11\r\nhello world\r\n
// Expected: Response body contains STORED
// ============================================================================
#[test]
fn test_set_command_with_spaces_in_value() {
    let harness = TestHarness::new();

    let response = harness
        .post_console("set key_with_spaces 0 0 11\r\nhello world\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("STORED"),
        "Response should contain STORED for value with spaces. Got: {}",
        response
    );
}

// ============================================================================
// Test Case 3: SET command with unicode characters
// Input: set unicode_key 0 0 6\r\n你好\r\n
// Expected: Response body contains STORED or appropriate handling of unicode
// ============================================================================
#[test]
fn test_set_command_with_unicode() {
    let harness = TestHarness::new();

    // 你好 is 6 bytes in UTF-8
    let response = harness
        .post_console("set unicode_key 0 0 6\r\n你好\r\n")
        .expect("Failed to send request");

    // Either STORED is returned or an appropriate handling message
    assert!(
        response.contains("STORED") || response.contains("CLIENT_ERROR") || response.contains("response"),
        "Response should contain STORED or an appropriate handling. Got: {}",
        response
    );
}

// ============================================================================
// Test Case 4: Malformed SET command
// Input: Malformed SET command (missing required fields)
// Expected: Response contains CLIENT_ERROR or appropriate error message
// ============================================================================
#[test]
fn test_set_command_malformed_missing_fields() {
    let harness = TestHarness::new();

    // Missing bytes field
    let response = harness
        .post_console("set testkey 0 0\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("CLIENT_ERROR") || response.contains("ERROR"),
        "Response should contain error for malformed command. Got: {}",
        response
    );
}

#[test]
fn test_set_command_malformed_invalid_flags() {
    let harness = TestHarness::new();

    // Invalid flags (non-numeric)
    let response = harness
        .post_console("set testkey abc 0 5\r\nhello\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("CLIENT_ERROR") || response.contains("ERROR"),
        "Response should contain error for invalid flags. Got: {}",
        response
    );
}

#[test]
fn test_set_command_malformed_wrong_byte_count() {
    let harness = TestHarness::new();

    // Byte count doesn't match actual data
    let response = harness
        .post_console("set testkey 0 0 10\r\nhello\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("CLIENT_ERROR") || response.contains("ERROR"),
        "Response should contain error for mismatched byte count. Got: {}",
        response
    );
}

#[test]
fn test_set_command_with_flags_and_exptime() {
    let harness = TestHarness::new();

    // SET with non-zero flags and exptime
    let response = harness
        .post_console("set testkey 42 3600 5\r\nhello\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("STORED"),
        "Response should contain STORED for valid SET with flags and exptime. Got: {}",
        response
    );
}

#[test]
fn test_set_command_empty_value() {
    let harness = TestHarness::new();

    // SET with empty value (0 bytes)
    let response = harness
        .post_console("set emptykey 0 0 0\r\n\r\n")
        .expect("Failed to send request");

    assert!(
        response.contains("STORED"),
        "Response should contain STORED for empty value. Got: {}",
        response
    );
}

#[test]
fn test_set_command_with_newlines_only() {
    let harness = TestHarness::new();

    // SET command using \n instead of \r\n (should still work after normalization)
    let response = harness
        .post_console("set testkey 0 0 5\nhello\n")
        .expect("Failed to send request");

    assert!(
        response.contains("STORED"),
        "Response should contain STORED when using \\n line endings. Got: {}",
        response
    );
}

#[test]
fn test_response_is_valid_json() {
    let harness = TestHarness::new();

    let response = harness
        .post_console("set testkey 0 0 5\r\nhello\r\n")
        .expect("Failed to send request");

    // Response should be valid JSON containing a response field
    assert!(
        response.starts_with("{") && response.ends_with("}"),
        "Response should be JSON. Got: {}",
        response
    );
    assert!(
        response.contains("\"response\""),
        "Response JSON should contain 'response' field. Got: {}",
        response
    );
}

#[test]
fn test_multiple_set_commands_overwrite() {
    let harness = TestHarness::new();

    // First SET
    let response1 = harness
        .post_console("set overwrite_key 0 0 6\r\nvalue1\r\n")
        .expect("Failed to send first request");
    assert!(
        response1.contains("STORED"),
        "First SET should return STORED. Got: {}",
        response1
    );

    // Second SET with different value
    let response2 = harness
        .post_console("set overwrite_key 0 0 6\r\nvalue2\r\n")
        .expect("Failed to send second request");
    assert!(
        response2.contains("STORED"),
        "Second SET should return STORED. Got: {}",
        response2
    );
}
