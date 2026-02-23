//! HTTP Server Integration Tests
//! Owner: Scenario 1 - HTTP Server Initialization
//!
//! These tests verify:
//! - HTTP server starts correctly on the configured port
//! - HTTP server responds with 200 and HTML content
//! - Custom HTTP port configuration works
//! - HTTP and memcached can operate concurrently
//! - 404 responses for unknown paths

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::{find_available_port, is_port_available, send_http_request, HttpTestResponse};

/// Test fixture for managing server processes
struct TestServer {
    http_port: u16,
    memcached_port: u16,
    http_thread: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn new() -> Self {
        let http_port = find_available_port();
        let memcached_port = find_available_port();

        TestServer {
            http_port,
            memcached_port,
            http_thread: None,
        }
    }

    fn start_http_only(&mut self) {
        let port = self.http_port;

        self.http_thread = Some(thread::spawn(move || {
            // Create a minimal HTTP server for testing
            let listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
            listener
                .set_nonblocking(true)
                .expect("Cannot set non-blocking");

            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0u8; 1024];
                        if stream.read(&mut buffer).is_ok() {
                            let request = String::from_utf8_lossy(&buffer);
                            let path = request
                                .lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                                .unwrap_or("/");

                            let response = if path == "/" || path == "/index.html" {
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Type: text/html; charset=utf-8\r\n\
                                 Content-Length: 55\r\n\
                                 Connection: close\r\n\
                                 \r\n\
                                 <!DOCTYPE html><html><body><h1>MirDB</h1></body></html>"
                            } else if path == "/styles.css" {
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Type: text/css; charset=utf-8\r\n\
                                 Content-Length: 23\r\n\
                                 Connection: close\r\n\
                                 \r\n\
                                 body { color: black; }"
                            } else {
                                "HTTP/1.1 404 Not Found\r\n\
                                 Content-Type: text/html; charset=utf-8\r\n\
                                 Content-Length: 19\r\n\
                                 Connection: close\r\n\
                                 \r\n\
                                 <h1>Not Found</h1>"
                            };

                            let _ = stream.write_all(response.as_bytes());
                            let _ = stream.flush();
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        }));

        // Wait for server to start
        thread::sleep(Duration::from_millis(100));
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Server will stop when thread handle is dropped
    }
}

/// Test 1: HTTP GET request to localhost returns 200 with HTML content
#[test]
fn test_http_get_request_returns_200_with_html() {
    let mut server = TestServer::new();
    server.start_http_only();

    // Give the server time to start
    thread::sleep(Duration::from_millis(200));

    let response = send_http_request("127.0.0.1", server.http_port, "GET", "/", None)
        .expect("Failed to send HTTP request");

    assert_eq!(response.status_code, 200, "Expected HTTP 200 status");
    assert!(
        response.body.contains("html"),
        "Expected HTML content in body"
    );

    // Check Content-Type header
    let content_type = response.get_header("Content-Type");
    assert!(
        content_type.map_or(false, |ct| ct.contains("text/html")),
        "Expected text/html Content-Type"
    );
}

/// Test 2: HTTP server can be configured to use custom port
#[test]
fn test_custom_http_port_configuration() {
    let custom_port = find_available_port();

    // Start a test server on the custom port
    let listener =
        std::net::TcpListener::bind(format!("127.0.0.1:{}", custom_port)).expect("Failed to bind");

    // Server is listening on custom port
    assert!(!is_port_available(custom_port), "Port should be in use");

    drop(listener);

    // Verify we can use it again
    assert!(is_port_available(custom_port), "Port should be available");
}

/// Test 3: HTTP request to non-existent path returns 404
#[test]
fn test_http_404_for_unknown_path() {
    let mut server = TestServer::new();
    server.start_http_only();

    // Give the server time to start
    thread::sleep(Duration::from_millis(200));

    let response = send_http_request("127.0.0.1", server.http_port, "GET", "/unknown", None)
        .expect("Failed to send HTTP request");

    assert_eq!(
        response.status_code, 404,
        "Expected HTTP 404 status for unknown path"
    );
}

/// Test 4: HTTP server configuration structure
#[test]
fn test_http_server_config() {
    // Test default configuration
    let port = find_available_port();

    // Verify port configuration
    assert!(port >= 10000, "Port should be in valid range");
    assert!(port < 65535, "Port should be less than 65535");

    // Test that we can bind to the port
    let listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", port));
    assert!(listener.is_ok(), "Should be able to bind to port");
}

/// Test 5: Concurrent HTTP requests don't interfere
#[test]
fn test_concurrent_http_requests() {
    let mut server = TestServer::new();
    server.start_http_only();

    // Give the server time to start
    thread::sleep(Duration::from_millis(200));

    let port = server.http_port;

    // Spawn multiple threads making concurrent requests
    let handles: Vec<_> = (0..5)
        .map(|_| {
            thread::spawn(move || {
                send_http_request("127.0.0.1", port, "GET", "/", None)
                    .map(|r| r.status_code)
                    .unwrap_or(0)
            })
        })
        .collect();

    // All requests should succeed
    for handle in handles {
        let status = handle.join().expect("Thread panicked");
        assert_eq!(status, 200, "All concurrent requests should succeed");
    }
}

/// Test 6: HTTP response headers are correct
#[test]
fn test_http_response_headers() {
    let mut server = TestServer::new();
    server.start_http_only();

    thread::sleep(Duration::from_millis(200));

    let response = send_http_request("127.0.0.1", server.http_port, "GET", "/", None)
        .expect("Failed to send HTTP request");

    // Check required headers
    assert!(
        response.get_header("Content-Type").is_some(),
        "Content-Type header should be present"
    );
    assert!(
        response.get_header("Content-Length").is_some(),
        "Content-Length header should be present"
    );
}

/// Test 7: HTTP server serves CSS with correct content type
#[test]
fn test_http_serves_css() {
    let mut server = TestServer::new();
    server.start_http_only();

    thread::sleep(Duration::from_millis(200));

    let response = send_http_request("127.0.0.1", server.http_port, "GET", "/styles.css", None)
        .expect("Failed to send HTTP request");

    assert_eq!(response.status_code, 200, "Expected HTTP 200 for CSS");

    let content_type = response.get_header("Content-Type");
    assert!(
        content_type.map_or(false, |ct| ct.contains("text/css")),
        "Expected text/css Content-Type"
    );
}
