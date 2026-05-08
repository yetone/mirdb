/**
 * Integration tests for the MirDB homepage HTTP endpoint.
 * Owner: Scenario 1 - Homepage HTTP Endpoint
 *
 * Tests HTTP response status, headers, body content, and performance.
 * Uses an HTTP client (reqwest or hyper) to make requests against
 * a test server instance.
 *
 * Expected test categories:
 * - HTTP status code verification
 * - Content-Type header verification
 * - Response body HTML validation
 * - Response time / performance thresholds
 * - Static asset serving
 */

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

/// Find an available ephemeral port for testing
fn find_ephemeral_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Start the HTTP server on an ephemeral port and return the port number
fn start_test_server() -> u16 {
    let port = find_ephemeral_port();
    let addr = format!("127.0.0.1:{}", port);

    mirdb::web_server::start_http_server(addr.parse().unwrap()).unwrap();

    // Give the server a moment to start listening
    thread::sleep(Duration::from_millis(100));

    port
}

/// Send an HTTP GET request and return the raw response
fn http_get(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
        path, port
    );
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn test_homepage_returns_200_ok() {
    let port = start_test_server();
    let response = http_get(port, "/");

    assert!(
        response.contains("HTTP/1.1 200 OK"),
        "Expected HTTP 200 OK, got: {}",
        response.lines().next().unwrap_or("empty response")
    );
}

#[test]
fn test_homepage_content_type_is_text_html() {
    let port = start_test_server();
    let response = http_get(port, "/");

    assert!(
        response.contains("Content-Type: text/html; charset=utf-8"),
        "Expected Content-Type: text/html; charset=utf-8, got headers: {}",
        response.split("\r\n\r\n").next().unwrap_or("")
    );
}

#[test]
fn test_homepage_body_contains_doctype() {
    let port = start_test_server();
    let response = http_get(port, "/");

    let body = response.split("\r\n\r\n").nth(1).unwrap_or("");
    assert!(
        body.starts_with("<!DOCTYPE html>"),
        "Expected body to start with <!DOCTYPE html>, got: {}",
        &body[..body.len().min(100)]
    );
}

#[test]
fn test_homepage_body_contains_required_tags() {
    let port = start_test_server();
    let response = http_get(port, "/");

    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_lowercase();
    assert!(body.contains("<html"), "Response body missing <html> tag");
    assert!(body.contains("<head>"), "Response body missing <head> tag");
    assert!(body.contains("<body>"), "Response body missing <body> tag");
}

#[test]
fn test_homepage_body_contains_title() {
    let port = start_test_server();
    let response = http_get(port, "/");

    let body = response.split("\r\n\r\n").nth(1).unwrap_or("");
    assert!(
        body.contains("<title>MirDB"),
        "Response body missing expected title"
    );
}

#[test]
fn test_unknown_path_returns_404() {
    let port = start_test_server();
    let response = http_get(port, "/unknown-path");

    assert!(
        response.contains("HTTP/1.1 404 Not Found"),
        "Expected HTTP 404 Not Found for unknown path, got: {}",
        response.lines().next().unwrap_or("empty response")
    );
}

#[test]
fn test_styles_css_returns_200() {
    let port = start_test_server();
    let response = http_get(port, "/styles.css");

    assert!(
        response.contains("HTTP/1.1 200 OK"),
        "Expected HTTP 200 OK for /styles.css"
    );
    assert!(
        response.contains("Content-Type: text/css"),
        "Expected Content-Type: text/css for /styles.css"
    );
}

#[test]
fn test_homepage_response_time_under_2_seconds() {
    let port = start_test_server();

    let start = std::time::Instant::now();
    let _response = http_get(port, "/");
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "Homepage response took {:?}, expected under 2 seconds",
        elapsed
    );
}
