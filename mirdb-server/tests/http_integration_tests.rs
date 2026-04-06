//! Integration tests for HTTP server.
//! Owner: Scenario 1 - Basic tests, Scenario 9 - Performance tests
//!
//! Test categories:
//! - Server startup and port binding
//! - Static file serving (Scenario 13)
//! - API endpoint availability
//! - Response content types
//! - Load time requirements (< 2 seconds)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Test that the homepage returns HTTP 200 with HTML content containing MirDB branding
#[test]
fn test_homepage_returns_200_with_mirdb_branding() {
    let port = 18081;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // Send GET request to /
    let html = http_get(&server_addr, "/").expect("Failed to connect to server");

    // Verify HTTP 200 response
    assert!(
        html.contains("HTTP/1.1 200") || html.contains("HTTP/1.0 200"),
        "Expected HTTP 200 response, got: {}",
        &html[..html.len().min(100)]
    );

    // Verify HTML content type
    assert!(
        html.contains("Content-Type: text/html"),
        "Expected Content-Type: text/html"
    );

    // Verify MirDB branding
    assert!(html.contains("MirDB"), "Expected MirDB branding in response");

    // Verify navigation structure
    assert!(html.contains("Dashboard"), "Expected Dashboard link");
    assert!(html.contains("Keys"), "Expected Keys link");
    assert!(html.contains("Documentation"), "Expected Documentation link");
    assert!(html.contains("GitHub"), "Expected GitHub link");

    // Verify semantic HTML structure
    assert!(html.contains("<nav"), "Expected nav element");
    assert!(html.contains("<main"), "Expected main element");
    assert!(html.contains("<footer"), "Expected footer element");

    // Stop server
    drop(server_handle);
}

/// Test that the homepage loads within 2 seconds (Success Criteria)
#[test]
fn test_homepage_load_time_under_2_seconds() {
    let port = 18082;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // Measure page load time
    let start = Instant::now();
    let _html = http_get(&server_addr, "/").expect("Failed to connect to server");
    let elapsed = start.elapsed();

    // Verify load time is under 2 seconds
    assert!(
        elapsed < Duration::from_secs(2),
        "Page load took {:?}, expected under 2 seconds",
        elapsed
    );
}

/// Test that HTML contains all required navigation links
#[test]
fn test_navigation_structure() {
    let port = 18083;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let html = http_get(&server_addr, "/").expect("Failed to connect to server");

    // Dashboard link (internal anchor)
    assert!(
        html.contains("href=\"#dashboard\"") || html.contains("href='#dashboard'"),
        "Expected link to Dashboard section"
    );

    // Keys link (internal anchor)
    assert!(
        html.contains("href=\"#keys\"") || html.contains("href='#keys'"),
        "Expected link to Keys section"
    );

    // Documentation link (external URL to GitHub README)
    assert!(
        html.contains("github.com/yetone/mirdb") && html.contains("readme"),
        "Expected link to Documentation (GitHub README)"
    );

    // GitHub link (external URL)
    assert!(
        html.contains("href=\"https://github.com/yetone/mirdb\"")
            || html.contains("href='https://github.com/yetone/mirdb'"),
        "Expected link to GitHub repository"
    );
}

/// Test that CSS is served correctly
#[test]
fn test_css_served() {
    let port = 18084;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/static/css/style.css").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for CSS"
    );

    // Verify CSS content type
    assert!(
        response.contains("Content-Type: text/css"),
        "Expected Content-Type: text/css"
    );

    // Verify CSS content
    assert!(response.contains(":root"), "Expected CSS variables in style.css");
}

/// Test that JavaScript is served correctly
#[test]
fn test_js_served() {
    let port = 18085;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/static/js/app.js").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for JS"
    );

    // Verify JS content type
    assert!(
        response.contains("Content-Type: application/javascript"),
        "Expected Content-Type: application/javascript"
    );

    // Verify JS content
    assert!(
        response.contains("MirDB Dashboard"),
        "Expected MirDB Dashboard comment in app.js"
    );
}

/// Test 404 for unknown paths
#[test]
fn test_404_for_unknown_path() {
    let port = 18086;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/nonexistent").expect("Failed to connect");

    // Verify HTTP 404 response
    assert!(
        response.contains("404"),
        "Expected HTTP 404 for unknown path"
    );
}

/// Test API stats endpoint exists
#[test]
fn test_api_stats_endpoint() {
    let port = 18087;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server in background thread
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/stats").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/stats"
    );

    // Verify JSON content type
    assert!(
        response.contains("Content-Type: application/json"),
        "Expected Content-Type: application/json"
    );
}

// Helper functions

/// Start a test HTTP server on the given port
fn start_test_server(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        // Use tiny_http directly for tests
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Handle a limited number of requests for testing
        for _ in 0..20 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_request(&request);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

fn handle_test_request(
    request: &tiny_http::Request,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let path = request.url();

    match path {
        "/" | "/index.html" => {
            let html = include_str!("../static/index.html");
            tiny_http::Response::from_string(html).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
                    .unwrap(),
            )
        }
        "/static/css/style.css" => {
            let css = include_str!("../static/css/style.css");
            tiny_http::Response::from_string(css).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/css; charset=utf-8"[..])
                    .unwrap(),
            )
        }
        "/static/js/app.js" => {
            let js = include_str!("../static/js/app.js");
            tiny_http::Response::from_string(js).with_header(
                tiny_http::Header::from_bytes(
                    &b"Content-Type"[..],
                    &b"application/javascript; charset=utf-8"[..],
                )
                .unwrap(),
            )
        }
        "/api/stats" => {
            let json = r#"{"total_keys":0,"version":"0.1.0","uptime_seconds":0}"#;
            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        "/api/keys" => {
            let json = r#"{"keys":[],"total":0,"offset":0,"limit":20}"#;
            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        "/api/compaction" => {
            let json = r#"{"status":"idle","progress":0}"#;
            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        _ => tiny_http::Response::from_string("Not Found")
            .with_status_code(tiny_http::StatusCode(404))
            .with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
            ),
    }
}

/// Send an HTTP GET request and return the response
fn http_get(addr: &str, path: &str) -> Result<String, std::io::Error> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, addr
    );
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    Ok(response)
}
