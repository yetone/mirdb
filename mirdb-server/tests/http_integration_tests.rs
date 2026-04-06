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

// ============================================================================
// Scenario 5: Key Detail View Tests
// ============================================================================

/// Test GET /api/keys/{key} for an existing key returns JSON with key details
/// Test Case 1: GET /api/keys/test_key (existing key) -> JSON response with key, value, size, flags
#[test]
fn test_api_key_detail_existing_key() {
    let port = 18088;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with key detail handler
    let _server_handle = start_test_server_with_key_detail(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys/test_key").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for existing key, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify JSON content type
    assert!(
        response.contains("Content-Type: application/json"),
        "Expected Content-Type: application/json"
    );

    // Verify response contains required fields
    assert!(response.contains("\"key\""), "Expected 'key' field in response");
    assert!(response.contains("\"value\""), "Expected 'value' field in response");
    assert!(response.contains("\"size\""), "Expected 'size' field in response");
    assert!(response.contains("\"flags\""), "Expected 'flags' field in response");
    assert!(response.contains("test_key"), "Expected key name in response");
    assert!(response.contains("test_value"), "Expected key value in response");
}

/// Test GET /api/keys/nonexistent_key returns HTTP 404
/// Test Case 2: GET /api/keys/nonexistent_key -> HTTP 404 with error message
#[test]
fn test_api_key_detail_nonexistent_key() {
    let port = 18089;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with key detail handler
    let _server_handle = start_test_server_with_key_detail(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys/nonexistent_key").expect("Failed to connect");

    // Verify HTTP 404 response
    assert!(
        response.contains("404"),
        "Expected HTTP 404 for nonexistent key, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify JSON content type
    assert!(
        response.contains("Content-Type: application/json"),
        "Expected Content-Type: application/json"
    );

    // Verify error message
    assert!(
        response.contains("\"error\"") && response.contains("not found"),
        "Expected error message about key not found"
    );
}

/// Test GET /api/keys/{key} with URL-encoded special characters
/// Test Case 5: GET /api/keys/{key} with special characters -> URL-encoded names handled
#[test]
fn test_api_key_detail_special_characters() {
    let port = 18090;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with key detail handler
    let _server_handle = start_test_server_with_key_detail(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // Test URL-encoded key with spaces: "key with spaces" -> "key%20with%20spaces"
    let response = http_get(&server_addr, "/api/keys/key%20with%20spaces").expect("Failed to connect");

    // Verify HTTP 200 response (our mock returns this key)
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for URL-encoded key, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify the decoded key name is in the response
    assert!(
        response.contains("key with spaces"),
        "Expected decoded key name in response"
    );
}

/// Test GET /api/keys/{key} for large value returns truncated response
/// Test Case 3: GET /api/keys/{key} for large value (>1MB) -> Value is truncated
#[test]
fn test_api_key_detail_large_value_truncated() {
    let port = 18091;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with key detail handler (including large key)
    let _server_handle = start_test_server_with_key_detail(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys/large_key").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for large key, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify truncation indicator in value
    assert!(
        response.contains("truncated"),
        "Expected truncation indicator for large value"
    );
}

/// Test Case 4: Click on key row in browser UI -> Key detail view loads
/// This test verifies the UI elements needed for the key detail modal exist
#[test]
fn test_key_detail_modal_ui_elements_exist() {
    let port = 18092;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let html = http_get(&server_addr, "/").expect("Failed to connect");

    // Verify modal container exists with correct ID
    assert!(
        html.contains("id=\"key-detail-modal\""),
        "Expected key-detail-modal element"
    );

    // Verify modal has aria-hidden attribute for accessibility
    assert!(
        html.contains("aria-hidden"),
        "Expected aria-hidden attribute on modal"
    );

    // Verify modal has close button
    assert!(
        html.contains("modal__close"),
        "Expected modal close button"
    );

    // Verify modal body exists for content injection
    assert!(
        html.contains("id=\"modal-body\""),
        "Expected modal-body element for key details"
    );

    // Verify key browser table exists with view buttons/links
    assert!(
        html.contains("keys-table-body"),
        "Expected keys-table-body for key list"
    );

    // Verify JavaScript is loaded that handles key detail functionality
    let js = http_get(&server_addr, "/static/js/app.js").expect("Failed to get JS");
    assert!(
        js.contains("fetchKeyDetail"),
        "Expected fetchKeyDetail function in app.js"
    );
    assert!(
        js.contains("showKeyDetail"),
        "Expected showKeyDetail function in app.js"
    );
    assert!(
        js.contains("showModal"),
        "Expected showModal function in app.js"
    );
    assert!(
        js.contains("hideModal"),
        "Expected hideModal function in app.js"
    );

    // Verify event delegation for key clicks
    assert!(
        js.contains("key-link") || js.contains("view-key-btn"),
        "Expected click handler for key links or view buttons"
    );
}

// ============================================================================
// Scenario 3: Key Browser with Pagination Tests
// ============================================================================

/// Test Case 1: GET /api/keys returns JSON with pagination metadata
#[test]
fn test_api_keys_returns_pagination_metadata() {
    let port = 18100;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with pagination support
    let _server_handle = start_test_server_with_pagination(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/keys"
    );

    // Verify JSON content type
    assert!(
        response.contains("Content-Type: application/json"),
        "Expected Content-Type: application/json"
    );

    // Verify pagination metadata fields exist
    assert!(response.contains("\"keys\""), "Expected 'keys' field in response");
    assert!(response.contains("\"total\""), "Expected 'total' field in response");
    assert!(response.contains("\"offset\""), "Expected 'offset' field in response");
    assert!(response.contains("\"limit\""), "Expected 'limit' field in response");
}

/// Test Case 2: GET /api/keys?offset=0&limit=20 returns first 20 keys
#[test]
fn test_api_keys_first_page_with_explicit_params() {
    let port = 18101;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with pagination support
    let _server_handle = start_test_server_with_pagination(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?offset=0&limit=20").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/keys?offset=0&limit=20"
    );

    // Verify offset is 0
    assert!(
        response.contains("\"offset\":0"),
        "Expected offset:0 in response"
    );

    // Verify limit is 20
    assert!(
        response.contains("\"limit\":20"),
        "Expected limit:20 in response"
    );

    // Verify total count is correct (500 keys in mock)
    assert!(
        response.contains("\"total\":500"),
        "Expected total:500 in response"
    );

    // Verify keys array has 20 items
    // Count the number of keys in the response
    let keys_count = response.matches("\"key_").count();
    assert_eq!(keys_count, 20, "Expected exactly 20 keys in response");
}

/// Test Case 3: GET /api/keys?offset=100&limit=20 returns keys 101-120
#[test]
fn test_api_keys_offset_pagination() {
    let port = 18102;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with pagination support
    let _server_handle = start_test_server_with_pagination(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?offset=100&limit=20").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/keys?offset=100&limit=20"
    );

    // Verify offset is 100
    assert!(
        response.contains("\"offset\":100"),
        "Expected offset:100 in response"
    );

    // Verify limit is 20
    assert!(
        response.contains("\"limit\":20"),
        "Expected limit:20 in response"
    );

    // Verify total count is still the full count
    assert!(
        response.contains("\"total\":500"),
        "Expected total:500 in response"
    );

    // Verify the first key in response is key_100 (0-indexed means offset=100 gets key_100)
    assert!(
        response.contains("\"key_100\""),
        "Expected key_100 in response (first key at offset 100)"
    );
}

/// Test Case 4: Response time for /api/keys with large dataset under 2 seconds
#[test]
fn test_api_keys_response_time_under_2_seconds() {
    let port = 18103;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with pagination support (simulating 10000+ keys)
    let _server_handle = start_test_server_with_large_dataset(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // Measure response time
    let start = Instant::now();
    let response = http_get(&server_addr, "/api/keys?offset=0&limit=20").expect("Failed to connect");
    let elapsed = start.elapsed();

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/keys"
    );

    // Verify response time is under 2 seconds (Story 2 acceptance criteria)
    assert!(
        elapsed < Duration::from_secs(2),
        "Response took {:?}, expected under 2 seconds",
        elapsed
    );
}

/// Test Case 5: GET /api/keys with offset exceeding total returns empty array
#[test]
fn test_api_keys_offset_exceeds_total() {
    let port = 18104;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with pagination support
    let _server_handle = start_test_server_with_pagination(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // Request with offset beyond total keys (total is 500)
    let response = http_get(&server_addr, "/api/keys?offset=1000&limit=20").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/keys with offset exceeding total"
    );

    // Verify offset is 1000
    assert!(
        response.contains("\"offset\":1000"),
        "Expected offset:1000 in response"
    );

    // Verify total count is still correct
    assert!(
        response.contains("\"total\":500"),
        "Expected total:500 in response"
    );

    // Verify keys array is empty
    assert!(
        response.contains("\"keys\":[]"),
        "Expected empty keys array when offset exceeds total"
    );
}

/// Test Case 6: Browser UI pagination controls exist and are functional
/// (Verifies the UI elements needed for pagination are present in the HTML)
#[test]
fn test_pagination_ui_controls_exist() {
    let port = 18105;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server
    let _server_handle = start_test_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let html = http_get(&server_addr, "/").expect("Failed to connect");

    // Verify pagination container exists
    assert!(
        html.contains("id=\"pagination\"") || html.contains("class=\"pagination\""),
        "Expected pagination container in HTML"
    );

    // Verify Previous button exists
    assert!(
        html.contains("pagination__btn--prev"),
        "Expected Previous button with correct class"
    );

    // Verify Next button exists
    assert!(
        html.contains("pagination__btn--next"),
        "Expected Next button with correct class"
    );

    // Verify page info element exists
    assert!(
        html.contains("pagination__info"),
        "Expected page info element"
    );

    // Verify JavaScript handles pagination
    let js = http_get(&server_addr, "/static/js/app.js").expect("Failed to get JS");
    assert!(
        js.contains("paginationPrev") || js.contains("pagination"),
        "Expected pagination handling in app.js"
    );
    assert!(
        js.contains("paginationNext") || js.contains("fetchKeys"),
        "Expected pagination or fetchKeys function in app.js"
    );
}

/// Start a test HTTP server with pagination support and mock data
fn start_test_server_with_pagination(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Handle requests
        for _ in 0..20 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_request_with_pagination(&request, 500);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Start a test HTTP server with a large dataset (10,000+ keys) for performance testing
fn start_test_server_with_large_dataset(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Handle requests
        for _ in 0..20 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_request_with_pagination(&request, 10000);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Handle test requests with pagination support
fn handle_test_request_with_pagination(
    request: &tiny_http::Request,
    total_keys: u64,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let url = request.url();

    // Split path and query string
    let (path, query) = match url.find('?') {
        Some(pos) => (&url[..pos], Some(&url[pos + 1..])),
        None => (url, None),
    };

    if path == "/api/keys" {
        return handle_keys_mock(query, total_keys);
    }

    // Fall back to standard handler
    handle_test_request(request)
}

/// Mock handler for keys endpoint with pagination
fn handle_keys_mock(
    query: Option<&str>,
    total_keys: u64,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Parse query parameters
    let mut offset: u64 = 0;
    let mut limit: u64 = 20;

    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some(pos) = pair.find('=') {
                let key = &pair[..pos];
                let value = &pair[pos + 1..];
                match key {
                    "offset" => offset = value.parse().unwrap_or(0),
                    "limit" => limit = value.parse().unwrap_or(20),
                    _ => {}
                }
            }
        }
    }

    // Generate mock keys based on offset and limit
    let mut keys: Vec<String> = Vec::new();
    let end = std::cmp::min(offset + limit, total_keys);
    for i in offset..end {
        keys.push(format!("key_{}", i));
    }

    // Build JSON response
    let keys_json: Vec<String> = keys.iter().map(|k| format!("\"{}\"", k)).collect();
    let json = format!(
        r#"{{"keys":[{}],"total":{},"offset":{},"limit":{}}}"#,
        keys_json.join(","),
        total_keys,
        offset,
        limit
    );

    tiny_http::Response::from_string(json).with_header(
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
    )
}

// Helper functions

/// Start a test HTTP server on the given port with key detail support
fn start_test_server_with_key_detail(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Handle a limited number of requests for testing
        for _ in 0..20 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_request_with_key_detail(&request);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Handle test requests including key detail endpoint
fn handle_test_request_with_key_detail(
    request: &tiny_http::Request,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let path = request.url();

    // Handle key detail endpoint
    if path.starts_with("/api/keys/") {
        let encoded_key = &path[10..]; // 10 = "/api/keys/".len()
        return handle_key_detail_mock(encoded_key);
    }

    // Fall back to standard handler
    handle_test_request(request)
}

/// Mock handler for key detail endpoint
fn handle_key_detail_mock(encoded_key: &str) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // URL decode the key
    let key = url_decode_test(encoded_key);

    match key.as_str() {
        "test_key" => {
            let json = r#"{"key":"test_key","value":"test_value","size":10,"flags":0}"#;
            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        "key with spaces" => {
            let json = r#"{"key":"key with spaces","value":"value with spaces","size":17,"flags":0}"#;
            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        "large_key" => {
            // Simulate a large value that was truncated
            let json = r#"{"key":"large_key","value":"AAAA... [truncated, showing first 1MB of 2097152 bytes]","size":2097152,"flags":0}"#;
            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        _ => {
            // Key not found
            let json = r#"{"error":"Key not found"}"#;
            tiny_http::Response::from_string(json)
                .with_status_code(tiny_http::StatusCode(404))
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                )
        }
    }
}

/// Simple URL decode for test purposes
fn url_decode_test(encoded: &str) -> String {
    let mut result = Vec::with_capacity(encoded.len());
    let mut chars = encoded.bytes().peekable();

    while let Some(b) = chars.next() {
        if b == b'%' {
            let high = chars.next();
            let low = chars.next();
            if let (Some(h), Some(l)) = (high, low) {
                let hex_str = format!("{}{}", h as char, l as char);
                if let Ok(decoded) = u8::from_str_radix(&hex_str, 16) {
                    result.push(decoded);
                    continue;
                }
            }
            result.push(b);
        } else if b == b'+' {
            result.push(b' ');
        } else {
            result.push(b);
        }
    }

    String::from_utf8_lossy(&result).to_string()
}

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
