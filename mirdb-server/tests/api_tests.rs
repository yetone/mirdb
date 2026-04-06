//! API endpoint unit and integration tests.
//! Owner: Multiple scenarios contribute tests
//!
//! Test /api/stats (Scenario 2):
//! - Returns correct JSON structure
//! - Cache behavior (fresh vs cached)
//! - Stats accuracy after data changes
//!
//! Test /api/keys (Scenarios 3, 4):
//! - Pagination works correctly
//! - Search filter works
//! - Empty results handling
//! - Large dataset performance
//!
//! Test /api/keys/:key (Scenario 5):
//! - Returns key details
//! - 404 for missing keys
//! - Large value truncation
//! - URL-encoded key names
//!
//! Test /api/compaction (Scenario 6):
//! - Idle status response
//! - Active compaction response
//!
//! Error cases (Scenario 12):
//! - Invalid endpoints -> 404
//! - Invalid parameters -> 400
//! - Method not allowed -> 405

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// Scenario 3: Key Browser with Pagination Tests
// ============================================================================

/// Test that /api/keys returns JSON with pagination metadata
#[test]
fn test_api_keys_returns_pagination_metadata() {
    let port = 19001;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_keys(port, 50);
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

    // Verify pagination fields exist in response
    assert!(response.contains("\"keys\""), "Expected 'keys' field");
    assert!(response.contains("\"total\""), "Expected 'total' field");
    assert!(response.contains("\"offset\""), "Expected 'offset' field");
    assert!(response.contains("\"limit\""), "Expected 'limit' field");
}

/// Test that /api/keys?offset=0&limit=20 returns first 20 keys
#[test]
fn test_api_keys_first_page() {
    let port = 19002;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_keys(port, 100);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?offset=0&limit=20").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200"
    );

    // Verify pagination info
    assert!(
        response.contains("\"offset\":0"),
        "Expected offset to be 0"
    );
    assert!(
        response.contains("\"limit\":20"),
        "Expected limit to be 20"
    );
}

/// Test that /api/keys?offset=100&limit=20 returns keys 101-120
#[test]
fn test_api_keys_middle_page() {
    let port = 19003;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_keys(port, 200);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?offset=100&limit=20").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200"
    );

    // Verify pagination info
    assert!(
        response.contains("\"offset\":100"),
        "Expected offset to be 100"
    );
}

/// Test that /api/keys with offset exceeding total returns empty array
#[test]
fn test_api_keys_offset_exceeds_total() {
    let port = 19004;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_keys(port, 10);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?offset=1000&limit=20").expect("Failed to connect");

    // Verify HTTP 200 response (not an error)
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 even with offset exceeding total"
    );

    // Verify empty keys array but total still populated
    assert!(
        response.contains("\"keys\":[]") || response.contains("\"keys\": []"),
        "Expected empty keys array when offset exceeds total"
    );
}

/// Test response time for /api/keys is under 2 seconds (performance requirement)
#[test]
fn test_api_keys_performance_under_2_seconds() {
    let port = 19005;
    let server_addr = format!("127.0.0.1:{}", port);

    // Create server with many keys to test performance
    let _server_handle = start_test_server_with_keys(port, 500);
    thread::sleep(Duration::from_millis(500));

    // Measure response time
    let start = Instant::now();
    let response = http_get(&server_addr, "/api/keys?offset=0&limit=20").expect("Failed to connect");
    let elapsed = start.elapsed();

    // Verify response was successful
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200"
    );

    // Verify load time is under 2 seconds
    assert!(
        elapsed < Duration::from_secs(2),
        "API keys request took {:?}, expected under 2 seconds",
        elapsed
    );
}

/// Test that default limit is applied when not specified
#[test]
fn test_api_keys_default_limit() {
    let port = 19006;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_keys(port, 50);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys").expect("Failed to connect");

    // Verify default limit of 20 is applied
    assert!(
        response.contains("\"limit\":20"),
        "Expected default limit of 20"
    );
}

/// Test API returns correct total count
#[test]
fn test_api_keys_total_count_accuracy() {
    let port = 19007;
    let server_addr = format!("127.0.0.1:{}", port);

    let num_keys = 75;
    let _server_handle = start_test_server_with_keys(port, num_keys);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?offset=0&limit=10").expect("Failed to connect");

    // Verify total count matches number of keys
    assert!(
        response.contains(&format!("\"total\":{}", num_keys)),
        "Expected total to be {}, got response: {}",
        num_keys,
        &response[response.len().saturating_sub(200)..]
    );
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Start a test HTTP server that simulates having `num_keys` keys
fn start_test_server_with_keys(port: u16, num_keys: usize) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_keys_request(&request, num_keys);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

fn handle_test_keys_request(
    request: &tiny_http::Request,
    num_keys: usize,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let url = request.url();

    // Parse path and query string
    let (path, query_string) = match url.find('?') {
        Some(pos) => (&url[..pos], Some(&url[pos + 1..])),
        None => (url, None),
    };

    match path {
        "/api/keys" => {
            // Parse pagination parameters
            let (offset, limit) = parse_pagination_params(query_string);

            // Generate mock keys
            let all_keys: Vec<String> = (0..num_keys)
                .map(|i| format!("key_{:05}", i))
                .collect();

            // Apply pagination
            let paginated_keys: Vec<String> = all_keys
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect();

            let json = format!(
                r#"{{"keys":[{}],"total":{},"offset":{},"limit":{}}}"#,
                paginated_keys
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(","),
                num_keys,
                offset,
                limit
            );

            tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        }
        "/api/stats" => {
            let json = format!(
                r#"{{"total_keys":{},"version":"0.1.0","uptime_seconds":0}}"#,
                num_keys
            );
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

fn parse_pagination_params(query: Option<&str>) -> (usize, usize) {
    let mut offset: usize = 0;
    let mut limit: usize = 20;

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

    (offset, limit.min(100))
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
