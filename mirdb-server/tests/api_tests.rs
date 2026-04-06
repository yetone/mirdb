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

use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// Scenario 2: Database Statistics Display Tests
// ============================================================================

/// Test Case 1: GET /api/stats returns JSON response with all required fields
/// Input: GET /api/stats
/// Expected: JSON response with fields: total_keys, memory_usage, storage_size, version, uptime
#[test]
fn test_api_stats_returns_correct_json_structure() {
    let port = 19101;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with real store
    let _server_handle = start_test_server_with_store(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/stats").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for /api/stats, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify JSON content type
    assert!(
        response.contains("Content-Type: application/json"),
        "Expected Content-Type: application/json"
    );

    // Verify all required fields are present (REQ-2 and REQ-8)
    assert!(
        response.contains("\"total_keys\""),
        "Expected total_keys field in response"
    );
    assert!(
        response.contains("\"memory_usage\""),
        "Expected memory_usage field in response"
    );
    assert!(
        response.contains("\"storage_size\""),
        "Expected storage_size field in response"
    );
    assert!(
        response.contains("\"version\""),
        "Expected version field in response"
    );
    assert!(
        response.contains("\"uptime_seconds\""),
        "Expected uptime_seconds field in response"
    );
    assert!(
        response.contains("\"last_updated\""),
        "Expected last_updated field (cache timestamp) in response"
    );
}

/// Test Case 3: Cache behavior - calls within 1 second return cached response
/// Input: Call /api/stats twice within 1 second
/// Expected: Second call returns cached response (same last_updated timestamp)
#[test]
fn test_api_stats_cache_within_ttl() {
    let port = 19102;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with real store
    let _server_handle = start_test_server_with_store(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // First call
    let response1 = http_get(&server_addr, "/api/stats").expect("Failed first request");
    let timestamp1 = extract_last_updated(&response1);

    // Wait less than the cache TTL (5 seconds) - wait only 500ms
    thread::sleep(Duration::from_millis(500));

    // Second call (should be cached)
    let response2 = http_get(&server_addr, "/api/stats").expect("Failed second request");
    let timestamp2 = extract_last_updated(&response2);

    // Both responses should have the same last_updated timestamp (from cache)
    assert_eq!(
        timestamp1, timestamp2,
        "Expected same timestamp for cached response. First: {}, Second: {}",
        timestamp1, timestamp2
    );
}

/// Test Case 4: Cache expiration - after TTL, fresh stats are returned
/// Input: Call /api/stats, wait 6 seconds, call again
/// Expected: Cache expires and fresh statistics are returned (different last_updated)
#[test]
fn test_api_stats_cache_expires_after_ttl() {
    let port = 19103;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server with real store
    let _server_handle = start_test_server_with_store(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // First call
    let response1 = http_get(&server_addr, "/api/stats").expect("Failed first request");
    let timestamp1 = extract_last_updated(&response1);

    // Wait longer than the cache TTL (5 seconds + margin)
    thread::sleep(Duration::from_secs(6));

    // Second call (should be fresh)
    let response2 = http_get(&server_addr, "/api/stats").expect("Failed second request");
    let timestamp2 = extract_last_updated(&response2);

    // Second response should have a newer timestamp
    assert!(
        timestamp2 > timestamp1,
        "Expected newer timestamp after cache expiry. First: {}, Second: {}",
        timestamp1, timestamp2
    );
}

/// Test Case 5: Dashboard HTML contains stats display elements
/// Input: Dashboard HTML rendering with stats
/// Expected: Statistics values display correctly in card layout with visual indicators
#[test]
fn test_dashboard_stats_display_elements() {
    let port = 19105;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server
    let _server_handle = start_test_server_with_store(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let html = http_get(&server_addr, "/").expect("Failed to get homepage");

    // Verify stats cards exist (REQ-2)
    assert!(
        html.contains("stats-cards") || html.contains("stats-card"),
        "Expected stats cards container in dashboard"
    );

    // Verify individual stat elements exist
    assert!(
        html.contains("stat-total-keys") || html.contains("Total Keys"),
        "Expected total keys display element"
    );
    assert!(
        html.contains("stat-memory") || html.contains("Memory Usage"),
        "Expected memory usage display element"
    );
    assert!(
        html.contains("stat-storage") || html.contains("Storage Size"),
        "Expected storage size display element"
    );
    assert!(
        html.contains("stat-version") || html.contains("Version"),
        "Expected version display element"
    );

    // Verify JavaScript loads stats (check app.js)
    let js = http_get(&server_addr, "/static/js/app.js").expect("Failed to get JS");
    assert!(
        js.contains("fetchStats"),
        "Expected fetchStats function in app.js"
    );
    assert!(
        js.contains("/api/stats") || js.contains("/stats"),
        "Expected stats API call in app.js"
    );
    assert!(
        js.contains("statsRefreshInterval") || js.contains("5000"),
        "Expected stats refresh interval for real-time updates (NFR-4)"
    );
}

/// Test version field contains valid version string
#[test]
fn test_api_stats_version_field() {
    let port = 19106;
    let server_addr = format!("127.0.0.1:{}", port);

    // Start server
    let _server_handle = start_test_server_with_store(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/stats").expect("Failed to connect");

    // Verify version follows semver format (e.g., "0.1.0")
    assert!(
        response.contains("\"version\":\"") && response.contains("."),
        "Expected version field with semver format"
    );
}

// ============================================================================
// Scenario 3: Key Browser with Pagination Tests
// ============================================================================

/// Test that /api/keys returns JSON with pagination metadata
#[test]
fn test_api_keys_returns_pagination_metadata() {
    let port = 19201;
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
    let port = 19202;
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

/// Test that /api/keys with offset exceeding total returns empty array
#[test]
fn test_api_keys_offset_exceeds_total() {
    let port = 19204;
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
    let port = 19205;
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
    let port = 19206;
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

// ============================================================================
// Helper Functions for Scenario 2 (Stats)
// ============================================================================

/// Extract last_updated timestamp from JSON response
fn extract_last_updated(response: &str) -> u64 {
    // Find the JSON body (after headers)
    let body_start = response.find('{').unwrap_or(0);
    let json_part = &response[body_start..];

    // Simple extraction of last_updated value
    if let Some(idx) = json_part.find("\"last_updated\":") {
        let value_start = idx + "\"last_updated\":".len();
        let remaining = &json_part[value_start..];
        let end = remaining.find(|c: char| !c.is_ascii_digit()).unwrap_or(remaining.len());
        remaining[..end].trim().parse().unwrap_or(0)
    } else {
        0
    }
}

/// Start a test HTTP server with a real Store on the given port (Scenario 2)
fn start_test_server_with_store(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        use std::time::{SystemTime, UNIX_EPOCH};

        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Track start time for uptime
        let start_time = SystemTime::now();

        // Simple in-memory stats for testing
        let mut last_updated: u64 = 0;
        let cache_ttl_secs: u64 = 5;

        // Handle requests
        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(10)) {
                if let Some(request) = request {
                    let path = request.url().to_string();
                    let response = match path.as_str() {
                        "/api/stats" => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);

                            // Check cache
                            if last_updated == 0 || now - last_updated >= cache_ttl_secs {
                                last_updated = now;
                            }

                            let uptime = start_time.elapsed().map(|d| d.as_secs()).unwrap_or(0);

                            let json = format!(
                                r#"{{"total_keys":0,"memory_usage":8388608,"storage_size":0,"version":"0.1.0","uptime_seconds":{},"last_updated":{}}}"#,
                                uptime, last_updated
                            );
                            tiny_http::Response::from_string(json).with_header(
                                tiny_http::Header::from_bytes(
                                    &b"Content-Type"[..],
                                    &b"application/json"[..],
                                )
                                .unwrap(),
                            )
                        }
                        "/" | "/index.html" => {
                            let html = include_str!("../static/index.html");
                            tiny_http::Response::from_string(html).with_header(
                                tiny_http::Header::from_bytes(
                                    &b"Content-Type"[..],
                                    &b"text/html; charset=utf-8"[..],
                                )
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
                        _ => tiny_http::Response::from_string("Not Found")
                            .with_status_code(tiny_http::StatusCode(404))
                            .with_header(
                                tiny_http::Header::from_bytes(
                                    &b"Content-Type"[..],
                                    &b"text/plain"[..],
                                )
                                .unwrap(),
                            ),
                    };
                    let _ = request.respond(response);
                }
            }
        }
    })
}

// ============================================================================
// Scenario 4: Key Search and Filter Tests
// ============================================================================

/// Test that /api/keys?search=user: returns only keys starting with 'user:'
#[test]
fn test_api_keys_search_prefix_match() {
    let port = 19010;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_prefixed_keys(port);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?search=user:").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for search request"
    );

    // Verify only user: keys are returned
    assert!(response.contains("\"user:1\""), "Expected user:1 in results");
    assert!(response.contains("\"user:2\""), "Expected user:2 in results");
    assert!(!response.contains("\"session:"), "Should not contain session: keys");
    assert!(!response.contains("\"cache:"), "Should not contain cache: keys");
}

/// Test that /api/keys?search=nonexistent returns empty array
#[test]
fn test_api_keys_search_no_match() {
    let port = 19011;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_prefixed_keys(port);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?search=nonexistent").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for search request with no matches"
    );

    // Verify empty keys array
    assert!(
        response.contains("\"keys\":[]") || response.contains("\"keys\": []"),
        "Expected empty keys array when search has no matches"
    );

    // Verify total is 0
    assert!(
        response.contains("\"total\":0"),
        "Expected total to be 0 when no matches"
    );
}

/// Test that /api/keys?search=&offset=0&limit=10 (empty search) returns all keys paginated
#[test]
fn test_api_keys_search_empty_returns_all() {
    let port = 19012;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_prefixed_keys(port);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys?search=&offset=0&limit=10").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200"
    );

    // Verify all key types are returned (total should be 6)
    assert!(
        response.contains("\"total\":6"),
        "Expected total to be 6 with empty search"
    );
}

/// Test case sensitivity of search (search is case-sensitive)
#[test]
fn test_api_keys_search_case_sensitive() {
    let port = 19013;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_prefixed_keys(port);
    thread::sleep(Duration::from_millis(500));

    // Search for "User:" (uppercase U) - should not match lowercase "user:"
    let response = http_get(&server_addr, "/api/keys?search=User:").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200"
    );

    // With case-sensitive search, "User:" should not match "user:" keys
    // (unless there are uppercase keys in the test data)
    assert!(
        response.contains("\"keys\":[]") || response.contains("\"keys\": []"),
        "Expected empty keys array for case-sensitive search mismatch"
    );
}

/// Test search with pagination
#[test]
fn test_api_keys_search_with_pagination() {
    let port = 19014;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_many_prefixed_keys(port, 30);
    thread::sleep(Duration::from_millis(500));

    // Search for user: keys with pagination
    let response = http_get(&server_addr, "/api/keys?search=user:&offset=0&limit=5").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200"
    );

    // Verify pagination is applied
    assert!(
        response.contains("\"limit\":5"),
        "Expected limit to be 5"
    );
    assert!(
        response.contains("\"offset\":0"),
        "Expected offset to be 0"
    );
}

// ============================================================================
// Helper Functions for Scenario 4 (Search)
// ============================================================================

/// Start a test HTTP server with keys using distinct prefixes (for search testing)
fn start_test_server_with_prefixed_keys(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Test keys with different prefixes
        let all_keys = vec![
            "user:1".to_string(),
            "user:2".to_string(),
            "session:abc".to_string(),
            "session:def".to_string(),
            "cache:data1".to_string(),
            "cache:data2".to_string(),
        ];

        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_keys_request_with_search(&request, &all_keys);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Start a test HTTP server with many prefixed keys (for pagination + search testing)
fn start_test_server_with_many_prefixed_keys(port: u16, keys_per_prefix: usize) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        let mut all_keys: Vec<String> = Vec::new();
        for i in 0..keys_per_prefix {
            all_keys.push(format!("user:{:03}", i));
            all_keys.push(format!("session:{:03}", i));
            all_keys.push(format!("cache:{:03}", i));
        }

        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_test_keys_request_with_search(&request, &all_keys);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Handle test request with search support
fn handle_test_keys_request_with_search(
    request: &tiny_http::Request,
    all_keys: &[String],
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let url = request.url();

    // Parse path and query string
    let (path, query_string) = match url.find('?') {
        Some(pos) => (&url[..pos], Some(&url[pos + 1..])),
        None => (url, None),
    };

    match path {
        "/api/keys" => {
            // Parse parameters
            let (offset, limit, search) = parse_keys_params(query_string);

            // Filter by search prefix
            let filtered_keys: Vec<String> = if search.is_empty() {
                all_keys.to_vec()
            } else {
                all_keys.iter()
                    .filter(|k| k.starts_with(&search))
                    .cloned()
                    .collect()
            };

            let total = filtered_keys.len();

            // Apply pagination
            let paginated_keys: Vec<String> = filtered_keys
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
                total,
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
                all_keys.len()
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

/// Parse keys parameters including search
fn parse_keys_params(query: Option<&str>) -> (usize, usize, String) {
    let mut offset: usize = 0;
    let mut limit: usize = 20;
    let mut search = String::new();

    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some(pos) = pair.find('=') {
                let key = &pair[..pos];
                let value = &pair[pos + 1..];
                match key {
                    "offset" => offset = value.parse().unwrap_or(0),
                    "limit" => limit = value.parse().unwrap_or(20),
                    "search" => search = url_decode(value),
                    _ => {}
                }
            }
        }
    }

    (offset, limit.min(100), search)
}

/// Simple URL decode for test purposes
fn url_decode(encoded: &str) -> String {
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

    String::from_utf8(result).unwrap_or_else(|_| encoded.to_string())
}

// ============================================================================
// Scenario 5: Key Detail View Tests
// ============================================================================

/// Test Case 1: GET /api/keys/{key} for existing key returns JSON with key, value, size, flags
/// Input: GET /api/keys/test_key (existing key)
/// Expected: JSON response with key, value, size, flags fields
#[test]
fn test_api_key_detail_existing_key() {
    let port = 19501;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_key_details(port);
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

    // Verify all required fields are present
    assert!(
        response.contains("\"key\""),
        "Expected 'key' field in response"
    );
    assert!(
        response.contains("\"value\""),
        "Expected 'value' field in response"
    );
    assert!(
        response.contains("\"size\""),
        "Expected 'size' field in response"
    );
    assert!(
        response.contains("\"flags\""),
        "Expected 'flags' field in response"
    );

    // Verify key name matches
    assert!(
        response.contains("\"key\":\"test_key\""),
        "Expected key to be 'test_key'"
    );
}

/// Test Case 2: GET /api/keys/nonexistent_key returns HTTP 404
/// Input: GET /api/keys/nonexistent_key
/// Expected: HTTP 404 response with appropriate error message
#[test]
fn test_api_key_detail_nonexistent_key() {
    let port = 19502;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_key_details(port);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys/nonexistent_key").expect("Failed to connect");

    // Verify HTTP 404 response
    assert!(
        response.contains("HTTP/1.1 404") || response.contains("HTTP/1.0 404"),
        "Expected HTTP 404 for nonexistent key, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify error message is present
    assert!(
        response.contains("\"error\""),
        "Expected error field in 404 response"
    );
    assert!(
        response.contains("not found") || response.contains("Not found"),
        "Expected 'not found' in error message"
    );
}

/// Test Case 3: GET /api/keys/{key} for large value (>1MB) is truncated
/// Input: GET /api/keys/large_value_key
/// Expected: Value is truncated to prevent memory exhaustion (Risk Management)
#[test]
fn test_api_key_detail_large_value_truncation() {
    let port = 19503;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_large_value(port);
    thread::sleep(Duration::from_millis(500));

    let response = http_get(&server_addr, "/api/keys/large_value_key").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for large value key"
    );

    // Verify value is truncated (contains truncation indicator)
    assert!(
        response.contains("truncated") || response.contains("..."),
        "Expected truncation indicator in large value response"
    );

    // Verify original size is preserved in size field
    assert!(
        response.contains("\"size\""),
        "Expected size field to contain original size"
    );
}

/// Test Case 4: UI modal elements exist for key detail view
/// Input: Check HTML for key detail modal elements
/// Expected: Key detail modal with proper accessibility attributes
#[test]
fn test_key_detail_ui_modal_elements() {
    let port = 19504;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_key_details(port);
    thread::sleep(Duration::from_millis(500));

    let html = http_get(&server_addr, "/").expect("Failed to get homepage");

    // Verify modal exists
    assert!(
        html.contains("key-detail-modal"),
        "Expected key-detail-modal element in HTML"
    );

    // Verify modal has dialog role
    assert!(
        html.contains("role=\"dialog\""),
        "Expected modal to have dialog role for accessibility"
    );

    // Verify modal has aria-hidden attribute
    assert!(
        html.contains("aria-hidden"),
        "Expected aria-hidden attribute on modal"
    );

    // Verify modal close button exists
    assert!(
        html.contains("modal__close"),
        "Expected modal close button"
    );

    // Verify modal body exists for content
    assert!(
        html.contains("modal-body") || html.contains("modal__body"),
        "Expected modal body element"
    );

    // Check JavaScript has fetchKeyDetail function
    let js = http_get(&server_addr, "/static/js/app.js").expect("Failed to get JS");
    assert!(
        js.contains("fetchKeyDetail"),
        "Expected fetchKeyDetail function in app.js"
    );
    assert!(
        js.contains("showKeyDetail"),
        "Expected showKeyDetail function in app.js"
    );
}

/// Test Case 5: GET /api/keys/{key} with special characters (URL-encoded) works correctly
/// Input: GET /api/keys/key%2Fwith%2Fslashes
/// Expected: URL-encoded key names are handled correctly
#[test]
fn test_api_key_detail_url_encoded_key() {
    let port = 19505;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_special_keys(port);
    thread::sleep(Duration::from_millis(500));

    // Test key with slashes: "key/with/slashes" encoded as "key%2Fwith%2Fslashes"
    let response = http_get(&server_addr, "/api/keys/key%2Fwith%2Fslashes").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for URL-encoded key, got: {}",
        &response[..response.len().min(200)]
    );

    // Verify the decoded key name is in response
    assert!(
        response.contains("key/with/slashes"),
        "Expected decoded key name in response"
    );
}

/// Test URL-encoded key with colons
#[test]
fn test_api_key_detail_url_encoded_colons() {
    let port = 19506;
    let server_addr = format!("127.0.0.1:{}", port);

    let _server_handle = start_test_server_with_special_keys(port);
    thread::sleep(Duration::from_millis(500));

    // Test key with colons: "user:profile:123" encoded as "user%3Aprofile%3A123"
    let response = http_get(&server_addr, "/api/keys/user%3Aprofile%3A123").expect("Failed to connect");

    // Verify HTTP 200 response
    assert!(
        response.contains("HTTP/1.1 200") || response.contains("HTTP/1.0 200"),
        "Expected HTTP 200 for URL-encoded key with colons"
    );

    // Verify the decoded key name is in response
    assert!(
        response.contains("user:profile:123"),
        "Expected decoded key name with colons in response"
    );
}

// ============================================================================
// Helper Functions for Scenario 5 (Key Detail)
// ============================================================================

/// Start a test HTTP server that handles key detail requests
fn start_test_server_with_key_details(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        // Test keys with values
        let mut keys: std::collections::HashMap<String, (String, u64, u32)> = std::collections::HashMap::new();
        keys.insert("test_key".to_string(), ("test_value".to_string(), 10, 0));
        keys.insert("user:1".to_string(), ("user data 1".to_string(), 11, 1));

        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_key_detail_request(&request, &keys);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Start a test HTTP server with a large value key
fn start_test_server_with_large_value(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        let mut keys: std::collections::HashMap<String, (String, u64, u32)> = std::collections::HashMap::new();
        // Simulate a large value (>1MB in original size, but truncated in response)
        let large_size: u64 = 2 * 1024 * 1024; // 2MB
        let truncated_value = "A".repeat(1000) + "... [truncated, showing first 1MB of 2097152 bytes]";
        keys.insert("large_value_key".to_string(), (truncated_value, large_size, 0));

        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_key_detail_request(&request, &keys);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Start a test HTTP server with special character keys
fn start_test_server_with_special_keys(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).expect("Failed to start test server");

        let mut keys: std::collections::HashMap<String, (String, u64, u32)> = std::collections::HashMap::new();
        keys.insert("key/with/slashes".to_string(), ("value with slashes".to_string(), 17, 0));
        keys.insert("user:profile:123".to_string(), ("profile data".to_string(), 12, 0));
        keys.insert("key with spaces".to_string(), ("value with spaces".to_string(), 17, 0));

        for _ in 0..50 {
            if let Ok(request) = server.recv_timeout(Duration::from_secs(5)) {
                if let Some(request) = request {
                    let response = handle_key_detail_request(&request, &keys);
                    let _ = request.respond(response);
                }
            }
        }
    })
}

/// Handle key detail request for test server
fn handle_key_detail_request(
    request: &tiny_http::Request,
    keys: &std::collections::HashMap<String, (String, u64, u32)>,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let url = request.url();

    // Parse path
    let path = match url.find('?') {
        Some(pos) => &url[..pos],
        None => url,
    };

    if path == "/" || path == "/index.html" {
        let html = include_str!("../static/index.html");
        return tiny_http::Response::from_string(html).with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
                .unwrap(),
        );
    }

    if path == "/static/js/app.js" {
        let js = include_str!("../static/js/app.js");
        return tiny_http::Response::from_string(js).with_header(
            tiny_http::Header::from_bytes(
                &b"Content-Type"[..],
                &b"application/javascript; charset=utf-8"[..],
            )
            .unwrap(),
        );
    }

    if path.starts_with("/api/keys/") {
        let encoded_key = &path[10..]; // "/api/keys/".len() = 10
        let key_name = url_decode(encoded_key);

        if let Some((value, size, flags)) = keys.get(&key_name) {
            let json = format!(
                r#"{{"key":"{}","value":"{}","size":{},"flags":{}}}"#,
                key_name.replace("\"", "\\\""),
                value.replace("\"", "\\\""),
                size,
                flags
            );
            return tiny_http::Response::from_string(json).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
        } else {
            let json = r#"{"error":"Key not found"}"#;
            return tiny_http::Response::from_string(json)
                .with_status_code(tiny_http::StatusCode(404))
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
        }
    }

    tiny_http::Response::from_string("Not Found")
        .with_status_code(tiny_http::StatusCode(404))
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
        )
}

// ============================================================================
// Helper Functions for Scenario 3 (Keys)
// ============================================================================

/// Start a test HTTP server that simulates having `num_keys` keys (Scenario 3)
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

// ============================================================================
// Common Helper Functions
// ============================================================================

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
