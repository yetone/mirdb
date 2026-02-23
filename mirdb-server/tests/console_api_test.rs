//! Console API Integration Tests
//! Owner: Scenario 4 - Interactive Console SET Command
//! Co-owners: Scenario 5 (GET), Scenario 6 (DELETE)
//!
//! These tests verify the /api/console endpoint for executing
//! memcached commands via HTTP.

mod common;

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use common::{find_available_port, send_http_request, HttpTestResponse};

/// Test fixture for managing a mock HTTP server with console API
struct ConsoleApiTestServer {
    http_port: u16,
    _http_thread: Option<thread::JoinHandle<()>>,
}

impl ConsoleApiTestServer {
    fn new() -> Self {
        let http_port = find_available_port();
        ConsoleApiTestServer {
            http_port,
            _http_thread: None,
        }
    }

    fn start(&mut self) {
        let port = self.http_port;

        let handle = thread::spawn(move || {
            // Use the actual HTTP server with store
            use mirdb::http::server::HttpServer;
            use mirdb::store::Store;
            use mirdb::options::Options;

            // Create a temporary directory for the store
            let work_dir = format!("/tmp/mirdb_console_test_{}", port);
            std::fs::create_dir_all(&work_dir).ok();

            let opt = Options {
                work_dir: work_dir.clone(),
                ..Default::default()
            };

            let store = Arc::new(Store::new(opt).expect("Failed to create store"));

            // Start HTTP server
            let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
                .expect("Failed to bind");
            listener.set_nonblocking(true).expect("Cannot set non-blocking");

            let start_time = std::time::Instant::now();
            let timeout = Duration::from_secs(30);

            while start_time.elapsed() < timeout {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0u8; 4096];
                        if let Ok(n) = stream.read(&mut buffer) {
                            if n > 0 {
                                let request_str = String::from_utf8_lossy(&buffer[..n]);
                                let response = handle_http_request(&request_str, &store);
                                let _ = stream.write_all(response.as_bytes());
                                let _ = stream.flush();
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }

            // Cleanup
            std::fs::remove_dir_all(&work_dir).ok();
        });

        self._http_thread = Some(handle);

        // Wait for server to start and verify it's ready
        thread::sleep(Duration::from_millis(100));

        // Try to connect to verify server is ready
        for _ in 0..20 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", self.http_port)).is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Handle HTTP request and return response string
fn handle_http_request(request: &str, store: &Arc<mirdb::store::Store>) -> String {
    use mirdb::http::handlers::{HttpRequest, HttpResponse};
    use mirdb::http::console_api::handle_console_command;

    // Parse the HTTP request
    let lines: Vec<&str> = request.lines().collect();
    if lines.is_empty() {
        return "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".to_string();
    }

    let first_line: Vec<&str> = lines[0].split_whitespace().collect();
    if first_line.len() < 2 {
        return "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".to_string();
    }

    let method = first_line[0];
    let path = first_line[1];

    // Find the body (after empty line)
    let body = if let Some(pos) = request.find("\r\n\r\n") {
        request[pos + 4..].as_bytes().to_vec()
    } else {
        Vec::new()
    };

    let http_request = HttpRequest {
        method: method.to_string(),
        path: path.to_string(),
        headers: vec![],
        body,
    };

    if path == "/api/console" && method == "POST" {
        let response = handle_console_command(&http_request, store);
        format_http_response(&response)
    } else {
        "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found".to_string()
    }
}

/// Format HttpResponse to string
fn format_http_response(response: &mirdb::http::handlers::HttpResponse) -> String {
    let mut result = format!(
        "HTTP/1.1 {} {}\r\n",
        response.status_code, response.status_text
    );

    for (key, value) in &response.headers {
        result.push_str(&format!("{}: {}\r\n", key, value));
    }

    result.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
    result.push_str("\r\n");
    result.push_str(&String::from_utf8_lossy(&response.body));

    result
}

// ============================================================================
// DELETE Command Tests (Scenario 6)
// ============================================================================

/// Test 1: DELETE command on existing key returns DELETED
#[test]
fn test_delete_existing_key_returns_deleted() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // First, set a key
    let set_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set testkey 0 0 5\r\nvalue"),
    ).expect("Failed to send SET request");

    assert_eq!(set_response.status_code, 200, "SET should return 200");
    assert!(
        set_response.body.contains("STORED"),
        "SET should return STORED, got: {}",
        set_response.body
    );

    // Now delete the key
    let delete_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete testkey"),
    ).expect("Failed to send DELETE request");

    assert_eq!(delete_response.status_code, 200, "DELETE should return 200");
    assert!(
        delete_response.body.contains("DELETED"),
        "DELETE should return DELETED response, got: {}",
        delete_response.body
    );
}

/// Test 2: DELETE command on non-existent key returns NOT_FOUND
#[test]
fn test_delete_nonexistent_key_returns_not_found() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // Try to delete a key that doesn't exist
    let delete_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete nonexistent_key_12345"),
    ).expect("Failed to send DELETE request");

    assert_eq!(delete_response.status_code, 200, "DELETE should return 200");
    assert!(
        delete_response.body.contains("NOT_FOUND"),
        "DELETE for non-existent key should return NOT_FOUND, got: {}",
        delete_response.body
    );
}

/// Test 3: GET after DELETE returns END (key successfully removed)
#[test]
fn test_get_after_delete_returns_end() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // First, set a key
    let set_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set deletetest 0 0 9\r\ntestvalue"),
    ).expect("Failed to send SET request");

    assert!(
        set_response.body.contains("STORED"),
        "SET should return STORED"
    );

    // Verify the key exists
    let get_before = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get deletetest"),
    ).expect("Failed to send GET request before delete");

    assert!(
        get_before.body.contains("VALUE") && get_before.body.contains("testvalue"),
        "GET before delete should return the value, got: {}",
        get_before.body
    );

    // Delete the key
    let delete_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete deletetest"),
    ).expect("Failed to send DELETE request");

    assert!(
        delete_response.body.contains("DELETED"),
        "DELETE should return DELETED"
    );

    // Now GET should return just END (no value)
    let get_after = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get deletetest"),
    ).expect("Failed to send GET request after delete");

    assert!(
        get_after.body.contains("END"),
        "GET after delete should contain END, got: {}",
        get_after.body
    );
    assert!(
        !get_after.body.contains("VALUE"),
        "GET after delete should NOT contain VALUE (key was deleted), got: {}",
        get_after.body
    );
}

/// Test 4: DELETE command with noreply flag
#[test]
fn test_delete_with_noreply() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // First, set a key
    let _ = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set noreplytest 0 0 4\r\ndata"),
    ).expect("Failed to send SET request");

    // Delete with noreply (should still work but typically doesn't send response)
    // In HTTP API context, we still get a response
    let delete_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete noreplytest noreply"),
    ).expect("Failed to send DELETE with noreply request");

    assert_eq!(delete_response.status_code, 200, "DELETE with noreply should return 200");

    // Verify the key is actually deleted
    let get_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get noreplytest"),
    ).expect("Failed to send GET request");

    assert!(
        !get_response.body.contains("VALUE"),
        "Key should be deleted after delete noreply, got: {}",
        get_response.body
    );
}

/// Test 5: DELETE command with empty key returns error
#[test]
fn test_delete_empty_key_returns_error() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // Try to delete without specifying a key
    let delete_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete"),
    ).expect("Failed to send DELETE request");

    assert_eq!(delete_response.status_code, 200, "Should return 200");
    assert!(
        delete_response.body.contains("error") || delete_response.body.contains("ERROR") || delete_response.body.contains("CLIENT_ERROR"),
        "DELETE without key should return error, got: {}",
        delete_response.body
    );
}

/// Test 6: Multiple DELETE operations on same key
#[test]
fn test_multiple_deletes_same_key() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // Set a key
    let _ = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set multikey 0 0 3\r\nabc"),
    ).expect("Failed to send SET request");

    // First delete should succeed
    let delete1 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete multikey"),
    ).expect("Failed to send first DELETE request");

    assert!(
        delete1.body.contains("DELETED"),
        "First DELETE should return DELETED, got: {}",
        delete1.body
    );

    // Second delete should return NOT_FOUND
    let delete2 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete multikey"),
    ).expect("Failed to send second DELETE request");

    assert!(
        delete2.body.contains("NOT_FOUND"),
        "Second DELETE should return NOT_FOUND, got: {}",
        delete2.body
    );
}

/// Test 7: DELETE followed by SET and DELETE again
#[test]
fn test_delete_set_delete_cycle() {
    let mut server = ConsoleApiTestServer::new();
    server.start();

    // Set initial value
    let _ = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set cyclekey 0 0 6\r\nvalue1"),
    );

    // Delete
    let delete1 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete cyclekey"),
    ).expect("Failed to send DELETE request");

    assert!(delete1.body.contains("DELETED"));

    // Set again with new value
    let set2 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set cyclekey 0 0 6\r\nvalue2"),
    ).expect("Failed to send SET request");

    assert!(set2.body.contains("STORED"));

    // Verify new value exists
    let get = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get cyclekey"),
    ).expect("Failed to send GET request");

    assert!(
        get.body.contains("value2"),
        "Should contain new value after re-set, got: {}",
        get.body
    );

    // Delete again
    let delete2 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("delete cyclekey"),
    ).expect("Failed to send DELETE request");

    assert!(
        delete2.body.contains("DELETED"),
        "Final DELETE should return DELETED"
    );
}
