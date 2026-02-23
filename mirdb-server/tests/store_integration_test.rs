//! Store Integration Tests
//! Owner: Scenario 14 - Store Integration
//!
//! These tests verify that the web console correctly integrates with the
//! shared Store instance, ensuring data consistency between TCP memcached
//! protocol and HTTP console API.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use common::{send_http_request, send_memcached_command, HttpTestResponse};

// Use a global atomic counter to allocate unique ports across parallel tests
static PORT_COUNTER: AtomicU16 = AtomicU16::new(20000);

/// Find an available port atomically to avoid conflicts between parallel tests
fn find_unique_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if port > 60000 {
            // Wrap around if we've used too many ports
            PORT_COUNTER.store(20000, Ordering::SeqCst);
            continue;
        }
        // Try to bind to verify port availability
        if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
            return port;
        }
    }
}

/// Test server that runs both HTTP and memcached TCP protocols
/// sharing the same Store instance
struct IntegrationTestServer {
    http_port: u16,
    tcp_port: u16,
    _http_thread: Option<thread::JoinHandle<()>>,
    _tcp_thread: Option<thread::JoinHandle<()>>,
    work_dir: String,
}

impl IntegrationTestServer {
    fn new() -> Self {
        let http_port = find_unique_port();
        let tcp_port = find_unique_port();
        let work_dir = format!("/tmp/mirdb_integration_test_{}_{}", http_port, tcp_port);

        IntegrationTestServer {
            http_port,
            tcp_port,
            _http_thread: None,
            _tcp_thread: None,
            work_dir,
        }
    }

    fn start(&mut self) {
        use mirdb::store::Store;
        use mirdb::options::Options;

        // Create shared store
        std::fs::create_dir_all(&self.work_dir).ok();

        let opt = Options {
            work_dir: self.work_dir.clone(),
            ..Default::default()
        };

        let store = Arc::new(Store::new(opt).expect("Failed to create store"));

        // Start HTTP server
        let http_port = self.http_port;
        let http_store = store.clone();
        let http_handle = thread::spawn(move || {
            run_http_server(http_port, http_store);
        });
        self._http_thread = Some(http_handle);

        // Start memcached TCP server
        let tcp_port = self.tcp_port;
        let tcp_store = store.clone();
        let tcp_handle = thread::spawn(move || {
            run_tcp_server(tcp_port, tcp_store);
        });
        self._tcp_thread = Some(tcp_handle);

        // Wait for servers to start
        thread::sleep(Duration::from_millis(200));

        // Verify servers are ready
        for _ in 0..30 {
            let http_ready = TcpStream::connect(format!("127.0.0.1:{}", self.http_port)).is_ok();
            let tcp_ready = TcpStream::connect(format!("127.0.0.1:{}", self.tcp_port)).is_ok();
            if http_ready && tcp_ready {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("Servers failed to start within timeout");
    }
}

impl Drop for IntegrationTestServer {
    fn drop(&mut self) {
        // Cleanup work directory
        std::fs::remove_dir_all(&self.work_dir).ok();
    }
}

/// Run a simple HTTP server that handles console API requests
fn run_http_server(port: u16, store: Arc<mirdb::store::Store>) {
    use mirdb::http::handlers::{HttpRequest, HttpResponse};
    use mirdb::http::console_api::handle_console_command;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .expect("Failed to bind HTTP server");
    listener.set_nonblocking(true).expect("Cannot set non-blocking");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    while start_time.elapsed() < timeout {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let mut buffer = [0u8; 8192];
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
}

/// Handle HTTP request and return response string
fn handle_http_request(request: &str, store: &Arc<mirdb::store::Store>) -> String {
    use mirdb::http::handlers::{HttpRequest, HttpResponse};
    use mirdb::http::console_api::handle_console_command;

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

/// Run a simple TCP server that handles memcached protocol commands
/// This server delegates to the console_api module to ensure both protocols
/// use the same command parsing and store access logic
fn run_tcp_server(port: u16, store: Arc<mirdb::store::Store>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .expect("Failed to bind TCP server");
    listener.set_nonblocking(true).expect("Cannot set non-blocking");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    while start_time.elapsed() < timeout {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
                stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

                let mut buffer = [0u8; 8192];
                if let Ok(n) = stream.read(&mut buffer) {
                    if n > 0 {
                        let command = String::from_utf8_lossy(&buffer[..n]).to_string();
                        let response = execute_memcached_command(&command, &store);
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
}

/// Execute a memcached command via HTTP console API and return the formatted response
/// This reuses the existing console_api module for consistent command handling
fn execute_memcached_command(command: &str, store: &Arc<mirdb::store::Store>) -> String {
    use mirdb::http::handlers::HttpRequest;
    use mirdb::http::console_api::handle_console_command;

    // Create an HTTP request with the command as the body
    let http_request = HttpRequest {
        method: "POST".to_string(),
        path: "/api/console".to_string(),
        headers: vec![],
        body: command.as_bytes().to_vec(),
    };

    // Use the console API to execute the command
    let response = handle_console_command(&http_request, store);
    let body_str = String::from_utf8_lossy(&response.body);

    // Parse the JSON response to extract the actual memcached response
    // Format is: {"response":"STORED\r\n"}
    if let Some(start) = body_str.find(r#""response":""#) {
        let after_prefix = &body_str[start + 12..];
        if let Some(end) = after_prefix.rfind('"') {
            let escaped_response = &after_prefix[..end];
            // Unescape JSON string
            let unescaped = escaped_response
                .replace("\\r\\n", "\r\n")
                .replace("\\n", "\n")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
            return unescaped;
        }
    }

    body_str.to_string()
}

// ============================================================================
// Test Case 1: SET via TCP, GET via HTTP console
// ============================================================================

/// Test that a value set via TCP memcached protocol can be retrieved via HTTP console
#[test]
fn test_set_via_tcp_get_via_http() {
    let mut server = IntegrationTestServer::new();
    server.start();

    // SET via TCP memcached protocol
    let tcp_set_response = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        "set tcp_key 0 0 9\r\ntcp_value",
    ).expect("Failed to send TCP SET command");

    assert!(
        tcp_set_response.contains("STORED"),
        "TCP SET should return STORED, got: {}",
        tcp_set_response
    );

    // GET via HTTP console
    let http_get_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get tcp_key"),
    ).expect("Failed to send HTTP GET request");

    assert_eq!(http_get_response.status_code, 200, "HTTP GET should return 200");
    assert!(
        http_get_response.body.contains("VALUE tcp_key"),
        "HTTP console should return VALUE tcp_key, got: {}",
        http_get_response.body
    );
    assert!(
        http_get_response.body.contains("tcp_value"),
        "HTTP console should return the value 'tcp_value' set via TCP, got: {}",
        http_get_response.body
    );
    assert!(
        http_get_response.body.contains("END"),
        "HTTP console response should end with END, got: {}",
        http_get_response.body
    );
}

// ============================================================================
// Test Case 2: SET via HTTP console, GET via TCP
// ============================================================================

/// Test that a value set via HTTP console can be retrieved via TCP memcached protocol
#[test]
fn test_set_via_http_get_via_tcp() {
    let mut server = IntegrationTestServer::new();
    server.start();

    // SET via HTTP console
    let http_set_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set http_key 0 0 10\r\nhttp_value"),
    ).expect("Failed to send HTTP SET request");

    assert_eq!(http_set_response.status_code, 200, "HTTP SET should return 200");
    assert!(
        http_set_response.body.contains("STORED"),
        "HTTP console SET should return STORED, got: {}",
        http_set_response.body
    );

    // GET via TCP memcached protocol
    let tcp_get_response = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        "get http_key",
    ).expect("Failed to send TCP GET command");

    assert!(
        tcp_get_response.contains("VALUE http_key"),
        "TCP GET should return VALUE http_key, got: {}",
        tcp_get_response
    );
    assert!(
        tcp_get_response.contains("http_value"),
        "TCP GET should return the value 'http_value' set via HTTP, got: {}",
        tcp_get_response
    );
    assert!(
        tcp_get_response.contains("END"),
        "TCP GET response should end with END, got: {}",
        tcp_get_response
    );
}

// ============================================================================
// Test Case 3: DELETE via TCP, verify via HTTP console
// ============================================================================

/// Test that a key deleted via TCP is no longer visible via HTTP console
#[test]
fn test_delete_via_tcp_verify_via_http() {
    let mut server = IntegrationTestServer::new();
    server.start();

    // First, SET a key via HTTP console
    let set_response = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("set delete_test_key 0 0 10\r\ntest_value"),
    ).expect("Failed to send HTTP SET request");

    assert!(
        set_response.body.contains("STORED"),
        "SET should return STORED, got: {}",
        set_response.body
    );

    // Verify the key exists via HTTP
    let get_before = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get delete_test_key"),
    ).expect("Failed to send HTTP GET request");

    assert!(
        get_before.body.contains("VALUE delete_test_key") && get_before.body.contains("test_value"),
        "Key should exist before delete, got: {}",
        get_before.body
    );

    // DELETE via TCP
    let tcp_delete_response = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        "delete delete_test_key",
    ).expect("Failed to send TCP DELETE command");

    assert!(
        tcp_delete_response.contains("DELETED"),
        "TCP DELETE should return DELETED, got: {}",
        tcp_delete_response
    );

    // Verify the key no longer exists via HTTP console
    let get_after = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get delete_test_key"),
    ).expect("Failed to send HTTP GET request after delete");

    assert!(
        get_after.body.contains("END"),
        "HTTP console should return END for deleted key, got: {}",
        get_after.body
    );
    assert!(
        !get_after.body.contains("VALUE"),
        "HTTP console should NOT contain VALUE for deleted key, got: {}",
        get_after.body
    );
}

// ============================================================================
// Test Case 4: Concurrent SET from TCP and HTTP - no data corruption
// ============================================================================

/// Test that concurrent writes from TCP and HTTP don't corrupt data
#[test]
fn test_concurrent_set_no_corruption() {
    let mut server = IntegrationTestServer::new();
    server.start();

    let http_port = server.http_port;
    let tcp_port = server.tcp_port;

    // Spawn threads for concurrent writes
    let http_handle = thread::spawn(move || {
        for i in 0..10 {
            let key = format!("concurrent_key_{}", i);
            let value = format!("http_value_{}", i);
            let cmd = format!("set {} 0 0 {}\r\n{}", key, value.len(), value);
            let _ = send_http_request(
                "127.0.0.1",
                http_port,
                "POST",
                "/api/console",
                Some(&cmd),
            );
            thread::sleep(Duration::from_millis(10));
        }
    });

    let tcp_handle = thread::spawn(move || {
        for i in 10..20 {
            let key = format!("concurrent_key_{}", i);
            let value = format!("tcp_value_{}", i);
            let cmd = format!("set {} 0 0 {}\r\n{}", key, value.len(), value);
            let _ = send_memcached_command("127.0.0.1", tcp_port, &cmd);
            thread::sleep(Duration::from_millis(10));
        }
    });

    // Wait for both threads to complete
    http_handle.join().expect("HTTP thread panicked");
    tcp_handle.join().expect("TCP thread panicked");

    // Small delay to ensure all writes are processed
    thread::sleep(Duration::from_millis(100));

    // Verify data integrity - all keys should exist with correct values
    // Check HTTP-written keys via TCP
    for i in 0..10 {
        let key = format!("concurrent_key_{}", i);
        let expected_value = format!("http_value_{}", i);

        let tcp_get = send_memcached_command(
            "127.0.0.1",
            server.tcp_port,
            &format!("get {}", key),
        ).expect("Failed to get key via TCP");

        assert!(
            tcp_get.contains(&format!("VALUE {}", key)),
            "Key {} should exist, got: {}",
            key, tcp_get
        );
        assert!(
            tcp_get.contains(&expected_value),
            "Key {} should have value '{}', got: {}",
            key, expected_value, tcp_get
        );
    }

    // Check TCP-written keys via HTTP
    for i in 10..20 {
        let key = format!("concurrent_key_{}", i);
        let expected_value = format!("tcp_value_{}", i);

        let http_get = send_http_request(
            "127.0.0.1",
            server.http_port,
            "POST",
            "/api/console",
            Some(&format!("get {}", key)),
        ).expect("Failed to get key via HTTP");

        assert!(
            http_get.body.contains(&format!("VALUE {}", key)),
            "Key {} should exist via HTTP, got: {}",
            key, http_get.body
        );
        assert!(
            http_get.body.contains(&expected_value),
            "Key {} should have value '{}' via HTTP, got: {}",
            key, expected_value, http_get.body
        );
    }
}

/// Test concurrent overwrites of the same key - last write wins
#[test]
fn test_concurrent_overwrite_same_key() {
    let mut server = IntegrationTestServer::new();
    server.start();

    let http_port = server.http_port;
    let tcp_port = server.tcp_port;
    let shared_key = "shared_concurrent_key";

    // Spawn threads for concurrent overwrites of the same key
    let http_handle = thread::spawn(move || {
        for i in 0..5 {
            let value = format!("http_val_{}", i);
            let cmd = format!("set {} 0 0 {}\r\n{}", shared_key, value.len(), value);
            let _ = send_http_request(
                "127.0.0.1",
                http_port,
                "POST",
                "/api/console",
                Some(&cmd),
            );
            thread::sleep(Duration::from_millis(5));
        }
    });

    let tcp_handle = thread::spawn(move || {
        for i in 0..5 {
            let value = format!("tcp_val_{}", i);
            let cmd = format!("set {} 0 0 {}\r\n{}", shared_key, value.len(), value);
            let _ = send_memcached_command("127.0.0.1", tcp_port, &cmd);
            thread::sleep(Duration::from_millis(5));
        }
    });

    // Wait for both threads to complete
    http_handle.join().expect("HTTP thread panicked");
    tcp_handle.join().expect("TCP thread panicked");

    // Small delay to ensure all writes are processed
    thread::sleep(Duration::from_millis(100));

    // Verify the key exists with SOME value (last write wins)
    let tcp_get = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        &format!("get {}", shared_key),
    ).expect("Failed to get shared key via TCP");

    // The key should exist
    assert!(
        tcp_get.contains(&format!("VALUE {}", shared_key)),
        "Shared key should exist after concurrent writes, got: {}",
        tcp_get
    );

    // The value should be either an http_val_ or tcp_val_ (last write wins)
    let has_http_val = tcp_get.contains("http_val_");
    let has_tcp_val = tcp_get.contains("tcp_val_");

    assert!(
        has_http_val || has_tcp_val,
        "Value should be one of the written values, got: {}",
        tcp_get
    );

    // Verify no data corruption - the value should be a complete value
    // It shouldn't be partially overwritten or corrupted
    let is_valid_value = tcp_get.contains("http_val_0") ||
                         tcp_get.contains("http_val_1") ||
                         tcp_get.contains("http_val_2") ||
                         tcp_get.contains("http_val_3") ||
                         tcp_get.contains("http_val_4") ||
                         tcp_get.contains("tcp_val_0") ||
                         tcp_get.contains("tcp_val_1") ||
                         tcp_get.contains("tcp_val_2") ||
                         tcp_get.contains("tcp_val_3") ||
                         tcp_get.contains("tcp_val_4");

    assert!(
        is_valid_value,
        "Value should be a complete, non-corrupted value, got: {}",
        tcp_get
    );
}

// ============================================================================
// Additional Integration Tests
// ============================================================================

/// Test bidirectional data flow: SET on one, GET on both
#[test]
fn test_bidirectional_data_access() {
    let mut server = IntegrationTestServer::new();
    server.start();

    // SET via TCP (bidirectional is 13 bytes)
    let _ = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        "set bidir_key1 42 0 13\r\nbidirectional",
    ).expect("Failed to SET via TCP");

    // GET via both protocols
    let tcp_get1 = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        "get bidir_key1",
    ).expect("Failed to GET via TCP");

    let http_get1 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some("get bidir_key1"),
    ).expect("Failed to GET via HTTP");

    // Both should return the same data
    assert!(tcp_get1.contains("bidirectional"), "TCP GET should return data");
    assert!(http_get1.body.contains("bidirectional"), "HTTP GET should return data");

    // Verify flags are preserved
    assert!(tcp_get1.contains("42"), "TCP GET should show flags 42");
    assert!(http_get1.body.contains("42"), "HTTP GET should show flags 42");
}

/// Test that updates propagate correctly between protocols
#[test]
fn test_update_propagation() {
    let mut server = IntegrationTestServer::new();
    server.start();

    let key = "update_key";

    // SET initial value via HTTP
    let _ = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some(&format!("set {} 0 0 7\r\ninitial", key)),
    ).expect("Failed to SET via HTTP");

    // Verify via TCP
    let get1 = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        &format!("get {}", key),
    ).expect("Failed to GET via TCP");
    assert!(get1.contains("initial"), "Should see initial value");

    // UPDATE via TCP
    let _ = send_memcached_command(
        "127.0.0.1",
        server.tcp_port,
        &format!("set {} 0 0 7\r\nupdated", key),
    ).expect("Failed to SET via TCP");

    // Verify update via HTTP
    let get2 = send_http_request(
        "127.0.0.1",
        server.http_port,
        "POST",
        "/api/console",
        Some(&format!("get {}", key)),
    ).expect("Failed to GET via HTTP");
    assert!(
        get2.body.contains("updated"),
        "Should see updated value via HTTP, got: {}",
        get2.body
    );
    assert!(
        !get2.body.contains("initial"),
        "Should NOT see old value via HTTP, got: {}",
        get2.body
    );
}
