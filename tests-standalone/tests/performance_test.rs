//! Performance tests for MirDB HTTP server (Scenario 9)
//!
//! These tests verify that the HTTP server meets performance requirements:
//! - NFR-1: Homepage shall respond within 100ms p95
//! - NFR-2: Support 50 concurrent visitors without degradation
//! - NFR-5: HTTP shall not impact memcached performance

use std::fs::{create_dir_all, remove_dir_all};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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

/// Send an HTTP GET request and return the response time and status
fn send_http_get_timed(host: &str, port: u16, path: &str) -> std::io::Result<(Duration, u16, usize)> {
    let start = Instant::now();

    let mut stream = TcpStream::connect(format!("{}:{}", host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: {}:{}\r\n\
         Connection: close\r\n\
         \r\n",
        path, host, port
    );

    stream.write_all(request.as_bytes())?;
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
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    let elapsed = start.elapsed();
    Ok((elapsed, status_code, body.len()))
}

/// Send an HTTP POST request to the console API and return timing
fn send_http_post_timed(host: &str, port: u16, path: &str, body: &str) -> std::io::Result<(Duration, u16)> {
    let start = Instant::now();

    let mut stream = TcpStream::connect(format!("{}:{}", host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let request = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}:{}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        path, host, port, body.len(), body
    );

    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);

    // Read status line
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;

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

    let elapsed = start.elapsed();
    Ok((elapsed, status_code))
}

/// Calculate p95 from a sorted slice of durations
fn calculate_p95(durations: &mut [Duration]) -> Duration {
    if durations.is_empty() {
        return Duration::ZERO;
    }
    durations.sort();
    let idx = (durations.len() as f64 * 0.95).ceil() as usize - 1;
    durations[idx.min(durations.len() - 1)]
}

/// Test harness that sets up the HTTP server for performance testing
struct TestHarness {
    port: u16,
    store: Arc<Store>,
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

        TestHarness { port, store }
    }

    fn post_console(&self, command: &str) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", self.port))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;

        let request = format!(
            "POST /api/console HTTP/1.1\r\n\
             Host: 127.0.0.1:{}\r\n\
             Content-Type: text/plain\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {}",
            self.port,
            command.len(),
            command
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
}

// ============================================================================
// Test Case 1: 100 sequential HTTP requests to homepage - p95 < 100ms
// NFR-1: Homepage shall respond within 100ms p95
// ============================================================================
#[test]
fn test_p95_response_time_under_100ms() {
    let harness = TestHarness::new();

    let mut durations: Vec<Duration> = Vec::with_capacity(100);
    let mut success_count = 0;

    // Send 100 sequential requests
    for _ in 0..100 {
        match send_http_get_timed("127.0.0.1", harness.port, "/") {
            Ok((duration, status_code, _)) => {
                if status_code == 200 {
                    success_count += 1;
                    durations.push(duration);
                }
            }
            Err(e) => {
                eprintln!("Request failed: {}", e);
            }
        }
    }

    // Calculate p95
    let p95 = calculate_p95(&mut durations);

    println!("Performance Test Results:");
    println!("  Total requests: 100");
    println!("  Successful requests: {}", success_count);
    println!("  p95 response time: {:?}", p95);

    // Verify all requests succeeded
    assert!(
        success_count >= 95,
        "At least 95% of requests should succeed. Got: {}/100",
        success_count
    );

    // Verify p95 is under 100ms
    assert!(
        p95 < Duration::from_millis(100),
        "p95 response time should be under 100ms. Got: {:?}",
        p95
    );
}

// ============================================================================
// Test Case 2: 50 concurrent HTTP connections sending requests
// NFR-2: Support 50 concurrent visitors without degradation
// ============================================================================
#[test]
fn test_50_concurrent_connections() {
    let harness = TestHarness::new();
    let port = harness.port;

    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(50);

    // Spawn 50 concurrent threads
    for _ in 0..50 {
        let success_count_clone = success_count.clone();
        let error_count_clone = error_count.clone();

        let handle = thread::spawn(move || {
            // Each thread sends multiple requests
            for _ in 0..5 {
                match send_http_get_timed("127.0.0.1", port, "/") {
                    Ok((_, status_code, _)) => {
                        if status_code == 200 {
                            success_count_clone.fetch_add(1, Ordering::SeqCst);
                        } else {
                            error_count_clone.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Err(_) => {
                        error_count_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
                // Small delay between requests from same thread
                thread::sleep(Duration::from_millis(10));
            }
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join();
    }

    let total_success = success_count.load(Ordering::SeqCst);
    let total_errors = error_count.load(Ordering::SeqCst);
    let total_requests = total_success + total_errors;

    println!("Concurrent Connections Test Results:");
    println!("  Total concurrent connections: 50");
    println!("  Requests per connection: 5");
    println!("  Total requests: {}", total_requests);
    println!("  Successful requests: {}", total_success);
    println!("  Failed requests: {}", total_errors);

    // Verify all requests succeeded (allowing for minimal failures due to timing)
    let success_rate = total_success as f64 / total_requests as f64;
    assert!(
        success_rate >= 0.98,
        "At least 98% of concurrent requests should succeed. Got: {:.2}% ({}/{})",
        success_rate * 100.0,
        total_success,
        total_requests
    );
}

// ============================================================================
// Test Case 3: Memcached SET/GET operations during 50 concurrent HTTP requests
// NFR-5: HTTP shall not impact memcached performance
// ============================================================================
#[test]
fn test_memcached_performance_during_http_load() {
    let harness = TestHarness::new();
    let port = harness.port;
    let store = harness.store.clone();

    // First, establish baseline memcached performance (without HTTP load)
    let baseline_durations = measure_store_operations(&store, 50);
    let baseline_p95 = calculate_p95(&mut baseline_durations.clone());

    println!("Baseline memcached p95 (no HTTP load): {:?}", baseline_p95);

    // Start HTTP load in background threads
    let http_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let http_running_clone = http_running.clone();

    let mut http_handles = Vec::new();
    for _ in 0..50 {
        let running = http_running_clone.clone();
        let handle = thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                let _ = send_http_get_timed("127.0.0.1", port, "/");
                thread::sleep(Duration::from_millis(20));
            }
        });
        http_handles.push(handle);
    }

    // Give HTTP load time to stabilize
    thread::sleep(Duration::from_millis(100));

    // Measure memcached performance under HTTP load
    let loaded_durations = measure_store_operations(&store, 50);
    let loaded_p95 = calculate_p95(&mut loaded_durations.clone());

    // Stop HTTP load
    http_running.store(false, Ordering::SeqCst);
    for handle in http_handles {
        let _ = handle.join();
    }

    println!("Memcached p95 under HTTP load: {:?}", loaded_p95);

    // Verify that memcached performance hasn't degraded significantly
    // Allow up to 3x increase in latency under load (reasonable for concurrent access)
    let max_allowed = baseline_p95.as_nanos() * 3;
    assert!(
        loaded_p95.as_nanos() <= max_allowed.max(Duration::from_millis(50).as_nanos()),
        "Memcached performance should not degrade significantly under HTTP load. \
         Baseline p95: {:?}, Loaded p95: {:?}",
        baseline_p95,
        loaded_p95
    );
}

/// Helper function to measure store operations performance
fn measure_store_operations(store: &Arc<Store>, count: usize) -> Vec<Duration> {
    use mirdb::request::{Request, SetterType, GetterType};
    use mirdb::slice::Slice;

    let mut durations = Vec::with_capacity(count * 2);

    for i in 0..count {
        let key = format!("perftest_key_{}", i);
        let value = format!("perftest_value_{}", i);

        // Time SET operation
        let start = Instant::now();
        let _ = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from(key.as_bytes().to_vec()),
            flags: 0,
            ttl: 0,
            bytes: value.len(),
            payload: Slice::from(value.as_bytes().to_vec()),
            no_reply: false,
        });
        durations.push(start.elapsed());

        // Time GET operation
        let start = Instant::now();
        let _ = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![Slice::from(key.as_bytes().to_vec())],
        });
        durations.push(start.elapsed());
    }

    durations
}

// ============================================================================
// Test Case 4: Console API command response time - within 200ms
// ============================================================================
#[test]
fn test_console_api_response_time_under_200ms() {
    let harness = TestHarness::new();

    let mut durations: Vec<Duration> = Vec::with_capacity(50);
    let mut success_count = 0;

    // Test various console commands
    let commands = vec![
        "set perfkey 0 0 5\r\nhello\r\n",
        "get perfkey",
        "set perfkey2 0 0 5\r\nworld\r\n",
        "get perfkey2",
        "delete perfkey",
    ];

    // Run multiple iterations
    for _ in 0..10 {
        for cmd in &commands {
            match send_http_post_timed("127.0.0.1", harness.port, "/api/console", cmd) {
                Ok((duration, status_code)) => {
                    if status_code == 200 {
                        success_count += 1;
                        durations.push(duration);
                    }
                }
                Err(e) => {
                    eprintln!("Console API request failed: {}", e);
                }
            }
        }
    }

    // Calculate p95
    let p95 = calculate_p95(&mut durations);

    println!("Console API Performance Test Results:");
    println!("  Total requests: 50");
    println!("  Successful requests: {}", success_count);
    println!("  p95 response time: {:?}", p95);

    // Verify p95 is under 200ms
    assert!(
        p95 < Duration::from_millis(200),
        "Console API p95 response time should be under 200ms. Got: {:?}",
        p95
    );
}

// ============================================================================
// Additional performance tests for comprehensive coverage
// ============================================================================

#[test]
fn test_homepage_css_js_asset_response_time() {
    let harness = TestHarness::new();

    let assets = vec!["/", "/styles.css", "/console.js"];
    let mut all_durations: Vec<Duration> = Vec::new();

    for path in &assets {
        for _ in 0..20 {
            if let Ok((duration, status_code, _)) = send_http_get_timed("127.0.0.1", harness.port, path) {
                if status_code == 200 {
                    all_durations.push(duration);
                }
            }
        }
    }

    let p95 = calculate_p95(&mut all_durations);

    println!("Asset Serving Performance:");
    println!("  p95 across all assets: {:?}", p95);

    assert!(
        p95 < Duration::from_millis(100),
        "Static asset serving p95 should be under 100ms. Got: {:?}",
        p95
    );
}

#[test]
fn test_sustained_load_no_degradation() {
    let harness = TestHarness::new();

    // Test that response times don't degrade over sustained load
    let mut early_durations: Vec<Duration> = Vec::new();
    let mut late_durations: Vec<Duration> = Vec::new();

    // Early requests (first 50)
    for _ in 0..50 {
        if let Ok((duration, status_code, _)) = send_http_get_timed("127.0.0.1", harness.port, "/") {
            if status_code == 200 {
                early_durations.push(duration);
            }
        }
    }

    // Continue with more load
    for _ in 0..100 {
        let _ = send_http_get_timed("127.0.0.1", harness.port, "/");
    }

    // Late requests (last 50)
    for _ in 0..50 {
        if let Ok((duration, status_code, _)) = send_http_get_timed("127.0.0.1", harness.port, "/") {
            if status_code == 200 {
                late_durations.push(duration);
            }
        }
    }

    let early_p95 = calculate_p95(&mut early_durations);
    let late_p95 = calculate_p95(&mut late_durations);

    println!("Sustained Load Test Results:");
    println!("  Early p95: {:?}", early_p95);
    println!("  Late p95: {:?}", late_p95);

    // Late p95 should not be more than 2x the early p95
    let max_allowed = early_p95.as_nanos() * 2;
    assert!(
        late_p95.as_nanos() <= max_allowed.max(Duration::from_millis(100).as_nanos()),
        "Response times should not degrade over sustained load. Early p95: {:?}, Late p95: {:?}",
        early_p95,
        late_p95
    );
}
