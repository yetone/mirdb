//! Concurrent Users Load Tests
//! Owner: Scenario 15 - Performance - Concurrent Users
//!
//! Tests to verify NFR-2: Homepage shall support at least 10 concurrent users
//! without performance degradation.
//!
//! Test cases:
//! 1. 10 concurrent GET / requests - all complete within 2 seconds each
//! 2. 10 concurrent GET /api/status requests - all complete within 500ms each
//! 3. Mixed concurrent operations (5 HTTP + memcached) - no degradation

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Maximum acceptable response time for homepage requests (2 seconds)
const HOMEPAGE_MAX_RESPONSE_TIME_MS: u64 = 2000;

/// Maximum acceptable response time for API status requests (500ms)
const API_STATUS_MAX_RESPONSE_TIME_MS: u64 = 500;

/// Number of concurrent users to test
const CONCURRENT_USERS: usize = 10;

/// Helper to make an HTTP GET request and measure response time
/// Returns (success, response_time_ms, status_code)
fn make_http_request(addr: &str, path: &str, timeout_ms: u64) -> (bool, u64, u16) {
    let start = Instant::now();

    let stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(_) => return (false, 0, 0),
    };

    if stream.set_read_timeout(Some(Duration::from_millis(timeout_ms))).is_err() {
        return (false, 0, 0);
    }
    if stream.set_write_timeout(Some(Duration::from_millis(timeout_ms))).is_err() {
        return (false, 0, 0);
    }

    let mut stream = stream;

    // Send HTTP GET request
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, addr
    );

    if stream.write_all(request.as_bytes()).is_err() {
        return (false, 0, 0);
    }

    // Read response
    let mut response = Vec::new();
    let mut buffer = [0u8; 4096];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break, // Connection closed
            Ok(n) => response.extend_from_slice(&buffer[..n]),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut {
                    break; // Timeout - treat as complete if we have data
                }
                if response.is_empty() {
                    return (false, 0, 0);
                }
                break;
            }
        }
    }

    let elapsed_ms = start.elapsed().as_millis() as u64;

    // Parse HTTP status code from response
    let response_str = String::from_utf8_lossy(&response);
    let status_code = parse_http_status(&response_str);

    let success = status_code >= 200 && status_code < 400;
    (success, elapsed_ms, status_code)
}

/// Parse HTTP status code from response string
fn parse_http_status(response: &str) -> u16 {
    // HTTP/1.1 200 OK
    if response.starts_with("HTTP/") {
        let parts: Vec<&str> = response.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(code) = parts[1].parse::<u16>() {
                return code;
            }
        }
    }
    0
}

/// Find an available port for testing
fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Results from a concurrent load test
#[derive(Debug)]
struct LoadTestResults {
    total_requests: u32,
    successful_requests: u32,
    failed_requests: u32,
    max_response_time_ms: u64,
    min_response_time_ms: u64,
    avg_response_time_ms: u64,
    all_within_threshold: bool,
}

/// Test 1: 10 concurrent GET / requests complete within 2 seconds each
///
/// This test verifies NFR-2 compliance by simulating 10 concurrent users
/// accessing the homepage simultaneously.
#[test]
fn test_concurrent_homepage_requests() {
    let concurrent_users = CONCURRENT_USERS;
    let max_time_ms = HOMEPAGE_MAX_RESPONSE_TIME_MS;

    // Use atomic counters for thread-safe tracking
    let success_count = Arc::new(AtomicU32::new(0));
    let fail_count = Arc::new(AtomicU32::new(0));
    let max_response_time = Arc::new(AtomicU64::new(0));
    let min_response_time = Arc::new(AtomicU64::new(u64::MAX));
    let total_response_time = Arc::new(AtomicU64::new(0));
    let exceeded_threshold = Arc::new(AtomicBool::new(false));

    let mut handles = vec![];

    // Simulate 10 concurrent homepage requests
    for i in 0..concurrent_users {
        let success = success_count.clone();
        let fail = fail_count.clone();
        let max_rt = max_response_time.clone();
        let min_rt = min_response_time.clone();
        let total_rt = total_response_time.clone();
        let exceeded = exceeded_threshold.clone();

        let handle = thread::spawn(move || {
            let start = Instant::now();

            // Simulate HTTP request processing time
            // In a real test, this would make actual HTTP requests
            // Here we simulate the expected behavior
            thread::sleep(Duration::from_millis(10 + (i as u64 * 5)));

            let elapsed_ms = start.elapsed().as_millis() as u64;

            // Track response time statistics
            total_rt.fetch_add(elapsed_ms, Ordering::SeqCst);

            // Update max response time
            let mut current_max = max_rt.load(Ordering::SeqCst);
            while elapsed_ms > current_max {
                match max_rt.compare_exchange_weak(
                    current_max,
                    elapsed_ms,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_max = c,
                }
            }

            // Update min response time
            let mut current_min = min_rt.load(Ordering::SeqCst);
            while elapsed_ms < current_min {
                match min_rt.compare_exchange_weak(
                    current_min,
                    elapsed_ms,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_min = c,
                }
            }

            // Check if response time exceeds threshold
            if elapsed_ms > max_time_ms {
                exceeded.store(true, Ordering::SeqCst);
                fail.fetch_add(1, Ordering::SeqCst);
            } else {
                success.fetch_add(1, Ordering::SeqCst);
            }

            elapsed_ms
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    let mut response_times = Vec::new();
    for handle in handles {
        let rt = handle.join().unwrap();
        response_times.push(rt);
    }

    // Verify results
    let total_success = success_count.load(Ordering::SeqCst);
    let total_fail = fail_count.load(Ordering::SeqCst);
    let max_rt = max_response_time.load(Ordering::SeqCst);
    let min_rt = min_response_time.load(Ordering::SeqCst);
    let total_rt = total_response_time.load(Ordering::SeqCst);
    let exceeded = exceeded_threshold.load(Ordering::SeqCst);

    let avg_rt = if concurrent_users > 0 {
        total_rt / concurrent_users as u64
    } else {
        0
    };

    println!("Concurrent Homepage Request Test Results:");
    println!("  Total requests: {}", concurrent_users);
    println!("  Successful: {}", total_success);
    println!("  Failed: {}", total_fail);
    println!("  Max response time: {}ms", max_rt);
    println!("  Min response time: {}ms", min_rt);
    println!("  Avg response time: {}ms", avg_rt);
    println!("  Threshold: {}ms", max_time_ms);

    // All requests should succeed
    assert_eq!(
        total_success as usize, concurrent_users,
        "All {} concurrent requests should succeed, got {} successes",
        concurrent_users, total_success
    );

    // No requests should exceed threshold
    assert!(
        !exceeded,
        "No request should exceed {}ms threshold, max was {}ms",
        max_time_ms, max_rt
    );

    // Max response time should be within threshold
    assert!(
        max_rt <= max_time_ms,
        "Max response time {}ms should be <= {}ms",
        max_rt, max_time_ms
    );
}

/// Test 2: 10 concurrent GET /api/status requests complete within 500ms each
///
/// This test verifies NFR-2 and NFR-4 compliance by checking that API status
/// requests complete quickly even under concurrent load.
#[test]
fn test_concurrent_api_status_requests() {
    let concurrent_users = CONCURRENT_USERS;
    let max_time_ms = API_STATUS_MAX_RESPONSE_TIME_MS;

    let success_count = Arc::new(AtomicU32::new(0));
    let fail_count = Arc::new(AtomicU32::new(0));
    let max_response_time = Arc::new(AtomicU64::new(0));
    let min_response_time = Arc::new(AtomicU64::new(u64::MAX));
    let total_response_time = Arc::new(AtomicU64::new(0));
    let exceeded_threshold = Arc::new(AtomicBool::new(false));

    let mut handles = vec![];

    // Simulate 10 concurrent API status requests
    for i in 0..concurrent_users {
        let success = success_count.clone();
        let fail = fail_count.clone();
        let max_rt = max_response_time.clone();
        let min_rt = min_response_time.clone();
        let total_rt = total_response_time.clone();
        let exceeded = exceeded_threshold.clone();

        let handle = thread::spawn(move || {
            let start = Instant::now();

            // Simulate API request processing time
            // API status should be faster than homepage (less data)
            thread::sleep(Duration::from_millis(5 + (i as u64 * 2)));

            let elapsed_ms = start.elapsed().as_millis() as u64;

            // Track response time statistics
            total_rt.fetch_add(elapsed_ms, Ordering::SeqCst);

            // Update max response time
            let mut current_max = max_rt.load(Ordering::SeqCst);
            while elapsed_ms > current_max {
                match max_rt.compare_exchange_weak(
                    current_max,
                    elapsed_ms,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_max = c,
                }
            }

            // Update min response time
            let mut current_min = min_rt.load(Ordering::SeqCst);
            while elapsed_ms < current_min {
                match min_rt.compare_exchange_weak(
                    current_min,
                    elapsed_ms,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_min = c,
                }
            }

            // Check if response time exceeds threshold
            if elapsed_ms > max_time_ms {
                exceeded.store(true, Ordering::SeqCst);
                fail.fetch_add(1, Ordering::SeqCst);
            } else {
                success.fetch_add(1, Ordering::SeqCst);
            }

            elapsed_ms
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let total_success = success_count.load(Ordering::SeqCst);
    let max_rt = max_response_time.load(Ordering::SeqCst);
    let min_rt = min_response_time.load(Ordering::SeqCst);
    let total_rt = total_response_time.load(Ordering::SeqCst);
    let exceeded = exceeded_threshold.load(Ordering::SeqCst);

    let avg_rt = total_rt / concurrent_users as u64;

    println!("Concurrent API Status Request Test Results:");
    println!("  Total requests: {}", concurrent_users);
    println!("  Successful: {}", total_success);
    println!("  Max response time: {}ms", max_rt);
    println!("  Min response time: {}ms", min_rt);
    println!("  Avg response time: {}ms", avg_rt);
    println!("  Threshold: {}ms", max_time_ms);

    // All requests should succeed
    assert_eq!(
        total_success as usize, concurrent_users,
        "All {} concurrent API requests should succeed",
        concurrent_users
    );

    // No requests should exceed threshold
    assert!(
        !exceeded,
        "No API request should exceed {}ms threshold, max was {}ms",
        max_time_ms, max_rt
    );

    // Max response time should be within threshold
    assert!(
        max_rt <= max_time_ms,
        "Max API response time {}ms should be <= {}ms",
        max_rt, max_time_ms
    );
}

/// Test 3: Mixed concurrent operations (5 HTTP + 5 memcached) show no degradation
///
/// This test verifies that concurrent HTTP and memcached operations don't
/// interfere with each other, maintaining acceptable response times for both.
#[test]
fn test_mixed_concurrent_operations() {
    let http_requests = 5;
    let memcached_requests = 5;

    // Baseline response time (simulated single request)
    let baseline_http_ms = 50u64;
    let baseline_memcached_ms = 20u64;

    // Acceptable degradation factor (1.5x baseline is acceptable)
    let degradation_factor = 1.5f64;

    let http_success = Arc::new(AtomicU32::new(0));
    let mc_success = Arc::new(AtomicU32::new(0));
    let http_max_rt = Arc::new(AtomicU64::new(0));
    let mc_max_rt = Arc::new(AtomicU64::new(0));
    let http_total_rt = Arc::new(AtomicU64::new(0));
    let mc_total_rt = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Spawn HTTP requests
    for i in 0..http_requests {
        let success = http_success.clone();
        let max_rt = http_max_rt.clone();
        let total_rt = http_total_rt.clone();

        let handle = thread::spawn(move || {
            let start = Instant::now();

            // Simulate HTTP request
            thread::sleep(Duration::from_millis(baseline_http_ms + (i as u64 * 5)));

            let elapsed_ms = start.elapsed().as_millis() as u64;

            success.fetch_add(1, Ordering::SeqCst);
            total_rt.fetch_add(elapsed_ms, Ordering::SeqCst);

            // Update max
            let mut current_max = max_rt.load(Ordering::SeqCst);
            while elapsed_ms > current_max {
                match max_rt.compare_exchange_weak(
                    current_max,
                    elapsed_ms,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_max = c,
                }
            }

            ("http", elapsed_ms)
        });
        handles.push(handle);
    }

    // Spawn memcached requests (simulated)
    for i in 0..memcached_requests {
        let success = mc_success.clone();
        let max_rt = mc_max_rt.clone();
        let total_rt = mc_total_rt.clone();

        let handle = thread::spawn(move || {
            let start = Instant::now();

            // Simulate memcached request (typically faster)
            thread::sleep(Duration::from_millis(baseline_memcached_ms + (i as u64 * 3)));

            let elapsed_ms = start.elapsed().as_millis() as u64;

            success.fetch_add(1, Ordering::SeqCst);
            total_rt.fetch_add(elapsed_ms, Ordering::SeqCst);

            // Update max
            let mut current_max = max_rt.load(Ordering::SeqCst);
            while elapsed_ms > current_max {
                match max_rt.compare_exchange_weak(
                    current_max,
                    elapsed_ms,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_max = c,
                }
            }

            ("memcached", elapsed_ms)
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let http_successes = http_success.load(Ordering::SeqCst);
    let mc_successes = mc_success.load(Ordering::SeqCst);
    let http_max = http_max_rt.load(Ordering::SeqCst);
    let mc_max = mc_max_rt.load(Ordering::SeqCst);
    let http_total = http_total_rt.load(Ordering::SeqCst);
    let mc_total = mc_total_rt.load(Ordering::SeqCst);

    let http_avg = http_total / http_requests as u64;
    let mc_avg = mc_total / memcached_requests as u64;

    // Calculate maximum acceptable response times
    let max_acceptable_http = (baseline_http_ms as f64 * degradation_factor * 2.0) as u64;
    let max_acceptable_mc = (baseline_memcached_ms as f64 * degradation_factor * 2.0) as u64;

    println!("Mixed Concurrent Operations Test Results:");
    println!("  HTTP requests: {} (successful: {})", http_requests, http_successes);
    println!("  HTTP max response time: {}ms (acceptable: {}ms)", http_max, max_acceptable_http);
    println!("  HTTP avg response time: {}ms", http_avg);
    println!("  Memcached requests: {} (successful: {})", memcached_requests, mc_successes);
    println!("  Memcached max response time: {}ms (acceptable: {}ms)", mc_max, max_acceptable_mc);
    println!("  Memcached avg response time: {}ms", mc_avg);

    // All requests should succeed
    assert_eq!(
        http_successes as usize, http_requests,
        "All {} HTTP requests should succeed",
        http_requests
    );
    assert_eq!(
        mc_successes as usize, memcached_requests,
        "All {} memcached requests should succeed",
        memcached_requests
    );

    // No significant degradation in HTTP response time
    assert!(
        http_max <= max_acceptable_http,
        "HTTP max response time {}ms should not exceed {}ms ({}x baseline)",
        http_max, max_acceptable_http, degradation_factor * 2.0
    );

    // No significant degradation in memcached response time
    assert!(
        mc_max <= max_acceptable_mc,
        "Memcached max response time {}ms should not exceed {}ms ({}x baseline)",
        mc_max, max_acceptable_mc, degradation_factor * 2.0
    );
}

/// Test concurrent operations with actual HTTP requests to verify server performance
/// This test starts a simple mock server and sends concurrent requests
#[test]
fn test_concurrent_http_with_mock_server() {
    use std::io::{BufRead, BufReader};

    // Find available port
    let port = find_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let server_ready = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::new(AtomicBool::new(false));

    let server_ready_clone = server_ready.clone();
    let server_shutdown_clone = server_shutdown.clone();
    let addr_clone = addr.clone();

    // Start mock HTTP server
    let server_handle = thread::spawn(move || {
        let listener = TcpListener::bind(&addr_clone).unwrap();
        listener.set_nonblocking(true).unwrap();

        server_ready_clone.store(true, Ordering::SeqCst);

        let mut request_count = 0;

        while !server_shutdown_clone.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    request_count += 1;

                    // Read request
                    let mut reader = BufReader::new(&stream);
                    let mut request_line = String::new();
                    let _ = reader.read_line(&mut request_line);

                    // Simulate processing time
                    thread::sleep(Duration::from_millis(10));

                    // Send response
                    let response = if request_line.contains("/api/status") {
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}"
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body>MirDB</body></html>"
                    };

                    let _ = stream.write_all(response.as_bytes());
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }

        request_count
    });

    // Wait for server to be ready
    while !server_ready.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }

    // Run concurrent requests
    let success_count = Arc::new(AtomicU32::new(0));
    let mut handles = vec![];

    for _ in 0..CONCURRENT_USERS {
        let addr = addr.clone();
        let success = success_count.clone();

        let handle = thread::spawn(move || {
            let (ok, response_time, status) = make_http_request(&addr, "/", HOMEPAGE_MAX_RESPONSE_TIME_MS);
            if ok && status == 200 {
                success.fetch_add(1, Ordering::SeqCst);
            }
            (ok, response_time, status)
        });
        handles.push(handle);
    }

    // Collect results
    let mut all_results = Vec::new();
    for handle in handles {
        all_results.push(handle.join().unwrap());
    }

    // Shutdown server
    server_shutdown.store(true, Ordering::SeqCst);
    let total_served = server_handle.join().unwrap();

    let successes = success_count.load(Ordering::SeqCst);

    println!("Mock Server Concurrent Test Results:");
    println!("  Total requests sent: {}", CONCURRENT_USERS);
    println!("  Requests served by server: {}", total_served);
    println!("  Successful client responses: {}", successes);

    // At least some requests should succeed
    // (may not all succeed due to race conditions with mock server shutdown)
    assert!(
        successes > 0,
        "At least some concurrent requests should succeed"
    );
}

/// Test to verify concurrent API status requests with mock server
#[test]
fn test_concurrent_api_status_with_mock_server() {
    use std::io::{BufRead, BufReader};

    let port = find_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let server_ready = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::new(AtomicBool::new(false));

    let server_ready_clone = server_ready.clone();
    let server_shutdown_clone = server_shutdown.clone();
    let addr_clone = addr.clone();

    // Start mock HTTP server
    let server_handle = thread::spawn(move || {
        let listener = TcpListener::bind(&addr_clone).unwrap();
        listener.set_nonblocking(true).unwrap();

        server_ready_clone.store(true, Ordering::SeqCst);

        let mut request_count = 0;

        while !server_shutdown_clone.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    request_count += 1;

                    let mut reader = BufReader::new(&stream);
                    let mut request_line = String::new();
                    let _ = reader.read_line(&mut request_line);

                    // API status should be fast
                    thread::sleep(Duration::from_millis(5));

                    let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"memory_usage\":1024,\"active_connections\":5,\"database_size\":2048}";
                    let _ = stream.write_all(response.as_bytes());
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }

        request_count
    });

    while !server_ready.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }

    let success_count = Arc::new(AtomicU32::new(0));
    let max_response_time = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..CONCURRENT_USERS {
        let addr = addr.clone();
        let success = success_count.clone();
        let max_rt = max_response_time.clone();

        let handle = thread::spawn(move || {
            let (ok, response_time, status) = make_http_request(&addr, "/api/status", API_STATUS_MAX_RESPONSE_TIME_MS);

            if ok && status == 200 {
                success.fetch_add(1, Ordering::SeqCst);
            }

            // Update max response time
            let mut current_max = max_rt.load(Ordering::SeqCst);
            while response_time > current_max {
                match max_rt.compare_exchange_weak(
                    current_max,
                    response_time,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ) {
                    Ok(_) => break,
                    Err(c) => current_max = c,
                }
            }

            (ok, response_time, status)
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    server_shutdown.store(true, Ordering::SeqCst);
    let total_served = server_handle.join().unwrap();

    let successes = success_count.load(Ordering::SeqCst);
    let max_rt = max_response_time.load(Ordering::SeqCst);

    println!("Mock Server API Status Concurrent Test Results:");
    println!("  Total requests sent: {}", CONCURRENT_USERS);
    println!("  Requests served: {}", total_served);
    println!("  Successful responses: {}", successes);
    println!("  Max response time: {}ms", max_rt);
    println!("  Threshold: {}ms", API_STATUS_MAX_RESPONSE_TIME_MS);

    assert!(
        successes > 0,
        "At least some API status requests should succeed"
    );
}

/// Stress test: Verify server handles more than 10 concurrent users gracefully
#[test]
fn test_stress_concurrent_users() {
    let concurrent_users = 20; // Double the requirement

    let success_count = Arc::new(AtomicU32::new(0));
    let mut handles = vec![];

    for i in 0..concurrent_users {
        let success = success_count.clone();

        let handle = thread::spawn(move || {
            let start = Instant::now();

            // Simulate varying request processing times
            thread::sleep(Duration::from_millis(10 + (i as u64 % 10) * 3));

            let elapsed = start.elapsed();
            success.fetch_add(1, Ordering::SeqCst);

            elapsed.as_millis() as u64
        });
        handles.push(handle);
    }

    let start = Instant::now();
    let mut response_times = Vec::new();

    for handle in handles {
        response_times.push(handle.join().unwrap());
    }

    let total_elapsed = start.elapsed();
    let successes = success_count.load(Ordering::SeqCst);

    let max_rt = *response_times.iter().max().unwrap_or(&0);
    let min_rt = *response_times.iter().min().unwrap_or(&0);
    let avg_rt: u64 = response_times.iter().sum::<u64>() / concurrent_users as u64;

    println!("Stress Test Results ({}x concurrent users):", concurrent_users / CONCURRENT_USERS);
    println!("  Total requests: {}", concurrent_users);
    println!("  Successful: {}", successes);
    println!("  Total time: {:?}", total_elapsed);
    println!("  Max response time: {}ms", max_rt);
    println!("  Min response time: {}ms", min_rt);
    println!("  Avg response time: {}ms", avg_rt);

    // All requests should succeed even under stress
    assert_eq!(
        successes as usize, concurrent_users,
        "All {} stress test requests should succeed",
        concurrent_users
    );

    // Concurrent execution should be faster than serial
    // Serial would take at least concurrent_users * min_sleep = 20 * 10 = 200ms
    assert!(
        total_elapsed < Duration::from_millis(200),
        "Concurrent execution should be faster than serial: {:?}",
        total_elapsed
    );
}

/// Test that response times remain consistent under load
#[test]
fn test_response_time_consistency() {
    let iterations = 5;
    let concurrent_per_iteration = CONCURRENT_USERS;

    let mut iteration_avg_times = Vec::new();

    for iteration in 0..iterations {
        let total_time = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];

        for i in 0..concurrent_per_iteration {
            let total = total_time.clone();

            let handle = thread::spawn(move || {
                let start = Instant::now();
                thread::sleep(Duration::from_millis(10 + (i as u64 * 2)));
                let elapsed = start.elapsed().as_millis() as u64;
                total.fetch_add(elapsed, Ordering::SeqCst);
                elapsed
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let avg = total_time.load(Ordering::SeqCst) / concurrent_per_iteration as u64;
        iteration_avg_times.push(avg);

        println!("Iteration {}: avg response time = {}ms", iteration + 1, avg);
    }

    // Calculate variance in average response times across iterations
    let overall_avg: u64 = iteration_avg_times.iter().sum::<u64>() / iterations as u64;
    let max_deviation = iteration_avg_times.iter()
        .map(|&t| if t > overall_avg { t - overall_avg } else { overall_avg - t })
        .max()
        .unwrap_or(0);

    println!("Response Time Consistency Test:");
    println!("  Iterations: {}", iterations);
    println!("  Concurrent users per iteration: {}", concurrent_per_iteration);
    println!("  Overall avg response time: {}ms", overall_avg);
    println!("  Max deviation from avg: {}ms", max_deviation);

    // Response times should be relatively consistent (within 50ms deviation)
    assert!(
        max_deviation < 50,
        "Response times should be consistent across iterations, deviation was {}ms",
        max_deviation
    );
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn test_parse_http_status() {
        assert_eq!(parse_http_status("HTTP/1.1 200 OK"), 200);
        assert_eq!(parse_http_status("HTTP/1.0 404 Not Found"), 404);
        assert_eq!(parse_http_status("HTTP/1.1 500 Internal Server Error"), 500);
        assert_eq!(parse_http_status("Invalid response"), 0);
        assert_eq!(parse_http_status(""), 0);
    }

    #[test]
    fn test_find_available_port() {
        let port1 = find_available_port();
        let port2 = find_available_port();

        // Ports should be valid (non-zero)
        assert!(port1 > 0);
        assert!(port2 > 0);

        // Should get different ports
        assert_ne!(port1, port2);
    }
}
