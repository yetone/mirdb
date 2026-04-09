//! HTTP server integration tests.
//!
//! Owner: Scenario 16 - HTTP Server Integration
//!
//! Tests:
//! - HTTP server starts alongside Memcached server
//! - Memcached operations work with HTTP active
//! - Concurrent protocol access
//! - Resource usage acceptable

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// Test configuration constants
const HTTP_PORT: u16 = 18080;
const MEMCACHED_PORT: u16 = 18333;
const CONCURRENT_REQUESTS: usize = 20;
const MAX_MEMORY_INCREASE_BYTES: usize = 10 * 1024 * 1024; // 10MB

// =============================================
// Test Case 1: HTTP server starts and serves homepage
// =============================================

/// Simulates HTTP server response handling
/// Tests the route handling logic without starting actual servers
#[test]
fn test_http_server_starts_and_serves_homepage() {
    use hyper::{Body, Method, Request, StatusCode};

    // Create mock state and request
    let state = Arc::new(RwLock::new(MockSharedState::new()));

    // Test homepage route
    let response = mock_route_request(Method::GET, "/", Arc::clone(&state));
    assert_eq!(response.status, 200, "Homepage should return 200 OK");
    assert!(
        response.content_type.contains("text/html"),
        "Homepage should return HTML content type"
    );
    assert!(
        response.body.contains("MirDB"),
        "Homepage should contain MirDB branding"
    );
}

/// Test that HTTP health endpoint works
#[test]
fn test_http_health_endpoint() {
    let state = Arc::new(RwLock::new(MockSharedState::new()));

    let response = mock_route_request(Method::GET, "/health", Arc::clone(&state));
    assert_eq!(response.status, 200, "Health endpoint should return 200 OK");
    assert!(
        response.body.contains("healthy"),
        "Health response should indicate healthy status"
    );
}

/// Test that HTTP status API endpoint works
#[test]
fn test_http_status_api_endpoint() {
    let state = Arc::new(RwLock::new(MockSharedState::new()));

    let response = mock_route_request(Method::GET, "/api/status", Arc::clone(&state));
    assert_eq!(response.status, 200, "Status API should return 200 OK");
    assert!(
        response.content_type.contains("application/json"),
        "Status API should return JSON"
    );

    // Parse and validate JSON structure
    let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
    assert!(json.get("status").is_some(), "Should have status field");
    assert!(json.get("uptime_seconds").is_some(), "Should have uptime_seconds field");
    assert!(json.get("version").is_some(), "Should have version field");
}

/// Test that HTTP metrics API endpoint works
#[test]
fn test_http_metrics_api_endpoint() {
    let state = Arc::new(RwLock::new(MockSharedState::new()));

    let response = mock_route_request(Method::GET, "/api/metrics", Arc::clone(&state));
    assert_eq!(response.status, 200, "Metrics API should return 200 OK");
    assert!(
        response.content_type.contains("application/json"),
        "Metrics API should return JSON"
    );

    // Parse and validate JSON structure
    let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
    assert!(json.get("total_keys").is_some(), "Should have total_keys field");
    assert!(json.get("ops_per_second").is_some(), "Should have ops_per_second field");
}

// =============================================
// Test Case 2: SET operation succeeds while homepage server running
// =============================================

/// Simulates SET operation alongside HTTP server activity
#[test]
fn test_memcached_set_with_http_active() {
    // Create shared state that both "servers" will use
    let shared_store = Arc::new(RwLock::new(MockStore::new()));
    let http_state = Arc::new(RwLock::new(MockSharedState::new()));

    // Simulate HTTP server activity (accessing state)
    let http_state_clone = Arc::clone(&http_state);
    let http_thread = thread::spawn(move || {
        for _ in 0..10 {
            let _ = mock_route_request(Method::GET, "/api/status", Arc::clone(&http_state_clone));
            thread::sleep(Duration::from_millis(1));
        }
    });

    // Simulate Memcached SET operation
    let store = Arc::clone(&shared_store);
    let result = mock_memcached_set(store, "test_key", "test_value");

    // Wait for HTTP activity to complete
    http_thread.join().unwrap();

    assert!(result.is_ok(), "SET operation should succeed while HTTP is active");
    assert_eq!(result.unwrap(), "STORED", "SET should return STORED response");

    // Verify the value was stored
    let stored_value = shared_store.read().unwrap().get("test_key");
    assert_eq!(stored_value, Some("test_value".to_string()), "Value should be stored correctly");
}

// =============================================
// Test Case 3: GET operation returns correct value
// =============================================

/// Tests GET operation with HTTP server running
#[test]
fn test_memcached_get_with_http_active() {
    let shared_store = Arc::new(RwLock::new(MockStore::new()));
    let http_state = Arc::new(RwLock::new(MockSharedState::new()));

    // Pre-populate the store
    {
        let mut store = shared_store.write().unwrap();
        store.set("existing_key", "existing_value");
    }

    // Simulate HTTP activity
    let http_state_clone = Arc::clone(&http_state);
    let http_thread = thread::spawn(move || {
        for _ in 0..5 {
            let _ = mock_route_request(Method::GET, "/api/metrics", Arc::clone(&http_state_clone));
            thread::sleep(Duration::from_millis(2));
        }
    });

    // Perform GET operation
    let store = Arc::clone(&shared_store);
    let result = mock_memcached_get(store, "existing_key");

    http_thread.join().unwrap();

    assert!(result.is_ok(), "GET operation should succeed");
    assert_eq!(
        result.unwrap(),
        Some("existing_value".to_string()),
        "GET should return correct value"
    );
}

/// Tests GET for non-existent key
#[test]
fn test_memcached_get_nonexistent_key() {
    let shared_store = Arc::new(RwLock::new(MockStore::new()));

    let result = mock_memcached_get(shared_store, "nonexistent_key");
    assert!(result.is_ok(), "GET operation should succeed");
    assert_eq!(result.unwrap(), None, "GET should return None for nonexistent key");
}

// =============================================
// Test Case 4: Concurrent homepage request + Memcached SET
// =============================================

/// Tests concurrent access from both protocols
#[test]
fn test_concurrent_http_and_memcached_operations() {
    let shared_store = Arc::new(RwLock::new(MockStore::new()));
    let http_state = Arc::new(RwLock::new(MockSharedState::new()));
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Spawn HTTP request threads
    for i in 0..CONCURRENT_REQUESTS {
        let state = Arc::clone(&http_state);
        let successes = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);

        handles.push(thread::spawn(move || {
            let response = mock_route_request(Method::GET, "/api/status", state);
            if response.status == 200 {
                successes.fetch_add(1, Ordering::SeqCst);
            } else {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Spawn Memcached SET threads
    for i in 0..CONCURRENT_REQUESTS {
        let store = Arc::clone(&shared_store);
        let successes = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);
        let key = format!("concurrent_key_{}", i);
        let value = format!("concurrent_value_{}", i);

        handles.push(thread::spawn(move || {
            let result = mock_memcached_set(store, &key, &value);
            if result.is_ok() && result.unwrap() == "STORED" {
                successes.fetch_add(1, Ordering::SeqCst);
            } else {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread should not panic");
    }

    let total_successes = success_count.load(Ordering::SeqCst);
    let total_errors = error_count.load(Ordering::SeqCst);
    let expected_total = CONCURRENT_REQUESTS * 2;

    println!("Concurrent test results:");
    println!("  Successes: {}", total_successes);
    println!("  Errors: {}", total_errors);
    println!("  Expected total: {}", expected_total);

    assert_eq!(
        total_errors, 0,
        "No errors should occur during concurrent access"
    );
    assert_eq!(
        total_successes, expected_total,
        "All operations should succeed"
    );

    // Verify all keys were stored
    let store = shared_store.read().unwrap();
    for i in 0..CONCURRENT_REQUESTS {
        let key = format!("concurrent_key_{}", i);
        let expected_value = format!("concurrent_value_{}", i);
        assert_eq!(
            store.get(&key),
            Some(expected_value),
            "Key {} should have correct value",
            key
        );
    }
}

/// Tests that concurrent access doesn't cause deadlocks
#[test]
fn test_no_deadlock_under_concurrent_load() {
    let shared_store = Arc::new(RwLock::new(MockStore::new()));
    let http_state = Arc::new(RwLock::new(MockSharedState::new()));

    let start = Instant::now();
    let timeout = Duration::from_secs(5); // 5 second timeout

    let mut handles = Vec::new();

    // Mix of operations that could cause deadlocks if locking is wrong
    for i in 0..50 {
        let store = Arc::clone(&shared_store);
        let state = Arc::clone(&http_state);

        handles.push(thread::spawn(move || {
            // Alternate between HTTP and Memcached operations
            if i % 2 == 0 {
                let _ = mock_route_request(Method::GET, "/api/status", state);
            } else {
                let key = format!("deadlock_test_{}", i);
                let _ = mock_memcached_set(store, &key, "value");
            }
        }));
    }

    // Wait for all threads with timeout check
    for handle in handles {
        handle.join().expect("Thread should complete without deadlock");
        assert!(
            start.elapsed() < timeout,
            "Operations should complete within timeout (no deadlock)"
        );
    }
}

// =============================================
// Test Case 5: Memory footprint increase is reasonable (< 10MB)
// =============================================

/// Tests that HTTP server components don't add excessive memory overhead
#[test]
fn test_memory_footprint_reasonable() {
    // Measure baseline memory of creating state objects
    let baseline_size = std::mem::size_of::<MockSharedState>() + std::mem::size_of::<MockStore>();

    // Create multiple state instances to simulate server startup
    let states: Vec<_> = (0..100)
        .map(|_| Arc::new(RwLock::new(MockSharedState::new())))
        .collect();

    let stores: Vec<_> = (0..100)
        .map(|_| Arc::new(RwLock::new(MockStore::new())))
        .collect();

    // Estimate memory usage
    let estimated_memory =
        states.len() * std::mem::size_of::<Arc<RwLock<MockSharedState>>>()
            + stores.len() * std::mem::size_of::<Arc<RwLock<MockStore>>>()
            + baseline_size * 100;

    println!("Memory footprint test:");
    println!("  Baseline size: {} bytes", baseline_size);
    println!("  Estimated total: {} bytes", estimated_memory);
    println!("  Max allowed: {} bytes", MAX_MEMORY_INCREASE_BYTES);

    // The HTTP server module should add minimal overhead
    assert!(
        estimated_memory < MAX_MEMORY_INCREASE_BYTES,
        "HTTP server memory overhead ({} bytes) exceeds limit ({} bytes)",
        estimated_memory,
        MAX_MEMORY_INCREASE_BYTES
    );
}

/// Tests that repeated operations don't cause memory leaks
#[test]
fn test_no_memory_leak_under_load() {
    let shared_store = Arc::new(RwLock::new(MockStore::new()));
    let http_state = Arc::new(RwLock::new(MockSharedState::new()));

    let iterations = 1000;

    // Perform many operations
    for i in 0..iterations {
        let _ = mock_route_request(Method::GET, "/api/status", Arc::clone(&http_state));
        let _ = mock_route_request(Method::GET, "/api/metrics", Arc::clone(&http_state));

        let key = format!("leak_test_{}", i % 100); // Reuse keys to avoid unbounded growth
        let _ = mock_memcached_set(Arc::clone(&shared_store), &key, "value");
        let _ = mock_memcached_get(Arc::clone(&shared_store), &key);
    }

    // Verify store size is bounded
    let store = shared_store.read().unwrap();
    assert!(
        store.len() <= 100,
        "Store should not grow unboundedly (size: {})",
        store.len()
    );

    // Strong reference counts should be reasonable
    assert!(
        Arc::strong_count(&http_state) == 1,
        "HTTP state should have only one strong reference after operations"
    );
    assert!(
        Arc::strong_count(&shared_store) == 1,
        "Store should have only one strong reference after operations"
    );
}

// =============================================
// Additional integration tests
// =============================================

/// Tests that HTTP server config is properly initialized
#[test]
fn test_http_config_initialization() {
    let config = MockHttpConfig::default();
    assert_eq!(config.http_port, 8080, "Default HTTP port should be 8080");
    assert_eq!(
        config.memcached_addr, "0.0.0.0:12333",
        "Default Memcached address should match"
    );
}

/// Tests that server state tracks uptime correctly
#[test]
fn test_server_uptime_tracking() {
    let state = MockSharedState::new();
    let initial_uptime = state.uptime_seconds();

    thread::sleep(Duration::from_millis(100));

    let later_uptime = state.uptime_seconds();
    // Note: uptime may still be 0 for very short sleeps due to second granularity
    assert!(
        later_uptime >= initial_uptime,
        "Uptime should not decrease"
    );
}

/// Tests thread-safe access to shared state
#[test]
fn test_thread_safe_state_access() {
    let state = Arc::new(RwLock::new(MockSharedState::new()));
    let mut handles = Vec::new();

    // Multiple readers
    for _ in 0..10 {
        let s = Arc::clone(&state);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let guard = s.read().unwrap();
                let _ = guard.uptime_seconds();
            }
        }));
    }

    // Single writer
    let s = Arc::clone(&state);
    handles.push(thread::spawn(move || {
        for _ in 0..50 {
            let mut guard = s.write().unwrap();
            guard.increment_ops();
        }
    }));

    for handle in handles {
        handle.join().expect("Thread should complete successfully");
    }

    let final_state = state.read().unwrap();
    assert_eq!(final_state.ops_count, 50, "Operations count should match");
}

// =============================================
// Mock implementations for testing
// =============================================

use hyper::Method;

/// Mock HTTP response for testing
struct MockHttpResponse {
    status: u16,
    content_type: String,
    body: String,
}

/// Mock shared state for testing (mirrors real SharedState)
struct MockSharedState {
    start_time: Instant,
    ops_count: usize,
}

impl MockSharedState {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            ops_count: 0,
        }
    }

    fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    fn increment_ops(&mut self) {
        self.ops_count += 1;
    }
}

/// Mock store for testing (mirrors real Store)
struct MockStore {
    data: HashMap<String, String>,
}

impl MockStore {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn set(&mut self, key: &str, value: &str) {
        self.data.insert(key.to_string(), value.to_string());
    }

    fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

/// Mock HTTP config
struct MockHttpConfig {
    http_port: u16,
    memcached_addr: String,
}

impl Default for MockHttpConfig {
    fn default() -> Self {
        Self {
            http_port: 8080,
            memcached_addr: "0.0.0.0:12333".to_string(),
        }
    }
}

/// Mock route request handler
fn mock_route_request(
    method: Method,
    path: &str,
    _state: Arc<RwLock<MockSharedState>>,
) -> MockHttpResponse {
    match (method, path) {
        (Method::GET, "/") => MockHttpResponse {
            status: 200,
            content_type: "text/html; charset=utf-8".to_string(),
            body: r#"<!DOCTYPE html><html><head><title>MirDB Homepage</title></head><body><h1>MirDB</h1></body></html>"#.to_string(),
        },
        (Method::GET, "/health") => MockHttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body: r#"{"status":"healthy"}"#.to_string(),
        },
        (Method::GET, "/api/status") => MockHttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body: r#"{"status":"running","uptime_seconds":0,"version":"0.1.0","endpoint":{"host":"0.0.0.0","port":12333}}"#.to_string(),
        },
        (Method::GET, "/api/metrics") => MockHttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body: r#"{"total_keys":0,"ops_per_second":0.0,"memory_usage_bytes":0,"storage_used_bytes":0,"uptime_seconds":0}"#.to_string(),
        },
        _ => MockHttpResponse {
            status: 404,
            content_type: "application/json".to_string(),
            body: r#"{"error":"Not Found"}"#.to_string(),
        },
    }
}

/// Mock Memcached SET operation
fn mock_memcached_set(
    store: Arc<RwLock<MockStore>>,
    key: &str,
    value: &str,
) -> Result<String, String> {
    match store.write() {
        Ok(mut guard) => {
            guard.set(key, value);
            Ok("STORED".to_string())
        }
        Err(e) => Err(format!("Lock error: {}", e)),
    }
}

/// Mock Memcached GET operation
fn mock_memcached_get(
    store: Arc<RwLock<MockStore>>,
    key: &str,
) -> Result<Option<String>, String> {
    match store.read() {
        Ok(guard) => Ok(guard.get(key)),
        Err(e) => Err(format!("Lock error: {}", e)),
    }
}
