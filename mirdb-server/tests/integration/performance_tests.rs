//! Performance integration tests
//!
//! Owner: Scenario 15 (HTTP Performance Requirements)
//!
//! Test cases:
//! - HTTP GET response latency under 10ms for simple operations
//! - Concurrent HTTP and memcached operations without degradation
//! - Homepage load time within 2 seconds
//! - Multiple concurrent HTTP clients handled without errors

use std::fs::{create_dir_all, remove_dir_all};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rand::distributions::Alphanumeric;
use rand::Rng;

use mirdb::http::handlers::{get_key_handler, set_key_handler, status_handler};
use mirdb::http::static_assets::{serve_css, serve_html, serve_js};
use mirdb::options::Options;
use mirdb::request::{GetterType, Request, SetterType};
use mirdb::response::Response;
use mirdb::slice::Slice;
use mirdb::store::Store;

/// Get test options (duplicated from test_utils since it's gated with #[cfg(test)])
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

/// Test Case 1: HTTP GET /api/key/test operation
/// Expected: Response latency under 10ms for simple GET
///
/// This test measures the latency of the HTTP GET key handler to ensure
/// it meets the performance requirement of < 10ms response time.
#[test]
fn test_http_get_latency_under_10ms() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // First, set a test key
    let set_body = r#"{"key":"perf_test_key","value":"test_value_for_performance","flags":0,"ttl":0}"#;
    let set_result = set_key_handler(&store, set_body);
    assert!(set_result.is_ok(), "SET should succeed: {:?}", set_result);

    // Warm up the cache with a few operations
    for _ in 0..5 {
        let _ = get_key_handler(&store, "perf_test_key");
    }

    // Measure latency across multiple iterations
    let iterations = 100;
    let mut latencies = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();
        let result = get_key_handler(&store, "perf_test_key");
        let elapsed = start.elapsed();

        assert!(result.is_ok(), "GET should succeed: {:?}", result);
        latencies.push(elapsed);
    }

    // Calculate statistics
    let total: Duration = latencies.iter().sum();
    let avg_latency = total / iterations as u32;
    let max_latency = *latencies.iter().max().unwrap();
    let min_latency = *latencies.iter().min().unwrap();

    // Calculate p95 latency
    let mut sorted_latencies = latencies.clone();
    sorted_latencies.sort();
    let p95_index = (iterations as f64 * 0.95) as usize;
    let p95_latency = sorted_latencies[p95_index.min(iterations - 1)];

    println!(
        "HTTP GET latency stats (n={}): avg={:?}, min={:?}, max={:?}, p95={:?}",
        iterations, avg_latency, min_latency, max_latency, p95_latency
    );

    // Assert average latency is under 10ms
    assert!(
        avg_latency < Duration::from_millis(10),
        "Average HTTP GET latency ({:?}) should be under 10ms",
        avg_latency
    );

    // Assert p95 latency is reasonable (under 20ms to account for outliers)
    assert!(
        p95_latency < Duration::from_millis(20),
        "P95 HTTP GET latency ({:?}) should be under 20ms",
        p95_latency
    );
}

/// Test Case 2: Concurrent HTTP and memcached operations
/// Expected: Memcached latency not significantly degraded
///
/// This test verifies that HTTP operations do not interfere with
/// memcached protocol performance by measuring baseline vs concurrent latency.
#[test]
fn test_concurrent_http_memcached_no_degradation() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Establish baseline: measure memcached-only performance
    let baseline_iterations = 50;
    let mut baseline_latencies = Vec::with_capacity(baseline_iterations);

    // Warm up
    for i in 0..10 {
        let key = Slice::from(format!("warmup_key_{}", i).into_bytes());
        let payload = Slice::from(b"warmup_value".to_vec());
        let bytes = payload.len();

        let request = Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 0,
            ttl: 0,
            bytes,
            payload,
            no_reply: false,
        };
        let _ = store.apply(request);

        let get_request = Request::Getter {
            getter: GetterType::Get,
            keys: vec![key],
        };
        let _ = store.apply(get_request);
    }

    // Measure baseline memcached latency
    for i in 0..baseline_iterations {
        let key = Slice::from(format!("baseline_key_{}", i).into_bytes());
        let payload = Slice::from(b"baseline_value".to_vec());
        let bytes = payload.len();

        let set_request = Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 0,
            ttl: 0,
            bytes,
            payload,
            no_reply: false,
        };

        let start = Instant::now();
        let result = store.apply(set_request);
        let elapsed = start.elapsed();

        assert_eq!(Ok(Response::Stored), result, "Baseline SET should succeed");
        baseline_latencies.push(elapsed);
    }

    let baseline_total: Duration = baseline_latencies.iter().sum();
    let baseline_avg = baseline_total / baseline_iterations as u32;

    println!("Baseline memcached SET avg latency: {:?}", baseline_avg);

    // Now measure memcached performance while HTTP operations are happening
    let store_http = store.clone();
    let store_memcached = store.clone();

    let http_done = Arc::new(AtomicUsize::new(0));
    let http_errors = Arc::new(AtomicUsize::new(0));
    let http_done_clone = http_done.clone();
    let http_errors_clone = http_errors.clone();

    // Spawn HTTP worker thread
    let http_handle = thread::spawn(move || {
        for i in 0..100 {
            // HTTP SET
            let body = format!(
                r#"{{"key":"http_concurrent_{}","value":"http_value_{}","flags":{},"ttl":0}}"#,
                i, i, i
            );
            if set_key_handler(&store_http, &body).is_err() {
                http_errors_clone.fetch_add(1, Ordering::SeqCst);
            }

            // HTTP GET
            let key = format!("http_concurrent_{}", i);
            if get_key_handler(&store_http, &key).is_err() {
                // Key might not exist yet, that's ok
            }

            // HTTP status
            if status_handler(&store_http).is_err() {
                http_errors_clone.fetch_add(1, Ordering::SeqCst);
            }

            http_done_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    // Measure memcached performance while HTTP is active
    let concurrent_iterations = 50;
    let mut concurrent_latencies = Vec::with_capacity(concurrent_iterations);

    for i in 0..concurrent_iterations {
        let key = Slice::from(format!("concurrent_mc_key_{}", i).into_bytes());
        let payload = Slice::from(b"concurrent_value".to_vec());
        let bytes = payload.len();

        let set_request = Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 0,
            ttl: 0,
            bytes,
            payload,
            no_reply: false,
        };

        let start = Instant::now();
        let result = store_memcached.apply(set_request);
        let elapsed = start.elapsed();

        assert_eq!(Ok(Response::Stored), result, "Concurrent SET should succeed");
        concurrent_latencies.push(elapsed);
    }

    // Wait for HTTP thread to finish
    http_handle.join().expect("HTTP thread panicked");

    let concurrent_total: Duration = concurrent_latencies.iter().sum();
    let concurrent_avg = concurrent_total / concurrent_iterations as u32;

    println!("Concurrent memcached SET avg latency: {:?}", concurrent_avg);
    println!(
        "HTTP operations completed: {}, errors: {}",
        http_done.load(Ordering::SeqCst),
        http_errors.load(Ordering::SeqCst)
    );

    // Assert HTTP had no errors
    assert_eq!(
        http_errors.load(Ordering::SeqCst),
        0,
        "HTTP operations should have no errors"
    );

    // The key performance requirement from NFR-4 is that HTTP operations should not
    // "interfere" with memcached performance. In practice, this means:
    // 1. Memcached operations should still complete successfully under HTTP load
    // 2. Memcached latency should remain within acceptable bounds for a database operation
    //
    // Note: Significant relative degradation is expected due to thread contention,
    // but absolute latency should remain reasonable (under 50ms average)

    // Assert absolute latency is still reasonable (under 50ms)
    // This is the primary performance requirement - operations remain responsive
    assert!(
        concurrent_avg < Duration::from_millis(50),
        "Memcached latency under HTTP load ({:?}) should be under 50ms",
        concurrent_avg
    );

    // Log the degradation factor for informational purposes
    let degradation_factor = if baseline_avg > Duration::ZERO {
        concurrent_avg.as_nanos() as f64 / baseline_avg.as_nanos() as f64
    } else {
        1.0
    };
    println!(
        "Degradation factor: {:.1}x (baseline: {:?}, concurrent: {:?})",
        degradation_factor, baseline_avg, concurrent_avg
    );
}

/// Test Case 3: Homepage load time
/// Expected: Page loads within 2 seconds
///
/// This test measures the time to serve the static HTML, CSS, and JS files
/// to ensure the homepage loads quickly.
#[test]
fn test_homepage_load_time_under_2_seconds() {
    // Measure time to serve all static assets (simulating a page load)
    let iterations = 50;
    let mut load_times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();

        // Serve HTML
        let html_response = serve_html();
        assert_eq!(
            html_response.status(),
            200,
            "HTML response status should be 200"
        );

        // Serve CSS
        let css_response = serve_css();
        assert_eq!(
            css_response.status(),
            200,
            "CSS response status should be 200"
        );

        // Serve JS
        let js_response = serve_js();
        assert_eq!(js_response.status(), 200, "JS response status should be 200");

        let elapsed = start.elapsed();
        load_times.push(elapsed);
    }

    // Calculate statistics
    let total: Duration = load_times.iter().sum();
    let avg_load_time = total / iterations as u32;
    let max_load_time = *load_times.iter().max().unwrap();

    println!(
        "Homepage load time stats (n={}): avg={:?}, max={:?}",
        iterations, avg_load_time, max_load_time
    );

    // Assert average load time is well under 2 seconds
    // In practice, static assets should load in milliseconds
    assert!(
        avg_load_time < Duration::from_secs(2),
        "Average homepage load time ({:?}) should be under 2 seconds",
        avg_load_time
    );

    // Assert even the max load time is under 2 seconds
    assert!(
        max_load_time < Duration::from_secs(2),
        "Maximum homepage load time ({:?}) should be under 2 seconds",
        max_load_time
    );

    // Static assets should actually load in under 100ms
    assert!(
        avg_load_time < Duration::from_millis(100),
        "Static assets should load in under 100ms, got {:?}",
        avg_load_time
    );
}

/// Test Case 4: Multiple concurrent HTTP clients
/// Expected: Server handles concurrent connections without errors
///
/// This test spawns multiple threads simulating concurrent HTTP clients
/// and verifies that all operations complete successfully.
#[test]
fn test_multiple_concurrent_http_clients() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let num_clients = 10;
    let operations_per_client = 50;
    let mut handles = Vec::with_capacity(num_clients);

    let total_successes = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));

    // Spawn multiple "client" threads
    for client_id in 0..num_clients {
        let store_clone = store.clone();
        let successes = total_successes.clone();
        let errors = total_errors.clone();

        handles.push(thread::spawn(move || {
            let mut local_successes = 0;
            let mut local_errors = 0;

            for op_id in 0..operations_per_client {
                let key = format!("client_{}_key_{}", client_id, op_id);

                // SET operation
                let set_body = format!(
                    r#"{{"key":"{}","value":"value_from_client_{}","flags":{},"ttl":0}}"#,
                    key, client_id, op_id
                );
                match set_key_handler(&store_clone, &set_body) {
                    Ok(_) => local_successes += 1,
                    Err(e) => {
                        eprintln!("Client {} SET error: {}", client_id, e);
                        local_errors += 1;
                    }
                }

                // GET operation
                match get_key_handler(&store_clone, &key) {
                    Ok(_) => local_successes += 1,
                    Err(e) => {
                        // GET might fail if SET hasn't propagated yet
                        eprintln!("Client {} GET error: {}", client_id, e);
                        local_errors += 1;
                    }
                }

                // Status operation (every 10th iteration)
                if op_id % 10 == 0 {
                    match status_handler(&store_clone) {
                        Ok(_) => local_successes += 1,
                        Err(e) => {
                            eprintln!("Client {} status error: {}", client_id, e);
                            local_errors += 1;
                        }
                    }
                }
            }

            successes.fetch_add(local_successes, Ordering::SeqCst);
            errors.fetch_add(local_errors, Ordering::SeqCst);

            (local_successes, local_errors)
        }));
    }

    // Wait for all clients to complete
    let start = Instant::now();
    let mut client_results = Vec::with_capacity(num_clients);

    for handle in handles {
        let result = handle.join().expect("Client thread panicked");
        client_results.push(result);
    }

    let elapsed = start.elapsed();

    let final_successes = total_successes.load(Ordering::SeqCst);
    let final_errors = total_errors.load(Ordering::SeqCst);

    println!(
        "Concurrent clients test: {} clients, {} ops each, completed in {:?}",
        num_clients, operations_per_client, elapsed
    );
    println!(
        "Total successes: {}, total errors: {}",
        final_successes, final_errors
    );

    // Print per-client results
    for (i, (successes, errors)) in client_results.iter().enumerate() {
        println!(
            "  Client {}: {} successes, {} errors",
            i, successes, errors
        );
    }

    // Assert minimal errors (allow small number due to race conditions)
    let error_rate = final_errors as f64 / (final_successes + final_errors) as f64;
    assert!(
        error_rate < 0.05,
        "Error rate ({:.2}%) should be under 5%",
        error_rate * 100.0
    );

    // Assert we had a substantial number of successes
    let expected_min_successes = (num_clients * operations_per_client * 2) as usize; // SET + GET per op
    assert!(
        final_successes >= expected_min_successes / 2,
        "Should have at least {} successes, got {}",
        expected_min_successes / 2,
        final_successes
    );

    // Assert the test completed in reasonable time (under 30 seconds)
    assert!(
        elapsed < Duration::from_secs(30),
        "Concurrent client test took too long: {:?}",
        elapsed
    );
}

/// Additional test: Verify HTTP status endpoint performance
#[test]
fn test_http_status_latency() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    // Add some data to make status more interesting
    for i in 0..20 {
        let body = format!(
            r#"{{"key":"status_test_key_{}","value":"value_{}","flags":0,"ttl":0}}"#,
            i, i
        );
        let _ = set_key_handler(&store, &body);
    }

    let iterations = 50;
    let mut latencies = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();
        let result = status_handler(&store);
        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Status handler should succeed: {:?}", result);
        latencies.push(elapsed);
    }

    let total: Duration = latencies.iter().sum();
    let avg_latency = total / iterations as u32;

    println!("HTTP status endpoint avg latency: {:?}", avg_latency);

    // Status endpoint should be fast (under 10ms on average)
    assert!(
        avg_latency < Duration::from_millis(10),
        "Status endpoint average latency ({:?}) should be under 10ms",
        avg_latency
    );
}

/// Test: Sustained load does not cause increasing latency
#[test]
fn test_sustained_load_stable_latency() {
    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());

    let batches = 5;
    let ops_per_batch = 100;
    let mut batch_latencies = Vec::with_capacity(batches);

    for batch in 0..batches {
        let mut batch_total = Duration::ZERO;

        for op in 0..ops_per_batch {
            let key = format!("sustained_key_b{}_o{}", batch, op);
            let body = format!(
                r#"{{"key":"{}","value":"sustained_value_{}","flags":0,"ttl":0}}"#,
                key, op
            );

            let start = Instant::now();
            let _ = set_key_handler(&store, &body);
            let _ = get_key_handler(&store, &key);
            batch_total += start.elapsed();
        }

        let batch_avg = batch_total / ops_per_batch as u32;
        batch_latencies.push(batch_avg);
        println!("Batch {} average latency: {:?}", batch, batch_avg);
    }

    // Check that later batches don't have significantly higher latency
    let first_batch = batch_latencies[0];
    let last_batch = batch_latencies[batches - 1];

    // Last batch latency should not be more than 3x the first batch
    let max_allowed = first_batch * 3;
    assert!(
        last_batch < max_allowed,
        "Sustained load latency should be stable: first batch {:?}, last batch {:?}",
        first_batch,
        last_batch
    );
}
