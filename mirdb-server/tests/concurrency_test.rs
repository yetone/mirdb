//! Concurrent user handling tests.
//! Owner: Scenario 16 - Concurrent User Handling
//!
//! Tests for verifying the homepage handles multiple concurrent visitors without degradation.
//! NFR-3: Handle at least 50 concurrent visitors
//!
//! Test cases:
//! 1. 50 concurrent GET / requests - all should complete successfully
//! 2. 50 concurrent GET /api/metrics requests - all should return valid JSON within 500ms
//! 3. Mixed load: 25 homepage + 25 API requests - all should succeed

use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    routing::get,
    Json, Router,
};
use tower::ServiceExt;

use mirdb::web::handlers::metrics::{create_metrics_state, SharedMetricsState};
use mirdb::web::handlers::static_files::serve_homepage;
use mirdb::web::routes::create_router;
use mirdb::web::types::MetricsResponse;

/// Number of concurrent requests for testing (NFR-3 requirement)
const CONCURRENT_USERS: usize = 50;

/// Maximum acceptable response time for metrics endpoint (NFR-7)
const MAX_METRICS_RESPONSE_TIME_MS: u64 = 500;

/// Create a test router with metrics endpoint for concurrency testing
fn create_test_router_with_metrics() -> Router {
    let metrics_state = create_metrics_state("/tmp/concurrency_test".to_string(), 7, 16 * 1024 * 1024);

    // Set some initial metrics for testing
    metrics_state.set_key_count(100);
    metrics_state.set_sstable_counts(vec![2, 4, 8, 0, 0, 0, 0]);
    metrics_state.set_memory_used(4 * 1024 * 1024);

    // Create a new router with state from scratch
    Router::new()
        .route("/", get(serve_homepage))
        .route("/api/metrics", get(get_metrics_handler))
        .with_state(metrics_state)
}

/// Handler for GET /api/metrics used in concurrency tests
async fn get_metrics_handler(
    State(state): State<SharedMetricsState>,
) -> Json<MetricsResponse> {
    Json(state.get_metrics())
}

/// Test Case 1: 50 concurrent GET / requests
/// Verifies that all requests complete successfully with no timeouts
#[tokio::test]
async fn test_50_concurrent_homepage_requests() {
    let app = create_router();

    // Clone the app for each concurrent request
    let mut handles = Vec::with_capacity(CONCURRENT_USERS);
    let start = Instant::now();

    for i in 0..CONCURRENT_USERS {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let request_start = Instant::now();
            let response = app_clone
                .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                .await;
            let request_duration = request_start.elapsed();
            (i, response, request_duration)
        });
        handles.push(handle);
    }

    // Collect results
    let mut success_count = 0;
    let mut max_response_time = Duration::ZERO;
    let mut total_response_time = Duration::ZERO;
    let mut errors = Vec::new();

    for handle in handles {
        match handle.await {
            Ok((idx, response_result, duration)) => {
                total_response_time += duration;
                if duration > max_response_time {
                    max_response_time = duration;
                }

                match response_result {
                    Ok(response) => {
                        if response.status() == StatusCode::OK {
                            success_count += 1;
                        } else {
                            errors.push(format!(
                                "Request {} returned status: {}",
                                idx,
                                response.status()
                            ));
                        }
                    }
                    Err(e) => {
                        errors.push(format!("Request {} failed: {}", idx, e));
                    }
                }
            }
            Err(e) => {
                errors.push(format!("Task join error: {}", e));
            }
        }
    }

    let total_duration = start.elapsed();

    // Log performance metrics
    println!("=== Test Case 1: 50 Concurrent Homepage Requests ===");
    println!("Total requests: {}", CONCURRENT_USERS);
    println!("Successful requests: {}", success_count);
    println!("Total test duration: {:?}", total_duration);
    println!("Max single response time: {:?}", max_response_time);
    println!(
        "Avg response time: {:?}",
        total_response_time / CONCURRENT_USERS as u32
    );

    // Assertions
    assert!(
        errors.is_empty(),
        "Some requests failed: {:?}",
        errors
    );
    assert_eq!(
        success_count, CONCURRENT_USERS,
        "All {} requests should succeed, but only {} did",
        CONCURRENT_USERS, success_count
    );

    // Verify no timeouts (all should complete within a reasonable time)
    assert!(
        total_duration < Duration::from_secs(10),
        "All concurrent requests should complete within 10 seconds, took {:?}",
        total_duration
    );
}

/// Test Case 2: 50 concurrent GET /api/metrics requests
/// Verifies all requests return valid JSON within 500ms (NFR-7)
#[tokio::test]
async fn test_50_concurrent_metrics_requests() {
    let app = create_test_router_with_metrics();

    let mut handles = Vec::with_capacity(CONCURRENT_USERS);
    let start = Instant::now();

    for i in 0..CONCURRENT_USERS {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let request_start = Instant::now();
            let response = app_clone
                .oneshot(
                    Request::builder()
                        .uri("/api/metrics")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
            let request_duration = request_start.elapsed();
            (i, response, request_duration)
        });
        handles.push(handle);
    }

    // Collect results
    let mut success_count = 0;
    let mut max_response_time = Duration::ZERO;
    let mut total_response_time = Duration::ZERO;
    let mut slow_requests = Vec::new();
    let mut errors = Vec::new();

    for handle in handles {
        match handle.await {
            Ok((idx, response_result, duration)) => {
                total_response_time += duration;
                if duration > max_response_time {
                    max_response_time = duration;
                }

                // Check if response exceeds 500ms threshold
                if duration > Duration::from_millis(MAX_METRICS_RESPONSE_TIME_MS) {
                    slow_requests.push((idx, duration));
                }

                match response_result {
                    Ok(response) => {
                        if response.status() == StatusCode::OK {
                            // Verify the response body is valid JSON
                            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                                .await
                                .unwrap();
                            let body_str = String::from_utf8_lossy(&body);

                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body_str)
                            {
                                // Verify required fields exist
                                if parsed.get("key_count").is_some()
                                    && parsed.get("compaction_running").is_some()
                                    && parsed.get("memory_used_bytes").is_some()
                                {
                                    success_count += 1;
                                } else {
                                    errors.push(format!(
                                        "Request {} missing required JSON fields",
                                        idx
                                    ));
                                }
                            } else {
                                errors.push(format!(
                                    "Request {} returned invalid JSON: {}",
                                    idx, body_str
                                ));
                            }
                        } else {
                            errors.push(format!(
                                "Request {} returned status: {}",
                                idx,
                                response.status()
                            ));
                        }
                    }
                    Err(e) => {
                        errors.push(format!("Request {} failed: {}", idx, e));
                    }
                }
            }
            Err(e) => {
                errors.push(format!("Task join error: {}", e));
            }
        }
    }

    let total_duration = start.elapsed();

    // Log performance metrics
    println!("=== Test Case 2: 50 Concurrent Metrics Requests ===");
    println!("Total requests: {}", CONCURRENT_USERS);
    println!("Successful requests: {}", success_count);
    println!("Total test duration: {:?}", total_duration);
    println!("Max single response time: {:?}", max_response_time);
    println!(
        "Avg response time: {:?}",
        total_response_time / CONCURRENT_USERS as u32
    );
    println!("Requests exceeding 500ms: {}", slow_requests.len());

    // Assertions
    assert!(
        errors.is_empty(),
        "Some requests failed: {:?}",
        errors
    );
    assert_eq!(
        success_count, CONCURRENT_USERS,
        "All {} requests should succeed with valid JSON, but only {} did",
        CONCURRENT_USERS, success_count
    );

    // NFR-7: All metric endpoints must respond within 500ms
    assert!(
        slow_requests.is_empty(),
        "All requests should complete within 500ms (NFR-7), but {} exceeded: {:?}",
        slow_requests.len(),
        slow_requests
    );
}

/// Test Case 3: Mixed load - 25 homepage + 25 API requests
/// Verifies all requests succeed with no resource exhaustion
#[tokio::test]
async fn test_mixed_load_homepage_and_api_requests() {
    let app = create_test_router_with_metrics();

    let homepage_count = CONCURRENT_USERS / 2; // 25 homepage requests
    let api_count = CONCURRENT_USERS / 2; // 25 API requests

    let mut handles = Vec::with_capacity(CONCURRENT_USERS);
    let start = Instant::now();

    // Spawn homepage requests
    for i in 0..homepage_count {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let request_start = Instant::now();
            let response = app_clone
                .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                .await;
            let request_duration = request_start.elapsed();
            ("homepage", i, response, request_duration)
        });
        handles.push(handle);
    }

    // Spawn API requests
    for i in 0..api_count {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let request_start = Instant::now();
            let response = app_clone
                .oneshot(
                    Request::builder()
                        .uri("/api/metrics")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
            let request_duration = request_start.elapsed();
            ("api", i, response, request_duration)
        });
        handles.push(handle);
    }

    // Collect results
    let mut homepage_success_count = 0;
    let mut api_success_count = 0;
    let mut max_response_time = Duration::ZERO;
    let mut total_response_time = Duration::ZERO;
    let mut errors = Vec::new();

    for handle in handles {
        match handle.await {
            Ok((request_type, idx, response_result, duration)) => {
                total_response_time += duration;
                if duration > max_response_time {
                    max_response_time = duration;
                }

                match response_result {
                    Ok(response) => {
                        if response.status() == StatusCode::OK {
                            match request_type {
                                "homepage" => homepage_success_count += 1,
                                "api" => {
                                    // Verify API response is valid JSON
                                    let body =
                                        axum::body::to_bytes(response.into_body(), usize::MAX)
                                            .await
                                            .unwrap();
                                    let body_str = String::from_utf8_lossy(&body);
                                    if serde_json::from_str::<serde_json::Value>(&body_str).is_ok()
                                    {
                                        api_success_count += 1;
                                    } else {
                                        errors.push(format!(
                                            "API request {} returned invalid JSON",
                                            idx
                                        ));
                                    }
                                }
                                _ => {}
                            }
                        } else {
                            errors.push(format!(
                                "{} request {} returned status: {}",
                                request_type,
                                idx,
                                response.status()
                            ));
                        }
                    }
                    Err(e) => {
                        errors.push(format!("{} request {} failed: {}", request_type, idx, e));
                    }
                }
            }
            Err(e) => {
                errors.push(format!("Task join error: {}", e));
            }
        }
    }

    let total_duration = start.elapsed();

    // Log performance metrics
    println!("=== Test Case 3: Mixed Load (25 Homepage + 25 API) ===");
    println!("Total requests: {}", CONCURRENT_USERS);
    println!("Homepage successful: {} / {}", homepage_success_count, homepage_count);
    println!("API successful: {} / {}", api_success_count, api_count);
    println!("Total test duration: {:?}", total_duration);
    println!("Max single response time: {:?}", max_response_time);
    println!(
        "Avg response time: {:?}",
        total_response_time / CONCURRENT_USERS as u32
    );

    // Assertions
    assert!(
        errors.is_empty(),
        "Some requests failed: {:?}",
        errors
    );
    assert_eq!(
        homepage_success_count, homepage_count,
        "All {} homepage requests should succeed, but only {} did",
        homepage_count, homepage_success_count
    );
    assert_eq!(
        api_success_count, api_count,
        "All {} API requests should succeed, but only {} did",
        api_count, api_success_count
    );

    // Verify no resource exhaustion (reasonable total time)
    assert!(
        total_duration < Duration::from_secs(10),
        "Mixed load should complete within 10 seconds, took {:?}",
        total_duration
    );
}

/// Additional test: Stress test with higher concurrency
#[tokio::test]
async fn test_sustained_concurrent_load() {
    let app = create_router();
    let iterations = 3;
    let requests_per_iteration = CONCURRENT_USERS;

    let mut all_success = true;
    let mut total_requests = 0;
    let mut total_successful = 0;

    let overall_start = Instant::now();

    for iteration in 0..iterations {
        let mut handles = Vec::with_capacity(requests_per_iteration);

        for _ in 0..requests_per_iteration {
            let app_clone = app.clone();
            let handle = tokio::spawn(async move {
                app_clone
                    .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                    .await
            });
            handles.push(handle);
        }

        for handle in handles {
            total_requests += 1;
            if let Ok(Ok(response)) = handle.await {
                if response.status() == StatusCode::OK {
                    total_successful += 1;
                } else {
                    all_success = false;
                }
            } else {
                all_success = false;
            }
        }

        println!(
            "Iteration {} complete: {}/{} successful",
            iteration + 1,
            total_successful,
            total_requests
        );
    }

    let overall_duration = overall_start.elapsed();

    println!("=== Sustained Load Test ===");
    println!("Total requests: {}", total_requests);
    println!("Successful requests: {}", total_successful);
    println!("Total duration: {:?}", overall_duration);
    println!(
        "Requests per second: {:.2}",
        total_requests as f64 / overall_duration.as_secs_f64()
    );

    assert!(
        all_success,
        "All {} requests should succeed across {} iterations",
        total_requests, iterations
    );
    assert_eq!(total_successful, total_requests);
}

/// Test: Verify no memory leaks under concurrent load
/// This test ensures that repeated concurrent requests don't cause memory issues
#[tokio::test]
async fn test_no_resource_exhaustion_under_load() {
    let app = create_test_router_with_metrics();

    // Run multiple batches to check for resource exhaustion
    for batch in 0..5 {
        let mut handles = Vec::with_capacity(20);

        for _ in 0..20 {
            let app_clone = app.clone();
            let handle = tokio::spawn(async move {
                // Mix of requests
                let uri = if batch % 2 == 0 { "/" } else { "/api/metrics" };
                app_clone
                    .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                    .await
            });
            handles.push(handle);
        }

        let mut batch_success = 0;
        for handle in handles {
            if let Ok(Ok(response)) = handle.await {
                if response.status() == StatusCode::OK {
                    batch_success += 1;
                }
            }
        }

        assert_eq!(
            batch_success, 20,
            "Batch {} should complete all 20 requests successfully, got {}",
            batch + 1,
            batch_success
        );
    }

    println!("=== Resource Exhaustion Test ===");
    println!("5 batches of 20 concurrent requests completed successfully");
    println!("No resource exhaustion detected");
}

/// Test: Verify response consistency under load
/// All responses should contain the same structure regardless of concurrent access
#[tokio::test]
async fn test_response_consistency_under_concurrent_load() {
    let app = create_test_router_with_metrics();

    let mut handles = Vec::with_capacity(CONCURRENT_USERS);

    for _ in 0..CONCURRENT_USERS {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let response = app_clone
                .oneshot(
                    Request::builder()
                        .uri("/api/metrics")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8_lossy(&body).to_string()
        });
        handles.push(handle);
    }

    let mut all_responses = Vec::with_capacity(CONCURRENT_USERS);

    for handle in handles {
        if let Ok(response_body) = handle.await {
            all_responses.push(response_body);
        }
    }

    assert_eq!(
        all_responses.len(),
        CONCURRENT_USERS,
        "All {} requests should complete",
        CONCURRENT_USERS
    );

    // Verify all responses have the same structure
    let first_response: serde_json::Value =
        serde_json::from_str(&all_responses[0]).expect("First response should be valid JSON");

    for (idx, response) in all_responses.iter().enumerate().skip(1) {
        let parsed: serde_json::Value =
            serde_json::from_str(response).expect("Response should be valid JSON");

        // All responses should have the same keys
        let first_keys: std::collections::HashSet<_> = first_response
            .as_object()
            .unwrap()
            .keys()
            .collect();
        let current_keys: std::collections::HashSet<_> =
            parsed.as_object().unwrap().keys().collect();

        assert_eq!(
            first_keys, current_keys,
            "Response {} has different keys than first response",
            idx
        );
    }

    println!("=== Response Consistency Test ===");
    println!(
        "All {} responses have consistent JSON structure",
        CONCURRENT_USERS
    );
}
