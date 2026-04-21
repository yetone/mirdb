//! Static file serving integration tests.
//! Owner: Scenario 2 - Homepage Static Content Serving
//!
//! Tests:
//! 1. GET / returns HTML with Content-Type: text/html and MirDB branding
//! 2. GET /static/style.css returns CSS with proper headers
//! 3. GET /static/main.js returns JavaScript with proper headers
//! 4. GET /nonexistent-file.css returns 404
//! 5. Homepage loads within performance requirements

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http::header;
use tower::ServiceExt;

use mirdb::web::routes::create_router;

/// Test Case 1: GET / returns HTML with Content-Type: text/html and MirDB branding
#[tokio::test]
async fn test_homepage_returns_html_with_mirdb_branding() {
    let app = create_router();

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Check Content-Type header
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("Content-Type header should be present");
    assert!(
        content_type.to_str().unwrap().contains("text/html"),
        "Content-Type should be text/html"
    );

    // Check body contains MirDB branding
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("MirDB"),
        "Homepage should contain MirDB branding"
    );
}

/// Test Case 2: GET /static/style.css returns CSS with Cache-Control header >= 3600s
#[tokio::test]
async fn test_css_served_with_correct_headers() {
    let app = create_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/style.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Check Content-Type header
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("Content-Type header should be present");
    assert!(
        content_type.to_str().unwrap().contains("text/css"),
        "Content-Type should be text/css"
    );

    // Check Cache-Control header (min 1 hour = 3600 seconds per NFR-4)
    let cache_control = response
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("Cache-Control header should be present");
    let cache_str = cache_control.to_str().unwrap();
    assert!(
        cache_str.contains("max-age="),
        "Cache-Control should include max-age"
    );

    // Extract max-age value and verify it's >= 3600
    if let Some(max_age_part) = cache_str.split("max-age=").nth(1) {
        let max_age: u32 = max_age_part
            .split(',')
            .next()
            .unwrap_or("0")
            .trim()
            .parse()
            .unwrap_or(0);
        assert!(
            max_age >= 3600,
            "Cache max-age should be at least 3600 seconds (1 hour), got {}",
            max_age
        );
    }

    // Verify CSS content
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains(":root"),
        "CSS should contain CSS variables"
    );
}

/// Test Case 3: GET /static/main.js returns JavaScript with correct content-type
#[tokio::test]
async fn test_javascript_served_with_correct_headers() {
    let app = create_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/main.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Check Content-Type header
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("Content-Type header should be present");
    let content_type_str = content_type.to_str().unwrap();
    assert!(
        content_type_str.contains("javascript")
            || content_type_str.contains("application/javascript"),
        "Content-Type should be application/javascript, got {}",
        content_type_str
    );

    // Verify JavaScript content
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("function") || body_str.contains("const") || body_str.contains("let"),
        "Response should contain JavaScript code"
    );
}

/// Test Case 4: GET /nonexistent-file.css returns 404 Not Found
#[tokio::test]
async fn test_nonexistent_file_returns_404() {
    let app = create_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/nonexistent-file.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Non-existent file should return 404"
    );
}

/// Test Case 5: Homepage load time performance (NFR-1: within 2 seconds)
/// Note: This tests that the response is returned quickly, not actual network time
#[tokio::test]
async fn test_homepage_load_performance() {
    use std::time::Instant;

    let app = create_router();

    let start = Instant::now();
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let duration = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);

    // The server-side response time should be well under 2 seconds
    // (actual page load time includes network, which we can't test here)
    assert!(
        duration.as_millis() < 2000,
        "Homepage should respond within 2 seconds, took {}ms",
        duration.as_millis()
    );

    // Also verify we can read the full body quickly
    let start = Instant::now();
    let _body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_duration = start.elapsed();

    assert!(
        body_duration.as_millis() < 1000,
        "Body should be readable within 1 second, took {}ms",
        body_duration.as_millis()
    );
}

/// Additional test: Verify all static files reference in HTML are available
#[tokio::test]
async fn test_all_referenced_assets_available() {
    let app = create_router();

    // Get the homepage
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);

    // Check that referenced CSS file exists
    assert!(
        html.contains("/static/style.css"),
        "HTML should reference style.css"
    );

    // Check that referenced JS file exists
    assert!(
        html.contains("/static/main.js"),
        "HTML should reference main.js"
    );

    // Verify CSS is actually served
    let css_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/static/style.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(css_response.status(), StatusCode::OK);

    // Verify JS is actually served
    let js_response = app
        .oneshot(
            Request::builder()
                .uri("/static/main.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(js_response.status(), StatusCode::OK);
}

/// Test: Verify HTML structure contains required sections
#[tokio::test]
async fn test_homepage_html_structure() {
    let app = create_router();

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);

    // Check required HTML structure
    assert!(html.contains("<!DOCTYPE html>"), "Should be valid HTML5");
    assert!(
        html.contains("<title>") && html.contains("MirDB"),
        "Should have title with MirDB"
    );
    assert!(html.contains("<header"), "Should have header section");
    assert!(html.contains("<main"), "Should have main section");
    assert!(html.contains("<footer"), "Should have footer section");

    // Check semantic sections
    assert!(html.contains("class=\"hero\""), "Should have hero section");
    assert!(
        html.contains("class=\"features\""),
        "Should have features section"
    );
    assert!(
        html.contains("class=\"dashboard\""),
        "Should have dashboard section"
    );
}
