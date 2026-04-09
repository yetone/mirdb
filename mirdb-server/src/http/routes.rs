//! Route definitions for the HTTP server.
//!
//! Owner: Scenario 16 - HTTP Server Integration
//! Co-owner for static routes: Scenario 17
//!
//! Expected routes:
//! - GET / -> Homepage HTML
//! - GET /api/status -> System status JSON
//! - GET /api/metrics -> Performance metrics JSON
//! - GET /static/* -> Static assets (Scenario 17)

use hyper::{Body, Method, Request, Response, StatusCode};
use log::debug;

use crate::http::api::metrics::{get_metrics, get_metrics_json};
use crate::http::api::status::{get_status, get_status_json};
use crate::http::server::AppState;

/// Route an incoming HTTP request to the appropriate handler
///
/// # Arguments
/// * `req` - The incoming HTTP request
/// * `state` - Shared application state
///
/// # Returns
/// HTTP response for the request
pub fn route_request(req: Request<Body>, state: AppState) -> Response<Body> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    debug!("HTTP {} {}", method, path);

    match (method, path.as_str()) {
        // Homepage route
        (Method::GET, "/") => handle_homepage(),

        // API routes
        (Method::GET, "/api/status") => handle_status(state),
        (Method::GET, "/api/metrics") => handle_metrics(state),

        // Health check endpoint
        (Method::GET, "/health") => handle_health(),

        // Static files (placeholder for Scenario 17)
        (Method::GET, path) if path.starts_with("/static/") => {
            handle_static_placeholder(path)
        }

        // 404 for everything else
        _ => handle_not_found(),
    }
}

/// Handle homepage request (GET /)
///
/// Returns a simple HTML page indicating the homepage is working.
/// The actual homepage content will be served by Scenario 17's static file serving.
fn handle_homepage() -> Response<Body> {
    let html = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MirDB Homepage</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 800px;
            margin: 50px auto;
            padding: 20px;
            background: #f5f5f5;
        }
        h1 { color: #333; }
        .status {
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #4caf50;
            margin-right: 8px;
        }
        code {
            background: #e8e8e8;
            padding: 2px 6px;
            border-radius: 3px;
        }
    </style>
</head>
<body>
    <h1>🗄️ MirDB</h1>
    <p>A Persistent Key-Value Store with Memcached Protocol</p>

    <div class="status">
        <p><span class="indicator"></span>Server is running</p>
        <p>Memcached Protocol: <code>localhost:12333</code></p>
        <p>HTTP Dashboard: <code>localhost:8080</code></p>
    </div>

    <h2>API Endpoints</h2>
    <ul>
        <li><a href="/api/status">/api/status</a> - Server status</li>
        <li><a href="/api/metrics">/api/metrics</a> - Performance metrics</li>
        <li><a href="/health">/health</a> - Health check</li>
    </ul>

    <h2>Quick Start</h2>
    <pre><code># Connect via telnet
telnet localhost 12333

# Set a value
set mykey 0 0 5
hello

# Get the value
get mykey</code></pre>
</body>
</html>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Body::from(html))
        .unwrap()
}

/// Handle status API request (GET /api/status)
fn handle_status(state: AppState) -> Response<Body> {
    let json = match state.read() {
        Ok(guard) => {
            match get_status_json(&guard.server_state) {
                Ok(json) => json,
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Failed to serialize status: {}", e),
                    );
                }
            }
        }
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to acquire state lock",
            );
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Body::from(json))
        .unwrap()
}

/// Handle metrics API request (GET /api/metrics)
fn handle_metrics(state: AppState) -> Response<Body> {
    let json = match state.read() {
        Ok(guard) => {
            match get_metrics_json(&guard.metrics_state) {
                Ok(json) => json,
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Failed to serialize metrics: {}", e),
                    );
                }
            }
        }
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to acquire state lock",
            );
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Body::from(json))
        .unwrap()
}

/// Handle health check request (GET /health)
fn handle_health() -> Response<Body> {
    let json = r#"{"status":"healthy"}"#;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

/// Placeholder handler for static files (to be implemented by Scenario 17)
fn handle_static_placeholder(path: &str) -> Response<Body> {
    // Scenario 17 will implement proper static file serving
    error_response(
        StatusCode::NOT_FOUND,
        &format!("Static file serving not yet implemented: {}", path),
    )
}

/// Handle 404 Not Found
fn handle_not_found() -> Response<Body> {
    error_response(StatusCode::NOT_FOUND, "Not Found")
}

/// Create an error response with JSON body
fn error_response(status: StatusCode, message: &str) -> Response<Body> {
    let json = format!(r#"{{"error":"{}"}}"#, message);

    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::server::create_app_state;

    #[test]
    fn test_handle_homepage() {
        let response = handle_homepage();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("Content-Type").unwrap().to_str().unwrap().contains("text/html"));
    }

    #[test]
    fn test_handle_health() {
        let response = handle_health();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap().to_str().unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_handle_status() {
        let state = create_app_state();
        let response = handle_status(state);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap().to_str().unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_handle_metrics() {
        let state = create_app_state();
        let response = handle_metrics(state);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap().to_str().unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_handle_not_found() {
        let response = handle_not_found();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_route_homepage() {
        let state = create_app_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .unwrap();

        let response = route_request(req, state);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_route_status() {
        let state = create_app_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/status")
            .body(Body::empty())
            .unwrap();

        let response = route_request(req, state);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_route_metrics() {
        let state = create_app_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/metrics")
            .body(Body::empty())
            .unwrap();

        let response = route_request(req, state);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_route_health() {
        let state = create_app_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = route_request(req, state);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_route_not_found() {
        let state = create_app_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/nonexistent")
            .body(Body::empty())
            .unwrap();

        let response = route_request(req, state);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_cors_headers() {
        let state = create_app_state();
        let response = handle_status(state);
        assert!(response.headers().get("Access-Control-Allow-Origin").is_some());
    }
}
