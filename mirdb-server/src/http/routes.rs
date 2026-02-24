//! Route handlers for HTTP requests.
//!
//! Owner: Scenario 1 - HTTP Server Setup
//!
//! Provides routing logic for the HTTP server:
//! - GET / -> Returns index.html (embedded asset)
//! - GET /styles.css -> Returns CSS (embedded asset)
//! - GET /script.js -> Returns JS (embedded asset)
//! - Other paths -> 404 Not Found

use hyper::{Body, Request, Response, StatusCode};

use super::assets;

/// Handle incoming HTTP requests and route to appropriate handlers.
pub fn handle_request(req: Request<Body>) -> Response<Body> {
    let path = req.uri().path();

    // Try to serve the asset using the assets module
    if let Some((content, content_type)) = assets::get_asset(path) {
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", content_type)
            .body(Body::from(content))
            .unwrap();
    }

    // Return 404 for unknown paths
    not_found()
}

/// Return a 404 Not Found response.
fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(Body::from("404 Not Found"))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_request_root_returns_200() {
        let req = Request::builder()
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = handle_request(req);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_handle_request_css_returns_200() {
        let req = Request::builder()
            .uri("/styles.css")
            .body(Body::empty())
            .unwrap();
        let resp = handle_request(req);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_handle_request_js_returns_200() {
        let req = Request::builder()
            .uri("/script.js")
            .body(Body::empty())
            .unwrap();
        let resp = handle_request(req);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_handle_request_nonexistent_returns_404() {
        let req = Request::builder()
            .uri("/nonexistent")
            .body(Body::empty())
            .unwrap();
        let resp = handle_request(req);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
