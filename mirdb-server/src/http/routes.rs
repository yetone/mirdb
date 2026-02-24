//! Route handlers for HTTP requests.
//!
//! Owner: Scenario 1 - HTTP Server Setup
//!
//! Provides routing logic for the HTTP server:
//! - GET / -> Returns index.html (placeholder)
//! - GET /styles.css -> Returns CSS (placeholder)
//! - GET /script.js -> Returns JS (placeholder)
//! - Other paths -> 404 Not Found

use hyper::{Body, Request, Response, StatusCode};

/// Handle incoming HTTP requests and route to appropriate handlers.
pub fn handle_request(req: Request<Body>) -> Response<Body> {
    let path = req.uri().path();

    match path {
        "/" => serve_index(),
        "/styles.css" => serve_css(),
        "/script.js" => serve_js(),
        _ => not_found(),
    }
}

/// Serve the index.html page (placeholder content for now).
fn serve_index() -> Response<Body> {
    let html = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MirDB - Persistent Key-Value Store</title>
    <link rel="stylesheet" href="/styles.css">
</head>
<body>
    <header>
        <h1>MirDB</h1>
        <p>A persistent key-value store with Memcached protocol support</p>
    </header>
    <main>
        <section>
            <h2>Welcome to MirDB</h2>
            <p>MirDB is a high-performance, persistent key-value store that implements the Memcached protocol.</p>
        </section>
    </main>
    <script src="/script.js"></script>
</body>
</html>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Body::from(html))
        .unwrap()
}

/// Serve the styles.css file (placeholder content for now).
fn serve_css() -> Response<Body> {
    let css = r#"/* MirDB Homepage Styles - Placeholder */
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    margin: 0;
    padding: 20px;
    background-color: #f5f5f5;
    color: #333;
}
header {
    text-align: center;
    margin-bottom: 2rem;
}
h1 {
    color: #2563eb;
}
"#;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/css; charset=utf-8")
        .body(Body::from(css))
        .unwrap()
}

/// Serve the script.js file (placeholder content for now).
fn serve_js() -> Response<Body> {
    let js = r#"/* MirDB Homepage JavaScript - Placeholder */
console.log('MirDB Homepage loaded');
"#;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/javascript; charset=utf-8")
        .body(Body::from(js))
        .unwrap()
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
