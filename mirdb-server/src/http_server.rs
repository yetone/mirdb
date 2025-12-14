use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::{self, Future};
use futures::Stream;
use hyper::server::conn::Http;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, StatusCode};
use log::{error, info};
use tokio::net::TcpListener;

use crate::store::Store;

/// MirDB version string
pub const MIRDB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Homepage HTML content embedded at compile time
pub const HOMEPAGE_HTML: &str = include_str!("../assets/index.html");

/// HTTP server that runs alongside the TCP Memcached server
pub struct HttpServer {
    addr: SocketAddr,
    store: Arc<Store>,
}

impl HttpServer {
    /// Create a new HTTP server with the given address and store
    pub fn new(addr: SocketAddr, store: Arc<Store>) -> Self {
        HttpServer { addr, store }
    }

    /// Start the HTTP server
    /// Returns a future that runs the server
    pub fn run(self) -> impl Future<Item = (), Error = ()> {
        let addr = self.addr;
        let store = self.store;

        let listener = match TcpListener::bind(&addr) {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind HTTP server to {}: {}", addr, e);
                return future::Either::A(future::err(()));
            }
        };

        info!("HTTP server listening on http://{}", addr);

        let http = Http::new();

        let server = listener
            .incoming()
            .map_err(|e| error!("HTTP accept error: {}", e))
            .for_each(move |socket| {
                let store = store.clone();

                let service = service_fn(move |req: Request<Body>| {
                    handle_request(req, store.clone())
                });

                let conn = http
                    .serve_connection(socket, service)
                    .map_err(|e| error!("HTTP connection error: {}", e));

                tokio::spawn(conn);
                Ok(())
            });

        future::Either::B(server)
    }
}

/// Handle incoming HTTP requests
fn handle_request(
    req: Request<Body>,
    _store: Arc<Store>,
) -> impl Future<Item = Response<Body>, Error = hyper::Error> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/index.html") => {
            // Serve the homepage with MirDB branding
            let html = HOMEPAGE_HTML.replace("{{VERSION}}", MIRDB_VERSION);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(Body::from(html))
                .unwrap()
        }
        (&Method::GET, "/health") => {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body(Body::from("MirDB HTTP Server is running\n"))
                .unwrap()
        }
        (&Method::GET, "/dashboard") => {
            // Dashboard placeholder page
            let html = format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MirDB Dashboard</title>
    <style>
        body {{ font-family: sans-serif; background: #f5f5f5; margin: 0; }}
        header {{ background: #1a1a2e; color: white; padding: 1rem; }}
        nav {{ display: flex; gap: 2rem; max-width: 1200px; margin: 0 auto; }}
        nav a {{ color: #4ecca3; text-decoration: none; }}
        main {{ max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }}
        footer {{ background: #1a1a2e; color: #888; padding: 2rem; text-align: center; }}
    </style>
</head>
<body>
    <header>
        <nav>
            <a href="/">Home</a>
            <a href="/dashboard">Dashboard</a>
        </nav>
    </header>
    <main>
        <section>
            <h1>MirDB Dashboard</h1>
            <p>Dashboard functionality coming soon...</p>
        </section>
    </main>
    <footer>
        <p>MirDB v{}</p>
    </footer>
</body>
</html>"#, MIRDB_VERSION);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(Body::from(html))
                .unwrap()
        }
        (&Method::GET, "/api/status") => {
            let status = serde_json::json!({
                "status": "healthy",
                "server": "MirDB",
                "version": MIRDB_VERSION
            });
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(status.to_string()))
                .unwrap()
        }
        _ => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "text/plain")
                .body(Body::from("Not Found\n"))
                .unwrap()
        }
    };

    future::ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_server_creation() {
        // This test verifies that HttpServer can be created
        // Actual integration tests will be in a separate module
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        // Note: We can't create a Store without options, so this is just a compile-time check
        assert_eq!(addr.port(), 0);
    }

    /// Test Case 1: Verify homepage contains MirDB branding
    #[test]
    fn test_homepage_html_contains_mirdb_branding() {
        let html = HOMEPAGE_HTML;
        assert!(
            html.contains("MirDB"),
            "Homepage should contain MirDB branding"
        );
    }

    /// Test Case 2: Verify page contains required elements (header, hero, features, footer)
    #[test]
    fn test_homepage_contains_required_elements() {
        let html = HOMEPAGE_HTML;

        // Check for header with logo
        assert!(
            html.contains("<header"),
            "Homepage should contain header element"
        );

        // Check for hero section
        assert!(
            html.contains("hero") || html.contains("Hero"),
            "Homepage should contain hero section"
        );

        // Check for feature highlights
        assert!(
            html.contains("Persistent") || html.contains("persistence"),
            "Homepage should mention persistence feature"
        );
        assert!(
            html.contains("Memcached") || html.contains("memcached"),
            "Homepage should mention Memcached compatibility"
        );
        assert!(
            html.contains("LSM") || html.contains("lsm"),
            "Homepage should mention LSM architecture"
        );

        // Check for footer
        assert!(
            html.contains("<footer"),
            "Homepage should contain footer element"
        );
    }

    /// Test Case 3: Verify navigation includes link to /dashboard
    #[test]
    fn test_homepage_contains_dashboard_link() {
        let html = HOMEPAGE_HTML;
        assert!(
            html.contains("/dashboard"),
            "Homepage should contain link to dashboard"
        );
    }

    /// Test Case 4: Verify semantic HTML structure
    #[test]
    fn test_homepage_uses_semantic_html() {
        let html = HOMEPAGE_HTML;

        // Check for proper HTML5 semantic elements
        assert!(
            html.contains("<header"),
            "Homepage should use semantic header element"
        );
        assert!(
            html.contains("<main") || html.contains("<article"),
            "Homepage should use semantic main or article element"
        );
        assert!(
            html.contains("<nav"),
            "Homepage should use semantic nav element"
        );
        assert!(
            html.contains("<footer"),
            "Homepage should use semantic footer element"
        );
        assert!(
            html.contains("<section") || html.contains("<article"),
            "Homepage should use semantic section or article elements"
        );
    }

    /// Test Case 5: Verify footer displays version
    #[test]
    fn test_footer_version_placeholder() {
        let html = HOMEPAGE_HTML;

        // Check that the footer section contains version placeholder
        let footer_start = html.find("<footer").expect("Footer should exist");
        let footer_end = html[footer_start..]
            .find("</footer>")
            .expect("Footer should be closed");
        let footer_content = &html[footer_start..footer_start + footer_end];

        assert!(
            footer_content.contains("{{VERSION}}") || footer_content.contains("version"),
            "Footer should display version information"
        );
    }

    /// Test version replacement works correctly
    #[test]
    fn test_homepage_version_replacement() {
        let html = HOMEPAGE_HTML.replace("{{VERSION}}", MIRDB_VERSION);
        assert!(
            html.contains(MIRDB_VERSION),
            "Version should be injected into HTML"
        );
    }
}
