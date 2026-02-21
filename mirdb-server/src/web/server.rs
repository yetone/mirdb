//! HTTP server implementation using hyper/tokio.
//!
//! Provides HTTP server struct with start capabilities
//! that integrates with the existing Tokio runtime.

use std::net::SocketAddr;

use futures::Future;
use hyper::rt;
use hyper::server::Server;
use hyper::service::service_fn_ok;
use hyper::{Body, Request, Response, StatusCode};

/// Configuration for the web server
#[derive(Debug, Clone)]
pub struct WebServerConfig {
    pub port: u16,
    pub static_dir: String,
}

/// HTTP web server for serving the MirDB homepage
pub struct WebServer {
    config: WebServerConfig,
}

impl WebServer {
    /// Create a new WebServer with the given configuration
    pub fn new(config: WebServerConfig) -> Self {
        WebServer { config }
    }

    /// Start the HTTP server
    /// This function blocks and runs the server until shutdown
    pub fn start(&self) {
        let addr: SocketAddr = ([0, 0, 0, 0], self.config.port).into();

        let server = Server::bind(&addr)
            .serve(|| service_fn_ok(handle_request))
            .map_err(|e| eprintln!("Web server error: {}", e));

        println!("Web server listening on http://{}", addr);

        rt::run(server);
    }

    /// Start the HTTP server in a separate thread
    /// Returns immediately, allowing the main thread to continue
    pub fn start_in_background(self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            self.start();
        })
    }
}

/// Handle incoming HTTP requests
fn handle_request(_req: Request<Body>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Body::from(
            r#"<!DOCTYPE html>
<html>
<head>
    <title>MirDB</title>
</head>
<body>
    <h1>MirDB</h1>
    <p>A persistent key-value store with Memcached protocol</p>
</body>
</html>"#,
        ))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_web_server_config() {
        let config = WebServerConfig {
            port: 8080,
            static_dir: "./web".to_string(),
        };

        let server = WebServer::new(config.clone());
        assert_eq!(server.config.port, 8080);
        assert_eq!(server.config.static_dir, "./web");
    }

    #[test]
    fn test_handle_request_returns_ok() {
        let req = Request::builder()
            .uri("/")
            .body(Body::empty())
            .unwrap();

        let response = handle_request(req);
        assert_eq!(response.status(), StatusCode::OK);
    }
}
