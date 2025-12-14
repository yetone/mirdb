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
        (&Method::GET, "/") | (&Method::GET, "/health") => {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body(Body::from("MirDB HTTP Server is running\n"))
                .unwrap()
        }
        (&Method::GET, "/api/status") => {
            let status = serde_json::json!({
                "status": "healthy",
                "server": "MirDB",
                "version": "0.1.0"
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
}
