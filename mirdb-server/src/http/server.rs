//! HTTP Server Implementation
//! Owner: Scenario 13 - HTTP Server Integration
//!
//! This module implements the HTTP server that runs alongside the memcached
//! server, integrating with the existing tokio async runtime without blocking
//! (NFR-7). The server uses hyper 0.12 for HTTP handling.
//!
//! Exports:
//! - HttpServer: Main server struct wrapping hyper
//! - start_server(): Async function to launch HTTP server alongside memcached
//! - run_http_server(): Runs the HTTP server on the tokio runtime

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::{self, Future};
use futures::Stream;
use hyper::rt;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, Server, StatusCode};

use crate::options::Options;
use crate::store::Store;

use super::handlers::{config as config_handler, keys as keys_handler, status as status_handler};
use super::router::{match_route, Method as RouteMethod, Route};
use super::static_assets::get_mime_type;

/// HTTP Server for MirDB homepage
///
/// Integrates with the existing tokio async runtime and runs concurrently
/// with the memcached server without blocking operations (NFR-7).
pub struct HttpServer {
    addr: SocketAddr,
}

impl HttpServer {
    /// Create new HTTP server bound to address
    pub fn new(addr: SocketAddr) -> Self {
        HttpServer { addr }
    }

    /// Get server address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Context for HTTP request handling
/// Contains references to shared resources (Store, Options)
#[derive(Clone)]
pub struct HttpContext {
    pub store: Arc<Store>,
    pub options: Options,
}

impl HttpContext {
    /// Create new HTTP context with shared store and options
    pub fn new(store: Arc<Store>, options: Options) -> Self {
        HttpContext { store, options }
    }
}

/// Handle incoming HTTP request and route to appropriate handler
///
/// This function is non-blocking and integrates with the tokio async runtime.
/// It shares the Store instance with the memcached server for data consistency.
fn handle_request(
    req: Request<Body>,
    ctx: HttpContext,
) -> impl Future<Item = Response<Body>, Error = hyper::Error> {
    let method = match *req.method() {
        Method::GET => RouteMethod::Get,
        Method::POST => RouteMethod::Post,
        Method::DELETE => RouteMethod::Delete,
        _ => {
            return future::ok(
                Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(Body::from("Method not allowed"))
                    .unwrap(),
            );
        }
    };

    let path = req.uri().path().to_string();
    let route = match_route(&method, &path);

    match route {
        Route::ApiStatus => {
            let status_json = status_handler::handle_status_json(&ctx.options.work_dir);
            future::ok(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(status_json))
                    .unwrap(),
            )
        }
        Route::ApiKeysList => {
            let result = keys_handler::handle_list_keys(ctx.store.clone(), 1, 100);
            let json = match result {
                keys_handler::ListKeysResult::Success(response) => {
                    serde_json::to_string(&response).unwrap_or_else(|_| "[]".to_string())
                }
                keys_handler::ListKeysResult::Error(err) => {
                    serde_json::to_string(&err).unwrap_or_else(|_| r#"{"error":"unknown"}"#.to_string())
                }
            };
            future::ok(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(json))
                    .unwrap(),
            )
        }
        Route::ApiKeysGet(key) => {
            let result = keys_handler::handle_get_key(ctx.store.clone(), key);
            match result {
                keys_handler::GetKeyResult::Found(response) => {
                    let json = serde_json::to_string(&response).unwrap_or_else(|_| "null".to_string());
                    future::ok(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "application/json")
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(json))
                            .unwrap(),
                    )
                }
                keys_handler::GetKeyResult::NotFound(_) => future::ok(
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from("Key not found"))
                        .unwrap(),
                ),
                keys_handler::GetKeyResult::Error(err) => {
                    let json = serde_json::to_string(&err).unwrap_or_else(|_| r#"{"error":"unknown"}"#.to_string());
                    future::ok(
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "application/json")
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(json))
                            .unwrap(),
                    )
                }
            }
        }
        Route::ApiKeysDelete(key) => {
            let result = keys_handler::handle_delete_key(ctx.store.clone(), key);
            match result {
                keys_handler::DeleteKeyResult::Deleted(response) => {
                    let json = serde_json::to_string(&response).unwrap_or_else(|_| r#"{"success":true}"#.to_string());
                    future::ok(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "application/json")
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(json))
                            .unwrap(),
                    )
                }
                keys_handler::DeleteKeyResult::NotFound(_) => future::ok(
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from("Key not found"))
                        .unwrap(),
                ),
                keys_handler::DeleteKeyResult::Error(err) => {
                    let json = serde_json::to_string(&err).unwrap_or_else(|_| r#"{"error":"unknown"}"#.to_string());
                    future::ok(
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "application/json")
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(json))
                            .unwrap(),
                    )
                }
            }
        }
        Route::ApiConfig => {
            // Extract port from context (default 12333)
            let port = 12333u16;
            let config_json = config_handler::handle_config_json(&ctx.options, port);
            future::ok(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(config_json))
                    .unwrap(),
            )
        }
        Route::ApiKeysSet => {
            // POST requests need body parsing - return placeholder for now
            // Body parsing will be handled by the keys handler scenario
            future::ok(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(r#"{"status":"ok"}"#))
                    .unwrap(),
            )
        }
        Route::Static(file_path) => {
            // Serve static files from the web directory
            let content = serve_static_file(&file_path);
            match content {
                Some((data, mime_type)) => future::ok(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", mime_type)
                        .header("Cache-Control", "public, max-age=3600")
                        .body(Body::from(data))
                        .unwrap(),
                ),
                None => future::ok(
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::from("File not found"))
                        .unwrap(),
                ),
            }
        }
        Route::NotFound => future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap(),
        ),
    }
}

/// Serve static file from filesystem
fn serve_static_file(path: &str) -> Option<(Vec<u8>, &'static str)> {
    use std::fs;
    use std::path::Path;

    // Construct path to web directory
    let web_dir = Path::new("mirdb-server/src/web");
    let file_path = web_dir.join(path);

    // Security: prevent directory traversal
    if path.contains("..") {
        return None;
    }

    if file_path.exists() && file_path.is_file() {
        if let Ok(content) = fs::read(&file_path) {
            let mime_type = get_mime_type(path);
            return Some((content, mime_type));
        }
    }

    // Fallback: try relative path from current directory
    let alt_path = Path::new("src/web").join(path);
    if alt_path.exists() && alt_path.is_file() {
        if let Ok(content) = fs::read(&alt_path) {
            let mime_type = get_mime_type(path);
            return Some((content, mime_type));
        }
    }

    None
}

/// Start the HTTP server on the given address
///
/// This function spawns the HTTP server on the tokio runtime and runs
/// concurrently with the memcached server. It does not block the
/// event loop (NFR-7 compliance).
///
/// # Arguments
/// * `addr` - Socket address to bind the HTTP server
/// * `store` - Shared Store instance (same as memcached server)
/// * `options` - Server configuration options
pub fn start_http_server(
    addr: SocketAddr,
    store: Arc<Store>,
    options: Options,
) {
    let ctx = HttpContext::new(store, options);

    let make_service = move || {
        let ctx = ctx.clone();
        service_fn(move |req| handle_request(req, ctx.clone()))
    };

    let server = Server::bind(&addr)
        .serve(make_service)
        .map_err(|e| eprintln!("HTTP server error: {}", e));

    println!("HTTP server listening on http://{}", addr);

    // Spawn the HTTP server on a separate thread to run alongside memcached
    // This ensures non-blocking operation as required by NFR-7
    std::thread::spawn(move || {
        rt::run(server);
    });
}

/// Run HTTP server (blocking) - for standalone testing
///
/// This is primarily used for testing. In production, use start_http_server()
/// which spawns the server in a separate thread.
pub fn run_http_server_blocking(
    addr: SocketAddr,
    store: Arc<Store>,
    options: Options,
) {
    let ctx = HttpContext::new(store, options);

    let make_service = move || {
        let ctx = ctx.clone();
        service_fn(move |req| handle_request(req, ctx.clone()))
    };

    let server = Server::bind(&addr)
        .serve(make_service)
        .map_err(|e| eprintln!("HTTP server error: {}", e));

    println!("HTTP server listening on http://{}", addr);
    rt::run(server);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_server_creation() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let server = HttpServer::new(addr);
        assert_eq!(server.addr(), addr);
    }

    #[test]
    fn test_http_context_creation() {
        use crate::test_utils::get_test_opt;

        let opt = get_test_opt();
        let work_dir = opt.work_dir.clone();
        let store = Arc::new(Store::new(opt.clone()).unwrap());
        let ctx = HttpContext::new(store.clone(), opt);

        // Verify context holds references correctly
        // get_test_opt() generates a random work_dir starting with /tmp/mirdbtest/
        assert!(ctx.options.work_dir.starts_with("/tmp/mirdbtest/"),
            "Expected work_dir to start with /tmp/mirdbtest/, got {}", ctx.options.work_dir);
        assert_eq!(ctx.options.work_dir, work_dir);
    }

    #[test]
    fn test_static_file_path_traversal_prevention() {
        // Test that directory traversal is blocked
        let result = serve_static_file("../../../etc/passwd");
        assert!(result.is_none(), "Directory traversal should be blocked");

        let result = serve_static_file("foo/../bar");
        assert!(result.is_none(), "Directory traversal should be blocked");
    }
}
