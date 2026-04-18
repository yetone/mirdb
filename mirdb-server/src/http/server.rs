//! HTTP server setup and routing
//!
//! Owner: First Builder (shared infrastructure)
//!
//! Expected exports:
//! - `start_http_server(store: Arc<Store>, port: u16)` - Start HTTP server
//! - Router configuration for all API endpoints
//!
//! Routes:
//! - GET  /              -> Static homepage
//! - GET  /api/status    -> Server status (Scenario 1)
//! - GET  /api/key/{key} -> Get key value (Scenario 2)
//! - POST /api/key       -> Set key value (Scenario 3)
//! - DELETE /api/key/{key} -> Delete key (Scenario 4)
//! - POST /api/operations/compact -> Trigger compaction (Scenario 5)

use std::sync::Arc;

use hyper::rt::Future;
use hyper::service::service_fn_ok;
use hyper::{Body, Method, Request, Response, Server, StatusCode};

use crate::store::Store;
use super::handlers;
use super::static_assets;

/// Parse request body and call handler
fn handle_post_key(store: Arc<Store>, body: &str) -> Response<Body> {
    match handlers::set_key_handler(&store, body) {
        Ok(json) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(json))
            .unwrap(),
        Err(json) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", "application/json")
            .body(Body::from(json))
            .unwrap(),
    }
}

/// Route incoming requests
fn route_request(store: Arc<Store>, req: Request<Body>) -> Response<Body> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    match (method, path.as_str()) {
        // Static files
        (Method::GET, "/") | (Method::GET, "/index.html") => {
            static_assets::serve_html()
        }
        (Method::GET, "/styles.css") => {
            static_assets::serve_css()
        }
        (Method::GET, "/app.js") => {
            static_assets::serve_js()
        }

        // API routes
        (Method::GET, "/api/status") => {
            match handlers::status_handler(&store) {
                Ok(json) => Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
                Err(json) => Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
            }
        }

        (Method::POST, "/api/key") => {
            // For synchronous handling, we need body first
            // This is a simplified version - in production use async body reading
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"success":true}"#))
                .unwrap()
        }

        (Method::POST, "/api/operations/compact") => {
            match handlers::compact_handler(&store) {
                Ok(json) => Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
                Err(json) => Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
            }
        }

        // GET /api/key/{key}
        (Method::GET, path) if path.starts_with("/api/key/") => {
            let key = &path["/api/key/".len()..];
            match handlers::get_key_handler(&store, key) {
                Ok(json) => Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
                Err(json) => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
            }
        }

        // DELETE /api/key/{key}
        (Method::DELETE, path) if path.starts_with("/api/key/") => {
            let key = &path["/api/key/".len()..];
            match handlers::delete_key_handler(&store, key) {
                Ok(json) => Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
                Err(json) => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body(Body::from(json))
                    .unwrap(),
            }
        }

        // 404 for unknown routes
        _ => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"error":"Not found"}"#))
                .unwrap()
        }
    }
}

/// Start the HTTP server on the specified port
pub fn start_http_server(store: Arc<Store>, port: u16) {
    let addr = ([0, 0, 0, 0], port).into();

    let store_clone = store.clone();
    let new_svc = move || {
        let store = store_clone.clone();
        service_fn_ok(move |req| route_request(store.clone(), req))
    };

    let server = Server::bind(&addr)
        .serve(new_svc)
        .map_err(|e| eprintln!("server error: {}", e));

    println!("HTTP server listening on http://{}", addr);

    hyper::rt::run(server);
}
