//! HTTP server initialization and startup.
//! Owner: Scenario 1 - Homepage Loading, Scenario 8 - Configuration
//!
//! Uses tiny_http for a simple synchronous HTTP server that doesn't
//! conflict with the existing tokio 0.1 runtime used by the Memcached
//! TCP server.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use tiny_http::{Header, Method, Response, Server, StatusCode};

use crate::store::Store;

/// Default HTTP port for the web interface
pub const DEFAULT_HTTP_PORT: u16 = 8080;

/// Start the HTTP server in a background thread
pub fn start_http_server(port: u16, store: Arc<Store>) {
    thread::spawn(move || {
        run_http_server(port, store);
    });
}

/// Run the HTTP server (blocking)
pub fn run_http_server(port: u16, store: Arc<Store>) {
    let addr = format!("0.0.0.0:{}", port);
    let server = match Server::http(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to start HTTP server: {}", e);
            return;
        }
    };

    println!("HTTP server started on http://{}", addr);

    for request in server.incoming_requests() {
        let response = handle_request(&request, &store);
        let _ = request.respond(response);
    }
}

fn handle_request(
    request: &tiny_http::Request,
    _store: &Arc<Store>,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let path = request.url();
    let method = request.method();

    // Only handle GET requests
    if *method != Method::Get {
        return Response::from_string("Method Not Allowed")
            .with_status_code(StatusCode(405))
            .with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
            );
    }

    match path {
        "/" | "/index.html" => serve_index(),
        "/static/css/style.css" => serve_css(),
        "/static/js/app.js" => serve_js(),
        "/api/stats" => serve_api_stats(),
        "/api/keys" => serve_api_keys(),
        "/api/compaction" => serve_api_compaction(),
        _ if path.starts_with("/api/keys/") => {
            let _key = &path[11..]; // Extract key from /api/keys/{key}
            serve_api_key_not_found()
        }
        _ => Response::from_string("Not Found")
            .with_status_code(StatusCode(404))
            .with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
            ),
    }
}

fn serve_index() -> Response<std::io::Cursor<Vec<u8>>> {
    let html = include_str!("../../static/index.html");
    Response::from_string(html).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap(),
    )
}

fn serve_css() -> Response<std::io::Cursor<Vec<u8>>> {
    let css = include_str!("../../static/css/style.css");
    Response::from_string(css).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"text/css; charset=utf-8"[..]).unwrap(),
    )
}

fn serve_js() -> Response<std::io::Cursor<Vec<u8>>> {
    let js = include_str!("../../static/js/app.js");
    Response::from_string(js).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/javascript; charset=utf-8"[..])
            .unwrap(),
    )
}

fn serve_api_stats() -> Response<std::io::Cursor<Vec<u8>>> {
    let json = r#"{"total_keys":0,"version":"0.1.0","uptime_seconds":0}"#;
    Response::from_string(json).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
    )
}

fn serve_api_keys() -> Response<std::io::Cursor<Vec<u8>>> {
    let json = r#"{"keys":[],"total":0,"offset":0,"limit":20}"#;
    Response::from_string(json).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
    )
}

fn serve_api_compaction() -> Response<std::io::Cursor<Vec<u8>>> {
    let json = r#"{"status":"idle","progress":0}"#;
    Response::from_string(json).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
    )
}

fn serve_api_key_not_found() -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_string(r#"{"error":"Key not found"}"#)
        .with_status_code(StatusCode(404))
        .with_header(
            Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        assert_eq!(DEFAULT_HTTP_PORT, 8080);
    }
}
