/**
 * Homepage module for MirDB.
 *
 * This file is created by the first scenario builder and
 * serves as the entry point for the homepage feature.
 *
 * Expected exports:
 * - serve_homepage() -> Response: Returns the homepage HTML response
 * - serve_static(path) -> Response: Serves embedded CSS and image assets
 */

use std::io::{Read, Write};
use std::net::TcpStream;

pub mod assets;
pub mod html;

/// HTTP response status codes
pub const HTTP_OK: &str = "HTTP/1.1 200 OK";
pub const HTTP_NOT_FOUND: &str = "HTTP/1.1 404 Not Found";
pub const HTTP_METHOD_NOT_ALLOWED: &str = "HTTP/1.1 405 Method Not Allowed";

/// Serve the homepage HTML document
pub fn serve_homepage() -> String {
    let body = html::homepage_html();
    format!(
        "{}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        HTTP_OK,
        body.len(),
        body
    )
}

/// Serve static assets (CSS, images) by path
pub fn serve_static(path: &str) -> String {
    match path {
        "/styles.css" => {
            let body = assets::styles_css();
            format!(
                "{}\r\nContent-Type: text/css; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                HTTP_OK,
                body.len(),
                body
            )
        }
        _ => {
            let body = "Not Found";
            format!(
                "{}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                HTTP_NOT_FOUND,
                body.len(),
                body
            )
        }
    }
}

/// Handle an incoming HTTP request and write the response to the stream
pub fn handle_request(stream: &mut TcpStream) {
    let mut buffer = [0u8; 4096];
    match stream.read(&mut buffer) {
        Ok(n) if n > 0 => {
            let request = String::from_utf8_lossy(&buffer[..n]);
            let response = route_request(&request);
            let _ = stream.write_all(response.as_bytes());
        }
        _ => {}
    }
}

/// Route an HTTP request to the appropriate handler
fn route_request(request: &str) -> String {
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    if parts.len() < 2 {
        return format_error_response(HTTP_NOT_FOUND, "Not Found");
    }

    let method = parts[0];
    let path = parts[1];

    if method != "GET" {
        return format_error_response(HTTP_METHOD_NOT_ALLOWED, "Method Not Allowed");
    }

    match path {
        "/" => serve_homepage(),
        "/styles.css" => serve_static(path),
        _ => format_error_response(HTTP_NOT_FOUND, "Not Found"),
    }
}

fn format_error_response(status: &str, body: &str) -> String {
    format!(
        "{}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    )
}
