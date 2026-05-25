use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::http_handlers::{get_config_handler, get_health_handler};
use crate::options::Options;

pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

pub fn parse_request(stream: &mut TcpStream) -> Option<HttpRequest> {
    let mut reader = BufReader::new(stream);
    let mut first_line = String::new();
    if reader.read_line(&mut first_line).ok()? == 0 {
        return None;
    }

    let parts: Vec<&str> = first_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let method = parts[0].to_string();
    let path = parts[1].to_string();

    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some(pos) = line.find(':') {
            let name = line[..pos].trim().to_lowercase();
            let value = line[pos + 1..].trim().to_string();
            headers.push((name, value));
        }
    }

    let mut body = String::new();
    if let Some(content_length) = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse::<usize>().ok())
    {
        let mut buf = vec![0u8; content_length];
        if reader.read_exact(&mut buf).is_ok() {
            body = String::from_utf8_lossy(&buf).to_string();
        }
    }

    Some(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

pub fn get_origin_header(request: &HttpRequest) -> Option<&str> {
    request
        .headers
        .iter()
        .find(|(k, _)| k == "origin")
        .map(|(_, v)| v.as_str())
}

pub fn build_response(status: u16, body: &str, origin: Option<&str>) -> String {
    let status_text = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };

    let mut response = format!(
        "HTTP/1.1 {} {}\r\n",
        status, status_text
    );
    response.push_str("Content-Type: application/json\r\n");
    response.push_str(&format!("Content-Length: {}\r\n", body.len()));

    if let Some(origin) = origin {
        response.push_str(&format!("Access-Control-Allow-Origin: {}\r\n", origin));
    } else {
        response.push_str("Access-Control-Allow-Origin: *\r\n");
    }
    response.push_str("Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n");
    response.push_str("Access-Control-Allow-Headers: Content-Type\r\n");

    response.push_str("\r\n");
    response.push_str(body);
    response
}

pub fn handle_request(
    request: &HttpRequest,
    opt: &Arc<Options>,
    shutting_down: &Arc<AtomicBool>,
) -> String {
    let origin = get_origin_header(request);

    if request.method == "OPTIONS" {
        return build_cors_preflight_response(origin);
    }

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/api/config") => {
            let (status, body) = get_config_handler(opt);
            build_response(status, &body, origin)
        }
        ("GET", "/api/health") => {
            let (status, body) = get_health_handler(shutting_down);
            build_response(status, &body, origin)
        }
        ("POST", "/api/config") => {
            build_response(405, "{\"error\":\"Method Not Allowed\"}", origin)
        }
        _ => {
            build_response(404, "{\"error\":\"Not Found\"}", origin)
        }
    }
}

fn build_cors_preflight_response(origin: Option<&str>) -> String {
    let mut response = "HTTP/1.1 204 No Content\r\n".to_string();
    if let Some(origin) = origin {
        response.push_str(&format!("Access-Control-Allow-Origin: {}\r\n", origin));
    } else {
        response.push_str("Access-Control-Allow-Origin: *\r\n");
    }
    response.push_str("Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n");
    response.push_str("Access-Control-Allow-Headers: Content-Type\r\n");
    response.push_str("Access-Control-Max-Age: 86400\r\n");
    response.push_str("\r\n");
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_response_with_cors() {
        let response = build_response(200, "{\"status\":\"ok\"}", Some("http://localhost:3000"));
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("Access-Control-Allow-Origin: http://localhost:3000"));
        assert!(response.contains("Access-Control-Allow-Methods: GET, POST, OPTIONS"));
        assert!(response.contains("{\"status\":\"ok\"}"));
    }

    #[test]
    fn test_build_cors_preflight_response() {
        let response = build_cors_preflight_response(Some("http://localhost:3000"));
        assert!(response.contains("HTTP/1.1 204 No Content"));
        assert!(response.contains("Access-Control-Allow-Origin: http://localhost:3000"));
        assert!(response.contains("Access-Control-Allow-Methods: GET, POST, OPTIONS"));
        assert!(response.contains("Access-Control-Allow-Headers: Content-Type"));
    }
}
