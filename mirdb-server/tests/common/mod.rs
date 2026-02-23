//! Shared test utilities for MirDB HTTP tests.
//! Created by: First test scenario builder

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Send an HTTP request and return the response
pub fn send_http_request(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<HttpTestResponse, std::io::Error> {
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect(&addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Build request
    let body_bytes = body.unwrap_or("");
    let request = format!(
        "{} {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Connection: close\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        method, path, addr, body_bytes.len(), body_bytes
    );

    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    // Read response
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;

    HttpTestResponse::parse(&response)
}

/// Simple memcached command sender
pub fn send_memcached_command(
    host: &str,
    port: u16,
    command: &str,
) -> Result<String, std::io::Error> {
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect(&addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\r\n")?;
    stream.flush()?;

    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response)?;

    Ok(String::from_utf8_lossy(&response[..n]).to_string())
}

/// HTTP response for testing
#[derive(Debug)]
pub struct HttpTestResponse {
    pub status_code: u16,
    pub status_text: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl HttpTestResponse {
    fn parse(data: &[u8]) -> Result<Self, std::io::Error> {
        let response_str = String::from_utf8_lossy(data);
        let parts: Vec<&str> = response_str.splitn(2, "\r\n\r\n").collect();

        let header_section = parts.get(0).unwrap_or(&"");
        let body = parts.get(1).unwrap_or(&"").to_string();

        let mut lines = header_section.lines();

        // Parse status line
        let status_line = lines.next().unwrap_or("");
        let status_parts: Vec<&str> = status_line.splitn(3, ' ').collect();

        let status_code = status_parts
            .get(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let status_text = status_parts.get(2).unwrap_or(&"").to_string();

        // Parse headers
        let mut headers = Vec::new();
        for line in lines {
            if let Some(pos) = line.find(':') {
                let name = line[..pos].trim().to_string();
                let value = line[pos + 1..].trim().to_string();
                headers.push((name, value));
            }
        }

        Ok(HttpTestResponse {
            status_code,
            status_text,
            headers,
            body,
        })
    }

    pub fn get_header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Check if a port is available
pub fn is_port_available(port: u16) -> bool {
    std::net::TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok()
}

/// Find an available port
pub fn find_available_port() -> u16 {
    for port in 10000..65535 {
        if is_port_available(port) {
            return port;
        }
    }
    panic!("No available port found")
}
