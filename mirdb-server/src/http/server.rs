//! HTTP server implementation using Tokio.
//! Owner: Scenario 1 - HTTP Server Initialization
//!
//! This module provides a minimal HTTP/1.1 server that runs alongside
//! the memcached TCP protocol server.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::store::Store;
use super::handlers::{handle_request, HttpRequest, HttpResponse};

/// HTTP Server configuration
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub bind_address: String,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        HttpServerConfig {
            port: 8080,
            bind_address: "0.0.0.0".to_string(),
        }
    }
}

impl HttpServerConfig {
    pub fn new(port: u16) -> Self {
        HttpServerConfig {
            port,
            bind_address: "0.0.0.0".to_string(),
        }
    }

    pub fn with_bind_address(mut self, addr: &str) -> Self {
        self.bind_address = addr.to_string();
        self
    }

    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }
}

/// HTTP Server that runs alongside the memcached protocol server
pub struct HttpServer {
    config: HttpServerConfig,
    store: Arc<Store>,
}

impl HttpServer {
    /// Create a new HTTP server instance
    pub fn new(config: HttpServerConfig, store: Arc<Store>) -> Self {
        HttpServer { config, store }
    }

    /// Start the HTTP server (blocking)
    pub fn run(&self) -> io::Result<()> {
        let addr = self.config.socket_addr();
        let listener = TcpListener::bind(&addr)?;

        println!("HTTP server listening on http://{}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let store = self.store.clone();
                    thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, store) {
                            eprintln!("Error handling HTTP connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Start the HTTP server in a background thread
    pub fn spawn(self) -> thread::JoinHandle<io::Result<()>> {
        thread::spawn(move || self.run())
    }

    /// Try to bind to the configured port (for testing port availability)
    pub fn try_bind(&self) -> io::Result<TcpListener> {
        TcpListener::bind(self.config.socket_addr())
    }
}

/// Handle a single HTTP connection
fn handle_connection(mut stream: TcpStream, store: Arc<Store>) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // Read the request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    if request_line.is_empty() {
        return Ok(());
    }

    // Parse the request line
    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        let response = HttpResponse::bad_request("Invalid request line");
        write_response(&mut stream, &response)?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    // Read headers
    let mut headers = Vec::new();
    let mut content_length: usize = 0;

    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;

        if line.trim().is_empty() {
            break;
        }

        // Parse Content-Length header
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(len_str) = line.split(':').nth(1) {
                content_length = len_str.trim().parse().unwrap_or(0);
            }
        }

        headers.push(line);
    }

    // Read body if present
    let mut body = Vec::new();
    if content_length > 0 {
        body.resize(content_length, 0);
        reader.read_exact(&mut body)?;
    }

    // Create request object
    let request = HttpRequest {
        method: method.to_string(),
        path: path.to_string(),
        headers,
        body,
    };

    // Handle the request
    let response = handle_request(&request, &store);

    // Write response
    write_response(&mut stream, &response)?;

    Ok(())
}

/// Write an HTTP response to the stream
fn write_response(stream: &mut TcpStream, response: &HttpResponse) -> io::Result<()> {
    // Status line
    write!(stream, "HTTP/1.1 {} {}\r\n", response.status_code, response.status_text)?;

    // Headers
    for (name, value) in &response.headers {
        write!(stream, "{}: {}\r\n", name, value)?;
    }

    // Content-Length header
    write!(stream, "Content-Length: {}\r\n", response.body.len())?;

    // End of headers
    write!(stream, "\r\n")?;

    // Body
    stream.write_all(&response.body)?;
    stream.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_server_config_default() {
        let config = HttpServerConfig::default();
        assert_eq!(config.port, 8080);
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.socket_addr(), "0.0.0.0:8080");
    }

    #[test]
    fn test_http_server_config_custom_port() {
        let config = HttpServerConfig::new(9090);
        assert_eq!(config.port, 9090);
        assert_eq!(config.socket_addr(), "0.0.0.0:9090");
    }

    #[test]
    fn test_http_server_config_with_bind_address() {
        let config = HttpServerConfig::new(8080)
            .with_bind_address("127.0.0.1");
        assert_eq!(config.bind_address, "127.0.0.1");
        assert_eq!(config.socket_addr(), "127.0.0.1:8080");
    }
}
