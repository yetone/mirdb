//! HTTP Server Implementation
//! Owner: Scenario 13 - HTTP Server Integration
//!
//! Expected exports:
//! - HttpServer: Main server struct wrapping hyper
//! - start(addr: SocketAddr, store: Arc<Store>) -> Result<()>
//! - Integration with existing tokio runtime

use std::net::SocketAddr;
use std::sync::Arc;

/// HTTP Server for MirDB homepage
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
