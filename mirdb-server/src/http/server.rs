//! HTTP server implementation using hyper 0.12 (tokio 0.1 compatible).
//!
//! Owner: Scenario 16 - HTTP Server Integration
//!
//! Exports:
//! - HttpServer: Main HTTP server struct
//! - HttpConfig: Server configuration
//! - start_http_server: Start the HTTP server alongside Memcached
//! - SharedState: Thread-safe state shared between HTTP and Memcached servers

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use futures::{future, Future, Stream};
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use log::{error, info};

use crate::http::api::metrics::{MetricsResponse, MetricsState};
use crate::http::api::status::{ServerState, StatusResponse};
use crate::http::routes;
use crate::store::Store;

/// HTTP server configuration
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// HTTP server listen address
    pub addr: SocketAddr,
    /// Memcached protocol endpoint address (for status display)
    pub memcached_addr: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:8080".parse().unwrap(),
            memcached_addr: "0.0.0.0:12333".to_string(),
        }
    }
}

impl HttpConfig {
    /// Create a new HTTP config with the specified port
    pub fn with_port(port: u16) -> Self {
        Self {
            addr: format!("0.0.0.0:{}", port).parse().unwrap(),
            memcached_addr: "0.0.0.0:12333".to_string(),
        }
    }

    /// Create HTTP config from addresses
    pub fn new(http_addr: SocketAddr, memcached_addr: String) -> Self {
        Self {
            addr: http_addr,
            memcached_addr,
        }
    }
}

/// Shared state between HTTP server and Memcached server
pub struct SharedState {
    /// Server state for status endpoint
    pub server_state: ServerState,
    /// Metrics state for metrics endpoint
    pub metrics_state: MetricsState,
    /// Reference to the key-value store
    store: Option<Arc<Store>>,
}

impl std::fmt::Debug for SharedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedState")
            .field("server_state", &self.server_state)
            .field("metrics_state", &self.metrics_state)
            .field("store", &self.store.as_ref().map(|_| "<Store>"))
            .finish()
    }
}

impl SharedState {
    /// Create new shared state
    pub fn new() -> Self {
        Self {
            server_state: ServerState::new(),
            metrics_state: MetricsState::new(),
            store: None,
        }
    }

    /// Create shared state with custom endpoint configuration
    pub fn with_endpoint(host: String, port: u16) -> Self {
        Self {
            server_state: ServerState::with_endpoint(host, port),
            metrics_state: MetricsState::new(),
            store: None,
        }
    }

    /// Set the store reference
    pub fn set_store(&mut self, store: Arc<Store>) {
        self.store = Some(store);
    }

    /// Get reference to the store
    pub fn store(&self) -> Option<&Arc<Store>> {
        self.store.as_ref()
    }
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe shared state wrapper
pub type AppState = Arc<RwLock<SharedState>>;

/// Create a new shared application state
pub fn create_app_state() -> AppState {
    Arc::new(RwLock::new(SharedState::new()))
}

/// Create shared state with endpoint configuration
pub fn create_app_state_with_endpoint(host: String, port: u16) -> AppState {
    Arc::new(RwLock::new(SharedState::with_endpoint(host, port)))
}

/// HTTP service handler type
type BoxFut = Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send>;

/// Handle incoming HTTP requests
fn handle_request(req: Request<Body>, state: AppState) -> BoxFut {
    let response = routes::route_request(req, state);
    Box::new(future::ok(response))
}

/// Start the HTTP server
///
/// This function starts an HTTP server on the configured address and runs it
/// alongside the existing Memcached protocol server.
///
/// # Arguments
/// * `config` - HTTP server configuration
/// * `state` - Shared application state
///
/// # Returns
/// A future that completes when the server shuts down
pub fn start_http_server(
    config: HttpConfig,
    state: AppState,
) -> impl Future<Item = (), Error = ()> {
    let addr = config.addr;

    // Update the endpoint info in the shared state
    if let Ok(mut state_guard) = state.write() {
        let endpoint = crate::http::api::status::EndpointInfo::from_addr_string(&config.memcached_addr);
        state_guard.server_state = ServerState::with_endpoint(endpoint.host, endpoint.port);
    }

    let make_service = move || {
        let state = Arc::clone(&state);
        service_fn(move |req| handle_request(req, Arc::clone(&state)))
    };

    let server = Server::bind(&addr)
        .serve(make_service)
        .map_err(move |e| {
            error!("HTTP server error: {}", e);
        });

    info!("HTTP server listening on http://{}", addr);

    server
}

/// Run the HTTP server (blocking)
///
/// This is a convenience function that runs the HTTP server using tokio::run.
/// For integration with an existing tokio runtime, use start_http_server instead.
///
/// # Arguments
/// * `config` - HTTP server configuration
/// * `state` - Shared application state
pub fn run_http_server(config: HttpConfig, state: AppState) {
    let server = start_http_server(config, state);
    tokio::run(server);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_config_default() {
        let config = HttpConfig::default();
        assert_eq!(config.addr.port(), 8080);
        assert_eq!(config.memcached_addr, "0.0.0.0:12333");
    }

    #[test]
    fn test_http_config_with_port() {
        let config = HttpConfig::with_port(3000);
        assert_eq!(config.addr.port(), 3000);
    }

    #[test]
    fn test_http_config_new() {
        let http_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let config = HttpConfig::new(http_addr, "localhost:11211".to_string());
        assert_eq!(config.addr.port(), 9000);
        assert_eq!(config.memcached_addr, "localhost:11211");
    }

    #[test]
    fn test_shared_state_new() {
        let state = SharedState::new();
        assert!(state.store().is_none());
    }

    #[test]
    fn test_shared_state_with_endpoint() {
        let state = SharedState::with_endpoint("192.168.1.1".to_string(), 5000);
        assert_eq!(state.server_state.endpoint().host, "192.168.1.1");
        assert_eq!(state.server_state.endpoint().port, 5000);
    }

    #[test]
    fn test_create_app_state() {
        let state = create_app_state();
        let guard = state.read().unwrap();
        assert!(guard.store().is_none());
    }

    #[test]
    fn test_create_app_state_with_endpoint() {
        let state = create_app_state_with_endpoint("10.0.0.1".to_string(), 8888);
        let guard = state.read().unwrap();
        assert_eq!(guard.server_state.endpoint().host, "10.0.0.1");
        assert_eq!(guard.server_state.endpoint().port, 8888);
    }
}
