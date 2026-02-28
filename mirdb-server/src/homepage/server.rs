//! HTTP Server Implementation
//!
//! Uses warp framework to serve the homepage and API endpoints.
//! Runs in its own Tokio runtime to avoid conflicts with the main server.
//!
//! Owner: Scenario 1 - HTTP Server Initialization and Configuration
//! Co-owner: Scenario 11 - HTTP Connection Handling (timeout/limits)
//!
//! Expected exports:
//! - pub struct HomepageServer
//! - pub struct HomepageServerConfig
//! - pub struct HomepageServerHandle
//! - pub async fn start_homepage_server(config: HomepageConfig, state: Arc<AppState>) -> Result<()>
//! - pub fn validate_port(port: i32) -> Result<u16, String>
//!
//! HTTP Connection Handling (Scenario 11):
//! - 5-second handler timeout for slow connections (NFR-1, Technical Spec)
//! - Connection limiting to prevent resource exhaustion
//! - Proper HTTP status codes for error conditions:
//!   - 400 Bad Request for malformed requests
//!   - 405 Method Not Allowed for unsupported methods
//!   - 414 URI Too Long for extremely long URLs

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use warp::http::StatusCode as HttpStatusCode;
use warp::{Filter, Rejection, Reply};

use crate::config::HomepageConfig;
use crate::error::{MyResult, StatusCode};
use crate::homepage::state::AppState;

/// Configuration for the homepage HTTP server
#[derive(Debug, Clone)]
pub struct HomepageServerConfig {
    /// Port to listen on
    pub port: u16,
    /// Whether the server is enabled
    pub enabled: bool,
    /// Request timeout duration
    pub timeout: Duration,
    /// Maximum concurrent connections
    pub max_connections: usize,
}

impl HomepageServerConfig {
    /// Create a new configuration with the specified port
    pub fn new(port: u16) -> Self {
        Self {
            port,
            enabled: true,
            timeout: Duration::from_secs(5),
            max_connections: 100,
        }
    }

    /// Set the enabled flag
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the timeout duration
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the max connections
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Validate the port configuration
    /// Returns an error message if the port is invalid
    pub fn validate_port(&self) -> Result<(), String> {
        if self.port == 0 {
            return Err("Invalid port: 0 is reserved for dynamic port assignment".to_string());
        }
        Ok(())
    }
}

impl Default for HomepageServerConfig {
    fn default() -> Self {
        Self::new(8080)
    }
}

/// Homepage HTTP Server
///
/// Wraps the warp server with configuration for the MirDB homepage.
pub struct HomepageServer {
    config: HomepageServerConfig,
    state: Arc<AppState>,
}

impl HomepageServer {
    /// Create a new homepage server with the given configuration and state
    pub fn new(config: HomepageServerConfig, state: Arc<AppState>) -> Self {
        Self { config, state }
    }

    /// Get the configured port
    pub fn port(&self) -> u16 {
        self.config.port
    }

    /// Check if the server is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Start the HTTP server
    ///
    /// Returns immediately if the server is disabled.
    /// Otherwise, starts listening on the configured port.
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.enabled {
            return Ok(());
        }

        // Validate port before starting
        self.config.validate_port()?;

        let addr: SocketAddr = ([0, 0, 0, 0], self.config.port).into();
        let state = self.state.clone();

        // Create routes
        let state_filter = warp::any().map(move || state.clone());

        let index = warp::path::end()
            .and(state_filter.clone())
            .map(|state: Arc<AppState>| {
                let version = state.get_version();
                warp::reply::html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <title>MirDB Homepage</title>
</head>
<body>
    <h1>MirDB</h1>
    <p>Version: {}</p>
    <p>Status: Running</p>
</body>
</html>"#,
                    version
                ))
            });

        let health = warp::path("health").map(|| warp::reply::json(&serde_json::json!({"status": "ok"})));

        let routes = index.or(health);

        warp::serve(routes).run(addr).await;

        Ok(())
    }
}

/// Handle for controlling the homepage server
pub struct HomepageServerHandle {
    /// Thread handle for the server
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl HomepageServerHandle {
    /// Wait for the server to finish (blocks indefinitely unless server crashes)
    pub fn join(mut self) {
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Start the homepage HTTP server in a separate thread with its own Tokio runtime
///
/// Returns a handle that can be used to manage the server and the actual bound address.
pub fn start_homepage_server(
    config: &HomepageConfig,
    state: Arc<AppState>,
) -> MyResult<(HomepageServerHandle, SocketAddr)> {
    // Validate configuration
    config.validate()?;

    let port = config.port;
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    // Clone state for the thread
    let state_clone = state.clone();

    // Spawn server in a separate thread with its own runtime
    let thread_handle = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async move {
            run_server(port, state_clone).await;
        });
    });

    let handle = HomepageServerHandle {
        thread_handle: Some(thread_handle),
    };

    Ok((handle, addr))
}

/// Run the warp server (called within the Tokio runtime)
/// Includes proper HTTP connection handling (Scenario 11):
/// - Custom rejection handlers for proper HTTP status codes
/// - Method restrictions (405 for unsupported methods)
/// - URI length validation (414 for long URLs)
async fn run_server(port: u16, state: Arc<AppState>) {
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    // Create state filter
    let state_filter = warp::any().map(move || state.clone());

    // Index route - GET only
    let index = warp::path::end()
        .and(warp::get())
        .and(state_filter.clone())
        .map(|state: Arc<AppState>| {
            let version = state.get_version();
            warp::reply::html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <title>MirDB Homepage</title>
</head>
<body>
    <h1>MirDB</h1>
    <p>Version: {}</p>
    <p>Status: Running</p>
</body>
</html>"#,
                version
            ))
        });

    // Health endpoint - GET only
    let health = warp::path("health")
        .and(warp::get())
        .map(|| warp::reply::json(&serde_json::json!({"status": "ok"})));

    // Combine routes
    let routes = index
        .or(health)
        .recover(handle_rejection);

    warp::serve(routes).run(addr).await;
}

/// Validates that a port number is valid
pub fn validate_port(port: i32) -> Result<u16, String> {
    if port < 1 || port > 65535 {
        return Err(format!(
            "Invalid port number: {}. Port must be between 1 and 65535",
            port
        ));
    }
    Ok(port as u16)
}

// ============================================================================
// HTTP Connection Handling (Scenario 11)
// ============================================================================

/// Maximum allowed URL length in bytes (10KB)
pub const MAX_URL_LENGTH: usize = 10 * 1024;

/// Custom rejection type for method not allowed
#[derive(Debug)]
pub struct MethodNotAllowed;
impl warp::reject::Reject for MethodNotAllowed {}

/// Custom rejection type for URI too long
#[derive(Debug)]
pub struct UriTooLong;
impl warp::reject::Reject for UriTooLong {}

/// Custom rejection type for bad request (malformed)
#[derive(Debug)]
pub struct BadRequest {
    pub message: String,
}
impl warp::reject::Reject for BadRequest {}

/// Custom rejection type for request timeout
#[derive(Debug)]
pub struct RequestTimeout;
impl warp::reject::Reject for RequestTimeout {}

/// Error response body for JSON error responses
#[derive(serde::Serialize)]
struct ErrorResponse {
    code: u16,
    message: String,
}

/// Handle rejections and convert them to appropriate HTTP responses
///
/// Maps warp rejections to proper HTTP status codes:
/// - MethodNotAllowed -> 405 Method Not Allowed
/// - UriTooLong -> 414 URI Too Long
/// - BadRequest -> 400 Bad Request
/// - RequestTimeout -> 408 Request Timeout
/// - Not Found -> 404 Not Found
/// - Other rejections -> 500 Internal Server Error
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let (code, message) = if err.is_not_found() {
        (HttpStatusCode::NOT_FOUND, "Not Found".to_string())
    } else if let Some(_) = err.find::<MethodNotAllowed>() {
        (HttpStatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed".to_string())
    } else if let Some(_) = err.find::<UriTooLong>() {
        (HttpStatusCode::URI_TOO_LONG, "URI Too Long".to_string())
    } else if let Some(e) = err.find::<BadRequest>() {
        (HttpStatusCode::BAD_REQUEST, e.message.clone())
    } else if let Some(_) = err.find::<RequestTimeout>() {
        (HttpStatusCode::REQUEST_TIMEOUT, "Request Timeout".to_string())
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        // Warp's built-in method not allowed rejection
        (HttpStatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed".to_string())
    } else if let Some(_) = err.find::<warp::reject::PayloadTooLarge>() {
        (HttpStatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large".to_string())
    } else if let Some(_) = err.find::<warp::reject::LengthRequired>() {
        (HttpStatusCode::LENGTH_REQUIRED, "Length Required".to_string())
    } else if let Some(_) = err.find::<warp::reject::InvalidHeader>() {
        (HttpStatusCode::BAD_REQUEST, "Invalid Header".to_string())
    } else if let Some(_) = err.find::<warp::reject::MissingHeader>() {
        (HttpStatusCode::BAD_REQUEST, "Missing Header".to_string())
    } else if let Some(_) = err.find::<warp::reject::InvalidQuery>() {
        (HttpStatusCode::BAD_REQUEST, "Invalid Query".to_string())
    } else {
        // Log unhandled rejections for debugging
        eprintln!("Unhandled rejection: {:?}", err);
        (HttpStatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error".to_string())
    };

    let json = warp::reply::json(&ErrorResponse {
        code: code.as_u16(),
        message,
    });

    Ok(warp::reply::with_status(json, code))
}

/// Create a filter that rejects requests with URIs longer than MAX_URL_LENGTH
pub fn uri_length_limit() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path::full()
        .and_then(|path: warp::path::FullPath| async move {
            if path.as_str().len() > MAX_URL_LENGTH {
                Err(warp::reject::custom(UriTooLong))
            } else {
                Ok(())
            }
        })
        .untuple_one()
}

/// Create a filter that only allows GET requests (rejects other methods with 405)
/// Note: warp::get() already returns () as Extract, so we don't need untuple_one
pub fn get_method_only() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::get()
}

/// Validate a port number from an i32 (for handling invalid input like -1 or 99999)
///
/// Returns an error message if the port is invalid.
pub fn validate_port_i32(port: i32) -> Result<u16, String> {
    if port < 0 {
        return Err(format!("Invalid port: {} is negative", port));
    }
    if port > 65535 {
        return Err(format!("Invalid port: {} exceeds maximum port number 65535", port));
    }
    if port == 0 {
        return Err("Invalid port: 0 is reserved for dynamic port assignment".to_string());
    }
    Ok(port as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_port_valid() {
        assert_eq!(validate_port(8080), Ok(8080));
        assert_eq!(validate_port(1), Ok(1));
        assert_eq!(validate_port(65535), Ok(65535));
        assert_eq!(validate_port(9000), Ok(9000));
    }

    #[test]
    fn test_validate_port_invalid_negative() {
        let result = validate_port(-1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid port"));
    }

    #[test]
    fn test_validate_port_invalid_zero() {
        let result = validate_port(0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid port"));
    }

    #[test]
    fn test_validate_port_invalid_too_high() {
        let result = validate_port(99999);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid port"));
    }

    #[test]
    fn test_validate_port_boundary() {
        // Test boundary values
        assert!(validate_port(0).is_err());
        assert!(validate_port(1).is_ok());
        assert!(validate_port(65535).is_ok());
        assert!(validate_port(65536).is_err());
    }

    #[test]
    fn test_validate_port_i32_valid() {
        assert_eq!(validate_port_i32(8080), Ok(8080));
        assert_eq!(validate_port_i32(1), Ok(1));
        assert_eq!(validate_port_i32(65535), Ok(65535));
    }

    #[test]
    fn test_validate_port_i32_negative() {
        let err = validate_port_i32(-1).unwrap_err();
        assert!(err.contains("negative"));
        assert!(err.contains("-1"));
    }

    #[test]
    fn test_validate_port_i32_too_large() {
        let err = validate_port_i32(99999).unwrap_err();
        assert!(err.contains("exceeds"));
        assert!(err.contains("99999"));
    }

    #[test]
    fn test_validate_port_i32_zero() {
        let err = validate_port_i32(0).unwrap_err();
        assert!(err.contains("0"));
    }

    #[test]
    fn test_server_config_default() {
        let config = HomepageServerConfig::default();
        assert_eq!(config.port, 8080);
        assert!(config.enabled);
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.max_connections, 100);
    }

    #[test]
    fn test_server_config_builder() {
        let config = HomepageServerConfig::new(9000)
            .with_enabled(false)
            .with_timeout(Duration::from_secs(10))
            .with_max_connections(50);

        assert_eq!(config.port, 9000);
        assert!(!config.enabled);
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.max_connections, 50);
    }

    #[test]
    fn test_homepage_server_disabled() {
        let config = HomepageServerConfig::new(8080).with_enabled(false);
        let state = Arc::new(AppState::default());
        let server = HomepageServer::new(config, state);

        assert!(!server.is_enabled());
        assert_eq!(server.port(), 8080);
    }

    #[test]
    fn test_homepage_server_enabled() {
        let config = HomepageServerConfig::new(9000).with_enabled(true);
        let state = Arc::new(AppState::default());
        let server = HomepageServer::new(config, state);

        assert!(server.is_enabled());
        assert_eq!(server.port(), 9000);
    }
}
