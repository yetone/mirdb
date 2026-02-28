//! HTTP Server Implementation
//!
//! Uses warp framework to serve the homepage and API endpoints.
//! Runs in its own Tokio runtime to avoid conflicts with the main server.
//!
//! Owner: Scenario 1 - HTTP Server Initialization and Configuration

use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use crate::config::HomepageConfig;
use crate::error::{MyResult, StatusCode};
use crate::homepage::state::AppState;

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
async fn run_server(port: u16, state: Arc<AppState>) {
    use warp::Filter;

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    // Create state filter
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
}
