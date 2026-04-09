//! System status API endpoint.
//!
//! Owner: Scenario 2 - System Status Display
//! Co-owner: Scenario 18 - Product Version Display
//!
//! Expected exports:
//! - StatusResponse: Status response structure
//! - ServerStatus: Running/Stopped enum
//! - get_status(): Handler returning system status
//!
//! Response format:
//! {
//!     "status": "running" | "stopped",
//!     "uptime_seconds": u64,
//!     "version": "x.y.z",
//!     "endpoint": { "host": "...", "port": u16 }
//! }

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Server status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    Running,
    Stopped,
}

impl Default for ServerStatus {
    fn default() -> Self {
        ServerStatus::Running
    }
}

impl std::fmt::Display for ServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerStatus::Running => write!(f, "running"),
            ServerStatus::Stopped => write!(f, "stopped"),
        }
    }
}

/// Endpoint information for client connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointInfo {
    pub host: String,
    pub port: u16,
}

impl Default for EndpointInfo {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 11211,
        }
    }
}

/// Status response structure for /api/status endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Server status: "running" or "stopped"
    pub status: ServerStatus,
    /// Server uptime in seconds
    pub uptime_seconds: u64,
    /// MirDB version string
    pub version: String,
    /// Connection endpoint information
    pub endpoint: EndpointInfo,
}

impl Default for StatusResponse {
    fn default() -> Self {
        Self {
            status: ServerStatus::Running,
            uptime_seconds: 0,
            version: env!("CARGO_PKG_VERSION").to_string(),
            endpoint: EndpointInfo::default(),
        }
    }
}

/// Server state container for tracking status and uptime
#[derive(Debug)]
pub struct ServerState {
    start_time: Instant,
    status: ServerStatus,
    endpoint: EndpointInfo,
}

impl ServerState {
    /// Create a new server state instance
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            status: ServerStatus::Running,
            endpoint: EndpointInfo::default(),
        }
    }

    /// Create server state with custom endpoint configuration
    pub fn with_endpoint(host: String, port: u16) -> Self {
        Self {
            start_time: Instant::now(),
            status: ServerStatus::Running,
            endpoint: EndpointInfo { host, port },
        }
    }

    /// Get current uptime duration
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get current uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.uptime().as_secs()
    }

    /// Get current server status
    pub fn status(&self) -> ServerStatus {
        self.status
    }

    /// Set server status
    pub fn set_status(&mut self, status: ServerStatus) {
        self.status = status;
    }

    /// Get endpoint information
    pub fn endpoint(&self) -> &EndpointInfo {
        &self.endpoint
    }
}

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the current server status as a StatusResponse
///
/// This function generates a status response based on the current server state.
///
/// # Arguments
/// * `state` - Reference to the ServerState
///
/// # Returns
/// A StatusResponse containing current status, uptime, version, and endpoint info
pub fn get_status(state: &ServerState) -> StatusResponse {
    StatusResponse {
        status: state.status(),
        uptime_seconds: state.uptime_seconds(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        endpoint: state.endpoint().clone(),
    }
}

/// Generate JSON string response for status endpoint
///
/// # Arguments
/// * `state` - Reference to the ServerState
///
/// # Returns
/// JSON string of the status response
pub fn get_status_json(state: &ServerState) -> Result<String, serde_json::Error> {
    let response = get_status(state);
    serde_json::to_string(&response)
}

/// Create a status response for a running server (convenience function)
pub fn running_status(uptime_seconds: u64) -> StatusResponse {
    StatusResponse {
        status: ServerStatus::Running,
        uptime_seconds,
        version: env!("CARGO_PKG_VERSION").to_string(),
        endpoint: EndpointInfo::default(),
    }
}

/// Create a status response for a stopped server (convenience function)
pub fn stopped_status() -> StatusResponse {
    StatusResponse {
        status: ServerStatus::Stopped,
        uptime_seconds: 0,
        version: env!("CARGO_PKG_VERSION").to_string(),
        endpoint: EndpointInfo::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_status_serialization() {
        let running = ServerStatus::Running;
        let stopped = ServerStatus::Stopped;

        assert_eq!(serde_json::to_string(&running).unwrap(), "\"running\"");
        assert_eq!(serde_json::to_string(&stopped).unwrap(), "\"stopped\"");
    }

    #[test]
    fn test_server_status_display() {
        assert_eq!(ServerStatus::Running.to_string(), "running");
        assert_eq!(ServerStatus::Stopped.to_string(), "stopped");
    }

    #[test]
    fn test_status_response_default() {
        let response = StatusResponse::default();
        assert_eq!(response.status, ServerStatus::Running);
        assert_eq!(response.uptime_seconds, 0);
        assert_eq!(response.endpoint.host, "localhost");
        assert_eq!(response.endpoint.port, 11211);
    }

    #[test]
    fn test_server_state_uptime() {
        let state = ServerState::new();
        // Uptime should be at least 0 seconds
        assert!(state.uptime_seconds() >= 0);
    }

    #[test]
    fn test_server_state_with_endpoint() {
        let state = ServerState::with_endpoint("127.0.0.1".to_string(), 5000);
        assert_eq!(state.endpoint().host, "127.0.0.1");
        assert_eq!(state.endpoint().port, 5000);
    }

    #[test]
    fn test_get_status() {
        let state = ServerState::new();
        let response = get_status(&state);

        assert_eq!(response.status, ServerStatus::Running);
        assert_eq!(response.endpoint.host, "localhost");
        assert_eq!(response.endpoint.port, 11211);
        assert!(!response.version.is_empty());
    }

    #[test]
    fn test_get_status_json() {
        let state = ServerState::new();
        let json = get_status_json(&state).unwrap();

        // Verify JSON contains expected fields
        assert!(json.contains("\"status\":\"running\""));
        assert!(json.contains("\"uptime_seconds\":"));
        assert!(json.contains("\"version\":"));
        assert!(json.contains("\"endpoint\":"));
    }

    #[test]
    fn test_running_status_helper() {
        let response = running_status(3600);
        assert_eq!(response.status, ServerStatus::Running);
        assert_eq!(response.uptime_seconds, 3600);
    }

    #[test]
    fn test_stopped_status_helper() {
        let response = stopped_status();
        assert_eq!(response.status, ServerStatus::Stopped);
        assert_eq!(response.uptime_seconds, 0);
    }

    #[test]
    fn test_status_response_json_structure() {
        let response = StatusResponse {
            status: ServerStatus::Running,
            uptime_seconds: 120,
            version: "0.1.0".to_string(),
            endpoint: EndpointInfo {
                host: "192.168.1.1".to_string(),
                port: 11211,
            },
        };

        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["status"], "running");
        assert_eq!(parsed["uptime_seconds"], 120);
        assert_eq!(parsed["version"], "0.1.0");
        assert_eq!(parsed["endpoint"]["host"], "192.168.1.1");
        assert_eq!(parsed["endpoint"]["port"], 11211);
    }

    #[test]
    fn test_server_state_status_change() {
        let mut state = ServerState::new();
        assert_eq!(state.status(), ServerStatus::Running);

        state.set_status(ServerStatus::Stopped);
        assert_eq!(state.status(), ServerStatus::Stopped);

        state.set_status(ServerStatus::Running);
        assert_eq!(state.status(), ServerStatus::Running);
    }
}
