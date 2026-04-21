//! HTTP server setup and initialization.
//! Owner: Scenario 1 - HTTP Server Startup and Coexistence
//!
//! Expected exports:
//! - start_web_server(addr, store, config) -> Result<(), Error>
//!
//! Integration:
//! - Uses shared Tokio runtime via tokio::spawn
//! - Receives Arc<Store> for shared state access
//! - Reads web port from Config struct

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::serve;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::store::Store;
use crate::web::handlers::config::create_config_state;
use crate::web::routes::{create_router_with_state, AppState};

/// Start the HTTP web server
///
/// This function creates an Axum-based HTTP server that shares the Tokio runtime
/// with the Memcached protocol server. It serves:
/// - Static homepage with real-time metrics dashboard
/// - JSON API endpoints for metrics, configuration, and key-value operations
///
/// # Arguments
/// * `addr` - The socket address to bind to
/// * `store` - Shared reference to the Store for data access
/// * `config` - Server configuration
///
/// # Returns
/// * `io::Result<()>` - Ok if server runs successfully, Err on failure
pub async fn start_web_server(
    addr: SocketAddr,
    store: Arc<Store>,
    config: Config,
) -> io::Result<()> {
    // Parse config values to create the config state for the web API
    let config_state = create_config_state_from_config(&config);

    let state = AppState {
        store,
        config,
        config_state,
    };

    let app = create_router_with_state(state);

    let listener = TcpListener::bind(addr).await?;
    log::info!("HTTP server listening on {}", addr);

    serve(listener, app).await
}

/// Create a ConfigState from the application Config
fn create_config_state_from_config(
    config: &Config,
) -> crate::web::handlers::config::SharedConfigState {
    // Parse size strings to bytes (simplified parsing)
    let parse_size = |s: &str| -> usize {
        let s = s.trim();
        if s.ends_with('M') {
            s[..s.len() - 1].parse::<usize>().unwrap_or(0) * 1024 * 1024
        } else if s.ends_with('K') {
            s[..s.len() - 1].parse::<usize>().unwrap_or(0) * 1024
        } else if s.ends_with('G') {
            s[..s.len() - 1].parse::<usize>().unwrap_or(0) * 1024 * 1024 * 1024
        } else {
            s.parse::<usize>().unwrap_or(0)
        }
    };

    create_config_state(
        config.addr.clone(),
        config.max_level,
        config.work_dir.clone(),
        parse_size(&config.sst_max_size),
        parse_size(&config.mem_table_max_size),
        parse_size(&config.block_size),
    )
}

/// Check if a port is available for binding
pub fn is_port_available(port: u16) -> bool {
    std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
}

/// Validate that the web port does not conflict with the Memcached port
pub fn validate_port_config(memcached_addr: &str, web_port: u16) -> Result<(), String> {
    // Parse the memcached address to get the port
    if let Some(memcached_port_str) = memcached_addr.split(':').last() {
        if let Ok(memcached_port) = memcached_port_str.parse::<u16>() {
            if memcached_port == web_port {
                return Err(format!(
                    "HTTP port {} conflicts with Memcached port {}. Please use different ports.",
                    web_port, memcached_port
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_port_config_no_conflict() {
        assert!(validate_port_config("0.0.0.0:12333", 8080).is_ok());
    }

    #[test]
    fn test_validate_port_config_conflict() {
        let result = validate_port_config("0.0.0.0:12333", 12333);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("conflicts"));
    }

    #[test]
    fn test_validate_port_config_custom_ports() {
        assert!(validate_port_config("127.0.0.1:9000", 8080).is_ok());
        assert!(validate_port_config("127.0.0.1:9000", 9000).is_err());
    }
}
