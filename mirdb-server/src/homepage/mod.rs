//! MirDB Homepage Module
//!
//! This module provides a web-based homepage for MirDB, including:
//! - HTTP server for serving static content
//! - JSON API endpoints for metrics, status, and configuration
//! - Theme support (dark/light mode)
//!
//! Owner: Scenario 1 - HTTP Server Initialization and Configuration
//!
//! Expected exports:
//! - pub mod server;
//! - pub mod routes;
//! - pub mod handlers;
//! - pub mod metrics;
//! - pub mod state;
//! - pub async fn start_homepage_server(config: &HomepageConfig, state: Arc<State>) -> Result<()>

pub mod handlers;
pub mod metrics;
pub mod routes;
pub mod server;
pub mod state;

// Re-export main types
pub use metrics::{Metrics, MetricsCache, MetricsCounters, SSTableLevel, CACHE_DURATION};
pub use routes::{routes, routes_with_metrics};
pub use server::{start_homepage_server, validate_port, HomepageServer, HomepageServerConfig, HomepageServerHandle};
// Re-export connection handling types (Scenario 11)
pub use server::{handle_rejection, BadRequest, MethodNotAllowed, RequestTimeout, UriTooLong, MAX_URL_LENGTH};
pub use state::AppState;
