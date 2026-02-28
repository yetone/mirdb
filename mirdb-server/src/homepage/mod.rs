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
pub mod routes;
pub mod state;

// Re-export main types
pub use routes::routes;
pub use state::AppState;
