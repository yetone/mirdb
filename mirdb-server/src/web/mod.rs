//! Web server module for MirDB homepage.
//!
//! This module provides an embedded HTTP server using Axum that serves:
//! - Static homepage with real-time metrics dashboard
//! - JSON API endpoints for metrics, configuration, and key-value operations
//!
//! The web server shares the Tokio runtime with the Memcached protocol server.

pub mod assets;
pub mod handlers;
pub mod routes;
pub mod types;

pub use routes::create_router;
pub use types::{ApiError, ApiResponse, ConfigResponse, MetricsResponse};
