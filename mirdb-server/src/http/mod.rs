//! HTTP server module for MirDB Web Dashboard
//!
//! This module provides HTTP/JSON endpoints alongside the memcached protocol
//! for key-value operations and system monitoring.
//!
//! Submodules:
//! - `server`: HTTP server setup and routing
//! - `handlers`: API request handlers
//! - `types`: JSON request/response types
//! - `static_assets`: Embedded static file serving

pub mod server;
pub mod handlers;
pub mod types;
pub mod static_assets;
