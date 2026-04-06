//! HTTP server module for MirDB web interface.
//!
//! This module provides a REST API and static file serving for the MirDB
//! dashboard. It runs alongside the existing Memcached TCP server.
//!
//! # Submodules
//! - `server`: Warp server setup and configuration
//! - `routes`: Route definitions for API and static files
//! - `handlers`: Request handlers for API endpoints
//! - `stats`: Statistics caching with TTL

pub mod handlers;
pub mod routes;
pub mod server;
pub mod stats;
