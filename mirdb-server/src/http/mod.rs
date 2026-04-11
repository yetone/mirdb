//! HTTP Server Module for MirDB Homepage
//!
//! This module provides an HTTP server that serves the MirDB homepage
//! and REST API endpoints for key-value operations and server monitoring.
//!
//! The HTTP server integrates with the existing tokio async runtime and runs
//! concurrently with the memcached server without blocking operations (NFR-7).
//!
//! # Exports
//! - `HttpServer`: Server struct for managing the HTTP server lifecycle
//! - `HttpContext`: Context for request handling with shared Store access
//! - `start_http_server()`: Launch HTTP server alongside memcached (non-blocking)
//! - Route handlers for all API endpoints

pub mod handlers;
pub mod router;
pub mod server;
pub mod static_assets;

pub use server::{HttpContext, HttpServer, start_http_server, run_http_server_blocking};
