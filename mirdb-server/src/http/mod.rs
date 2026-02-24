//! HTTP server module for serving MirDB homepage.
//!
//! This module provides an HTTP server that runs alongside the
//! Memcached protocol server on a separate configurable port.
//!
//! Owner: Scenario 1 - HTTP Server Setup

pub mod assets;
pub mod routes;
pub mod server;

pub use self::server::run_http_server;
