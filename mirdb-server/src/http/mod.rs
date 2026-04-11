//! HTTP Server Module for MirDB Homepage
//!
//! This module provides an HTTP server that serves the MirDB homepage
//! and REST API endpoints for key-value operations and server monitoring.
//!
//! Expected exports:
//! - HttpServer struct for managing the HTTP server lifecycle
//! - start_server() function to launch HTTP server alongside memcached
//! - Route handlers for all API endpoints

pub mod handlers;
pub mod router;
pub mod server;
pub mod static_assets;

pub use server::HttpServer;
