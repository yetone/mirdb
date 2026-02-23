//! HTTP server module for MirDB homepage.
//!
//! This module provides HTTP interface capabilities including:
//! - Static asset serving (homepage, CSS, JS)
//! - Interactive console API for memcached commands
//! - Server configuration and lifecycle management
//!
//! Created by: First HTTP scenario builder
//! Shared by: All HTTP-related scenarios

pub mod server;
pub mod handlers;
pub mod console_api;
pub mod assets;

// Re-export commonly used types
pub use server::HttpServer;
pub use handlers::get_homepage_html;
