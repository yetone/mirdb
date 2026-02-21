//! Web server module for MirDB homepage.
//!
//! This module provides HTTP server functionality for serving
//! the MirDB homepage and static assets.
//!
//! # Modules
//! - `assets`: Static asset serving utilities (MIME types, caching)
//! - `routes`: Route handlers for HTTP endpoints

pub mod assets;
pub mod routes;

// Re-export commonly used types
pub use assets::{get_cache_control, get_extension, get_mime_type, is_safe_path};
pub use routes::{HttpResponse, RouteConfig, Router, StatusCode};
