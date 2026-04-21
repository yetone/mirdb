//! Request handlers for web endpoints.
//!
//! Submodules:
//! - static_files: Serves embedded HTML, CSS, JS assets
//! - metrics: Handles /api/metrics endpoint
//! - config: Handles /api/config endpoint
//! - kv: Handles /api/kv/* endpoints (set, get, delete)
//! - health: Handles /api/health endpoint

pub mod config;
pub mod health;
pub mod kv;
pub mod metrics;
pub mod static_files;
