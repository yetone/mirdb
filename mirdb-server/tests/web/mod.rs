//! Web server integration tests.
//!
//! Submodules:
//! - server_test: HTTP server startup and coexistence (Scenario 1)
//! - static_files_test: Static asset serving (Scenario 2)
//! - metrics_api_test: /api/metrics endpoint (Scenario 3)
//! - config_api_test: /api/config endpoint (Scenario 4)
//! - kv_api_test: /api/kv/* endpoints (Scenarios 5, 6, 7)

mod config_api_test;
mod kv_api_test;
mod metrics_api_test;
mod server_test;
mod static_files_test;

// Other test modules will be added by their respective scenarios:
// mod health_api_test;
// mod concurrency_test;
// mod error_handling_test;
