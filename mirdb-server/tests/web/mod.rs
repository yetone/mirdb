//! Web server integration tests.
//!
//! Submodules:
//! - server_test: HTTP server startup and coexistence (Scenario 1)
//! - static_files_test: Static asset serving (Scenario 2)
//! - metrics_api_test: /api/metrics endpoint (Scenario 3)
//! - config_api_test: /api/config endpoint (Scenario 4)
//! - kv_api_test: /api/kv/* endpoints (Scenarios 5, 6, 7)
//! - concurrency_test: Load testing and concurrent requests (Scenario 16)
//! - error_handling_test: Error handling and edge cases (Scenario 17)

mod concurrency_test;
mod config_api_test;
mod error_handling_test;
mod health_api_test;
mod kv_api_test;
mod metrics_api_test;
mod server_test;
mod static_files_test;
