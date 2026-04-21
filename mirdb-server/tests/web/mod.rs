//! Web server integration tests.
//!
//! Submodules:
//! - static_files_test: Static asset serving (Scenario 2)
//! - metrics_api_test: /api/metrics endpoint (Scenario 3)

mod static_files_test;
mod metrics_api_test;

// Other test modules will be added by their respective scenarios:
// mod server_test;
// mod config_api_test;
// mod kv_api_test;
// mod health_api_test;
// mod concurrency_test;
// mod error_handling_test;
