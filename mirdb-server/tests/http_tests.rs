//! HTTP API integration tests
//!
//! This module aggregates all HTTP-related tests for the MirDB Web Dashboard.

mod http;

// Re-export tests from submodules
pub use http::compaction_tests;
pub use http::error_handling_tests;
pub use http::key_operations_tests;
pub use http::static_assets_tests;
pub use http::status_tests;
