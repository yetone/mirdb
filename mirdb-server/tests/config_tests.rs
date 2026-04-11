//! Configuration Tests
//! Owner: Scenario 8 - Configuration Display
//!
//! Top-level test file that includes config-related integration tests

mod http;

// Re-export config tests from http module
pub use http::config_tests::*;
