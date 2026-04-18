//! Integration test module
//!
//! Tests cross-protocol consistency and performance.
//!
//! Test submodules:
//! - `protocol_compat_tests` - Scenario 7: Memcached Protocol Backward Compatibility
//! - `http_config_tests` - Scenario 8: HTTP Server Configuration
//! - `performance_tests` - Scenario 15: HTTP Performance Requirements

pub mod protocol_compat_tests;
pub mod http_config_tests;
pub mod performance_tests;
