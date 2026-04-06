//! Route definitions for HTTP endpoints.
//! Owner: Scenario 1 - Basic Navigation, Scenario 13 - Static Files
//!
//! Note: With tiny_http, routing is handled directly in server.rs.
//! This module provides route path constants and documentation.
//!
//! Expected routes:
//! - GET /              -> serve index.html
//! - GET /static/*      -> serve static files (css, js)
//! - GET /api/stats     -> handlers::stats
//! - GET /api/keys      -> handlers::list_keys
//! - GET /api/keys/:key -> handlers::get_key
//! - GET /api/compaction -> handlers::compaction_status

/// Root path - serves the homepage
pub const ROUTE_INDEX: &str = "/";

/// Static CSS path (primary)
pub const ROUTE_CSS: &str = "/static/css/style.css";

/// Static CSS path (simplified alias for Scenario 13)
pub const ROUTE_CSS_SIMPLE: &str = "/static/style.css";

/// Static JS path (primary)
pub const ROUTE_JS: &str = "/static/js/app.js";

/// Static JS path (simplified alias for Scenario 13)
pub const ROUTE_JS_SIMPLE: &str = "/static/app.js";

/// API stats endpoint
pub const ROUTE_API_STATS: &str = "/api/stats";

/// API keys list endpoint
pub const ROUTE_API_KEYS: &str = "/api/keys";

/// API key detail endpoint prefix (append key name)
pub const ROUTE_API_KEY_PREFIX: &str = "/api/keys/";

/// API compaction status endpoint
pub const ROUTE_API_COMPACTION: &str = "/api/compaction";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_constants() {
        assert_eq!(ROUTE_INDEX, "/");
        assert_eq!(ROUTE_API_STATS, "/api/stats");
        assert_eq!(ROUTE_API_KEYS, "/api/keys");
    }
}
