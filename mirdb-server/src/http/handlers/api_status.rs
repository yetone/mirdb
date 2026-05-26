/**
 * API status JSON endpoint handler.
 * Owner: Scenario 6 - API Status JSON Endpoint
 *
 * Expected exports:
 * - fn handle(store: Arc<Store>) -> HttpResponse
 *   Returns HTTP 200 with JSON status data per Appendix A schema.
 *
 * Dependencies:
 * - store::stats() for retrieving metrics
 * - response::ok_json()
 *
 * Caching: Status data should be cached for 1 second minimum.
 */

use std::sync::Arc;

use crate::http::response::{ok_json, HttpResponse};
use crate::store::Store;

pub fn handle(_store: Arc<Store>) -> HttpResponse {
    let json = r#"{"version":"0.0.1","uptime_seconds":0,"keys_stored":0,"memory_usage_bytes":0}"#;
    ok_json(json)
}
