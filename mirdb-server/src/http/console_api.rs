//! Interactive console API endpoint.
//! Owner: Scenario 4 - Interactive Console SET Command
//! Co-owners: Scenario 5 (GET), Scenario 6 (DELETE), Scenario 12 (errors), Scenario 14 (store integration)
//!
//! This module provides the /api/console endpoint for executing
//! memcached commands via HTTP.

use std::sync::Arc;

use crate::store::Store;
use super::handlers::{HttpRequest, HttpResponse};

/// Handle console command request
///
/// Accepts POST requests with memcached commands in the body
/// and returns the command response.
pub fn handle_console_command(request: &HttpRequest, _store: &Arc<Store>) -> HttpResponse {
    // Placeholder implementation - to be completed by Scenario 4, 5, 6
    let command = String::from_utf8_lossy(&request.body);

    // Return a basic response indicating the API is available
    let response_body = format!(
        r#"{{"status":"ok","message":"Console API is available. Command received: {}"}}"#,
        command.trim().chars().take(100).collect::<String>()
    );

    HttpResponse::ok_json(&response_body)
}

/// Parse a console request body into a command
#[allow(dead_code)]
pub fn parse_console_request(body: &[u8]) -> Option<String> {
    String::from_utf8(body.to_vec()).ok()
}

/// Format a command response for the web display
#[allow(dead_code)]
pub fn format_console_response(result: &str) -> String {
    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_console_request_valid() {
        let body = b"set key 0 0 5\r\nvalue";
        let result = parse_console_request(body);
        assert!(result.is_some());
        assert!(result.unwrap().contains("set key"));
    }

    #[test]
    fn test_format_console_response() {
        let response = format_console_response("STORED");
        assert_eq!(response, "STORED");
    }
}
