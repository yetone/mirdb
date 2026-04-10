//! Query execution API endpoint handler
//!
//! Owner: Scenario 6 - Interactive Query Builder
//!
//! Expected endpoint:
//! - POST /api/query -> QueryResponse
//!
//! Request: { "command": "stats" }
//! Response: { "response": "...", "success": true }
//!
//! This handler parses memcached commands sent via HTTP,
//! executes them against the store, and returns JSON responses.

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crate::parser::parse;
use crate::parser_util::macros::IRResult;
use crate::response::{BufferWriter, Response};
use crate::store::Store;

/// Request body for the query endpoint
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// The memcached command to execute (e.g., "stats", "get mykey")
    pub command: String,
}

/// Response body for the query endpoint
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// The raw response from the memcached command
    pub response: String,
    /// Whether the command executed successfully
    pub success: bool,
    /// Error message if the command failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Execution time in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_time_ms: Option<u64>,
}

impl QueryResponse {
    /// Create a successful response
    pub fn success(response: String, execution_time_ms: u64) -> Self {
        Self {
            response,
            success: true,
            error: None,
            execution_time_ms: Some(execution_time_ms),
        }
    }

    /// Create an error response
    pub fn error(message: String) -> Self {
        Self {
            response: String::new(),
            success: false,
            error: Some(message),
            execution_time_ms: None,
        }
    }
}

/// Execute a memcached command against the store
///
/// # Arguments
/// * `store` - The MirDB store instance
/// * `command` - The raw memcached command string
///
/// # Returns
/// A QueryResponse containing the result or error
pub fn execute_query(store: &Arc<Store>, command: &str) -> QueryResponse {
    let start = Instant::now();

    // Normalize the command: ensure it ends with \r\n
    let normalized = normalize_command(command);

    // Parse the command
    let request = match parse(normalized.as_bytes()) {
        IRResult::Ok((_, request)) => request,
        IRResult::Incomplete(_) => {
            return QueryResponse::error("Incomplete command - missing data".to_string());
        }
        IRResult::Err(_) => {
            return QueryResponse::error(format!("Invalid command: {}", command));
        }
    };

    // Execute the request against the store
    let response = match store.apply(request) {
        Ok(resp) => resp,
        Err(e) => {
            return QueryResponse::error(format!("Execution error: {}", e.msg));
        }
    };

    // Serialize the response to memcached protocol format
    let response_str = match serialize_response(&response) {
        Ok(s) => s,
        Err(e) => {
            return QueryResponse::error(format!("Failed to serialize response: {}", e));
        }
    };

    let elapsed = start.elapsed().as_millis() as u64;
    QueryResponse::success(response_str, elapsed)
}

/// Normalize a command by ensuring it ends with \r\n
fn normalize_command(command: &str) -> String {
    let trimmed = command.trim_end();
    format!("{}\r\n", trimmed)
}

/// Serialize a Response to its memcached protocol string representation
fn serialize_response(response: &Response) -> Result<String, String> {
    let mut buf = BytesMut::with_capacity(1024);
    {
        let mut writer = BufferWriter::new(&mut buf);
        response
            .write(&mut writer)
            .map_err(|e| format!("{}", e.msg))?;
    }

    String::from_utf8(buf.to_vec()).map_err(|e| format!("UTF-8 error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::get_test_opt;

    fn create_test_store() -> Arc<Store> {
        let opt = get_test_opt();
        Arc::new(Store::new(opt).unwrap())
    }

    #[test]
    fn test_execute_get_nonexistent() {
        let store = create_test_store();
        let result = execute_query(&store, "get nonexistent");

        assert!(result.success);
        assert!(result.response.contains("END"));
    }

    #[test]
    fn test_execute_set_and_get() {
        let store = create_test_store();

        // Set a key
        let set_result = execute_query(&store, "set testkey 0 3600 5\r\nhello");
        assert!(set_result.success, "Set should succeed: {:?}", set_result.error);
        assert!(set_result.response.contains("STORED"));

        // Get the key
        let get_result = execute_query(&store, "get testkey");
        assert!(get_result.success);
        assert!(get_result.response.contains("VALUE testkey"));
        assert!(get_result.response.contains("hello"));
    }

    #[test]
    fn test_execute_invalid_command() {
        let store = create_test_store();
        let result = execute_query(&store, "invalid_command");

        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_execute_delete() {
        let store = create_test_store();

        // Try to delete a nonexistent key
        let result = execute_query(&store, "delete nonexistent");
        assert!(result.success);
        assert!(result.response.contains("NOT_FOUND"));
    }

    #[test]
    fn test_execute_info() {
        let store = create_test_store();
        let result = execute_query(&store, "info");

        assert!(result.success);
        assert!(result.response.contains("INFO"));
    }

    #[test]
    fn test_normalize_command() {
        assert_eq!(normalize_command("get key"), "get key\r\n");
        assert_eq!(normalize_command("get key\r\n"), "get key\r\n");
        assert_eq!(normalize_command("get key  "), "get key\r\n");
    }
}
