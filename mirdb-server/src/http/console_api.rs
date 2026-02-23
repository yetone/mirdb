//! Interactive console API endpoint.
//! Owner: Scenario 4 - Interactive Console SET Command
//! Co-owners: Scenario 5 (GET), Scenario 6 (DELETE), Scenario 12 (errors), Scenario 14 (store integration)
//!
//! This module provides the /api/console endpoint for executing
//! memcached commands via HTTP.

use std::sync::Arc;

use crate::request::{Request, SetterType};
use crate::response::Response;
use crate::slice::Slice;
use crate::store::Store;
use super::handlers::{HttpRequest, HttpResponse};

/// Handle console command request
///
/// Accepts POST requests with memcached commands in the body
/// and returns the command response.
pub fn handle_console_command(request: &HttpRequest, store: &Arc<Store>) -> HttpResponse {
    let command = match parse_console_request(&request.body) {
        Some(cmd) => cmd,
        None => {
            return HttpResponse::ok_json(r#"{"response":"CLIENT_ERROR invalid encoding\r\n"}"#);
        }
    };

    // Parse and execute the command
    let response = match parse_and_execute_command(&command, store) {
        Ok(resp) => resp,
        Err(err) => err,
    };

    // Return the response as JSON
    let json_response = format!(r#"{{"response":"{}"}}"#, escape_json_string(&response));
    HttpResponse::ok_json(&json_response)
}

/// Parse and execute a memcached command
fn parse_and_execute_command(command: &str, store: &Arc<Store>) -> Result<String, String> {
    // Parse the command to determine its type
    let trimmed = command.trim();

    if trimmed.is_empty() {
        return Err("CLIENT_ERROR empty command\r\n".to_string());
    }

    // Check if this is a SET command
    if trimmed.starts_with("set ") || trimmed.starts_with("set\t") {
        return execute_set_command(command, store);
    }

    // Placeholder for other commands (GET, DELETE to be implemented by scenarios 5, 6)
    Err("ERROR unsupported command\r\n".to_string())
}

/// Execute a SET command
/// Format: set <key> <flags> <exptime> <bytes>\r\n<data>\r\n
fn execute_set_command(command: &str, store: &Arc<Store>) -> Result<String, String> {
    // Parse the SET command
    let parsed = parse_set_command(command)?;

    // Create the request
    let request = Request::Setter {
        setter: SetterType::Set,
        key: Slice::from(parsed.key.as_bytes().to_vec()),
        flags: parsed.flags,
        ttl: parsed.exptime,
        bytes: parsed.bytes,
        payload: Slice::from(parsed.data.clone()),
        no_reply: false,
    };

    // Execute the command against the store
    match store.apply(request) {
        Ok(Response::Stored) => Ok("STORED\r\n".to_string()),
        Ok(Response::NotStored) => Ok("NOT_STORED\r\n".to_string()),
        Ok(Response::ClientError(msg)) => Ok(format!("CLIENT_ERROR {}\r\n", msg)),
        Ok(Response::ServerError(msg)) => Ok(format!("SERVER_ERROR {}\r\n", msg)),
        Ok(_) => Ok("ERROR\r\n".to_string()),
        Err(e) => Ok(format!("SERVER_ERROR {:?}\r\n", e)),
    }
}

/// Parsed SET command structure
#[derive(Debug)]
struct ParsedSetCommand {
    key: String,
    flags: u32,
    exptime: u32,
    bytes: usize,
    data: Vec<u8>,
}

/// Parse a SET command string into its components
fn parse_set_command(command: &str) -> Result<ParsedSetCommand, String> {
    // Normalize line endings: convert \n to \r\n if necessary
    let normalized = normalize_line_endings(command);

    // Split by \r\n to get header and data
    let parts: Vec<&str> = normalized.splitn(2, "\r\n").collect();

    if parts.is_empty() {
        return Err("CLIENT_ERROR malformed command\r\n".to_string());
    }

    // Parse the header line: set <key> <flags> <exptime> <bytes>
    let header = parts[0];
    let header_parts: Vec<&str> = header.split_whitespace().collect();

    if header_parts.len() < 5 {
        return Err("CLIENT_ERROR invalid command format\r\n".to_string());
    }

    if header_parts[0] != "set" {
        return Err("CLIENT_ERROR expected set command\r\n".to_string());
    }

    let key = header_parts[1].to_string();

    let flags: u32 = header_parts[2]
        .parse()
        .map_err(|_| "CLIENT_ERROR invalid flags\r\n".to_string())?;

    let exptime: u32 = header_parts[3]
        .parse()
        .map_err(|_| "CLIENT_ERROR invalid exptime\r\n".to_string())?;

    let bytes: usize = header_parts[4]
        .parse()
        .map_err(|_| "CLIENT_ERROR invalid byte count\r\n".to_string())?;

    // Get the data block
    let data_section = if parts.len() > 1 { parts[1] } else { "" };

    // Remove trailing \r\n from data if present
    let data_cleaned = data_section.trim_end_matches("\r\n");
    let data_bytes = data_cleaned.as_bytes();

    // Check if we have enough data
    if data_bytes.len() < bytes {
        return Err(format!("CLIENT_ERROR bad data chunk: expected {} bytes, got {}\r\n", bytes, data_bytes.len()));
    }

    // Take exactly the number of bytes specified
    let data = data_bytes[..bytes].to_vec();

    Ok(ParsedSetCommand {
        key,
        flags,
        exptime,
        bytes,
        data,
    })
}

/// Normalize line endings: convert lone \n to \r\n
fn normalize_line_endings(input: &str) -> String {
    // Replace \r\n with a placeholder, then \n with \r\n, then restore \r\n
    let mut result = String::with_capacity(input.len() * 2);
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '\r' && i + 1 < chars.len() && chars[i + 1] == '\n' {
            result.push('\r');
            result.push('\n');
            i += 2;
        } else if chars[i] == '\n' {
            result.push('\r');
            result.push('\n');
            i += 1;
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}

/// Parse a console request body into a command string
pub fn parse_console_request(body: &[u8]) -> Option<String> {
    String::from_utf8(body.to_vec()).ok()
}

/// Format a command response for the web display
pub fn format_console_response(result: &str) -> String {
    result.to_string()
}

/// Escape special characters for JSON string
fn escape_json_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
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

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "hello");
        assert_eq!(escape_json_string("hello\nworld"), "hello\\nworld");
        assert_eq!(escape_json_string("hello\r\n"), "hello\\r\\n");
        assert_eq!(escape_json_string("say \"hi\""), "say \\\"hi\\\"");
    }

    #[test]
    fn test_normalize_line_endings() {
        assert_eq!(normalize_line_endings("hello\nworld"), "hello\r\nworld");
        assert_eq!(normalize_line_endings("hello\r\nworld"), "hello\r\nworld");
        assert_eq!(normalize_line_endings("a\nb\nc"), "a\r\nb\r\nc");
    }

    #[test]
    fn test_parse_set_command_valid() {
        let cmd = "set testkey 0 0 5\r\nhello\r\n";
        let result = parse_set_command(cmd);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, "testkey");
        assert_eq!(parsed.flags, 0);
        assert_eq!(parsed.exptime, 0);
        assert_eq!(parsed.bytes, 5);
        assert_eq!(parsed.data, b"hello");
    }

    #[test]
    fn test_parse_set_command_with_spaces_in_value() {
        let cmd = "set key_with_spaces 0 0 11\r\nhello world\r\n";
        let result = parse_set_command(cmd);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, "key_with_spaces");
        assert_eq!(parsed.bytes, 11);
        assert_eq!(parsed.data, b"hello world");
    }

    #[test]
    fn test_parse_set_command_with_lone_newlines() {
        let cmd = "set testkey 0 0 5\nhello\n";
        let result = parse_set_command(cmd);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, "testkey");
        assert_eq!(parsed.data, b"hello");
    }

    #[test]
    fn test_parse_set_command_malformed_missing_data() {
        let cmd = "set testkey 0 0 10\r\nhello\r\n";
        let result = parse_set_command(cmd);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CLIENT_ERROR"));
    }

    #[test]
    fn test_parse_set_command_malformed_invalid_flags() {
        let cmd = "set testkey abc 0 5\r\nhello\r\n";
        let result = parse_set_command(cmd);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid flags"));
    }

    #[test]
    fn test_parse_set_command_malformed_missing_parts() {
        let cmd = "set testkey\r\n";
        let result = parse_set_command(cmd);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CLIENT_ERROR"));
    }
}
