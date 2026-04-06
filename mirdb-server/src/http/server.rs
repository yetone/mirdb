//! HTTP server initialization and startup.
//! Owner: Scenario 1 - Homepage Loading, Scenario 8 - Configuration
//!
//! Uses tiny_http for a simple synchronous HTTP server that doesn't
//! conflict with the existing tokio 0.1 runtime used by the Memcached
//! TCP server.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use tiny_http::{Header, Method, Response, Server, StatusCode};

use crate::http::handlers::{ErrorResponse, KeyDetailResponse, KeysResponse};
use crate::slice::Slice;
use crate::store::Store;
use crate::utils::to_str;

/// Maximum value size to return in full (1MB). Larger values are truncated.
const MAX_VALUE_SIZE: usize = 1024 * 1024;

/// Default HTTP port for the web interface
pub const DEFAULT_HTTP_PORT: u16 = 8080;

/// Start the HTTP server in a background thread
pub fn start_http_server(port: u16, store: Arc<Store>) {
    thread::spawn(move || {
        run_http_server(port, store);
    });
}

/// Run the HTTP server (blocking)
pub fn run_http_server(port: u16, store: Arc<Store>) {
    let addr = format!("0.0.0.0:{}", port);
    let server = match Server::http(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to start HTTP server: {}", e);
            return;
        }
    };

    println!("HTTP server started on http://{}", addr);

    for request in server.incoming_requests() {
        let response = handle_request(&request, &store);
        let _ = request.respond(response);
    }
}

fn handle_request(
    request: &tiny_http::Request,
    store: &Arc<Store>,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let url = request.url();
    let method = request.method();

    // Parse path and query string
    let (path, query_string) = match url.find('?') {
        Some(pos) => (&url[..pos], Some(&url[pos + 1..])),
        None => (url, None),
    };

    // Only handle GET requests
    if *method != Method::Get {
        return Response::from_string("Method Not Allowed")
            .with_status_code(StatusCode(405))
            .with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
            );
    }

    match path {
        "/" | "/index.html" => serve_index(),
        "/static/css/style.css" => serve_css(),
        "/static/js/app.js" => serve_js(),
        "/api/stats" => serve_api_stats(store),
        "/api/keys" => serve_api_keys(store, query_string),
        "/api/compaction" => serve_api_compaction(),
        _ if path.starts_with("/api/keys/") => {
            let encoded_key = &path[11..]; // Extract key from /api/keys/{key}
            serve_api_key_detail(encoded_key, store)
        }
        _ => Response::from_string("Not Found")
            .with_status_code(StatusCode(404))
            .with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
            ),
    }
}

fn serve_index() -> Response<std::io::Cursor<Vec<u8>>> {
    let html = include_str!("../../static/index.html");
    Response::from_string(html).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap(),
    )
}

fn serve_css() -> Response<std::io::Cursor<Vec<u8>>> {
    let css = include_str!("../../static/css/style.css");
    Response::from_string(css).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"text/css; charset=utf-8"[..]).unwrap(),
    )
}

fn serve_js() -> Response<std::io::Cursor<Vec<u8>>> {
    let js = include_str!("../../static/js/app.js");
    Response::from_string(js).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/javascript; charset=utf-8"[..])
            .unwrap(),
    )
}

fn serve_api_stats(store: &Arc<Store>) -> Response<std::io::Cursor<Vec<u8>>> {
    // Get total key count from store
    let total_keys = store.count_keys().unwrap_or(0);
    let json = format!(
        r#"{{"total_keys":{},"version":"0.1.0","uptime_seconds":0}}"#,
        total_keys
    );
    Response::from_string(json).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
    )
}

/// Parse query string into a HashMap (Scenario 3: Key Browser with Pagination)
fn parse_query_string(query: Option<&str>) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some(pos) = pair.find('=') {
                let key = &pair[..pos];
                let value = &pair[pos + 1..];
                params.insert(key.to_string(), value.to_string());
            }
        }
    }
    params
}

/// Serve keys list endpoint: GET /api/keys (Scenario 3: Key Browser with Pagination)
/// Supports pagination via offset and limit query parameters
fn serve_api_keys(store: &Arc<Store>, query: Option<&str>) -> Response<std::io::Cursor<Vec<u8>>> {
    let params = parse_query_string(query);

    // Parse pagination parameters with defaults
    let offset: usize = params
        .get("offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .min(100); // Cap limit at 100 for safety

    // Get keys from store with pagination
    match store.list_keys(offset, limit) {
        Ok((keys, total)) => {
            // Convert Slice keys to strings
            let key_strings: Vec<String> = keys
                .iter()
                .map(|k| to_str(k).to_string())
                .collect();

            let response = KeysResponse {
                keys: key_strings,
                total: total as u64,
                offset: offset as u64,
                limit: limit as u64,
            };

            match serde_json::to_string(&response) {
                Ok(json) => Response::from_string(json).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                ),
                Err(_) => Response::from_string(r#"{"error":"Serialization error"}"#)
                    .with_status_code(StatusCode(500))
                    .with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    ),
            }
        }
        Err(e) => {
            let error_json = format!(r#"{{"error":"{}"}}"#, e);
            Response::from_string(error_json)
                .with_status_code(StatusCode(500))
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                )
        }
    }
}

fn serve_api_compaction() -> Response<std::io::Cursor<Vec<u8>>> {
    let json = r#"{"status":"idle","progress":0}"#;
    Response::from_string(json).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
    )
}

/// URL-decode a percent-encoded string (Scenario 5: Key Detail View)
fn url_decode(encoded: &str) -> Result<String, std::string::FromUtf8Error> {
    let mut result = Vec::with_capacity(encoded.len());
    let mut chars = encoded.bytes().peekable();

    while let Some(b) = chars.next() {
        if b == b'%' {
            // Try to decode hex pair
            let high = chars.next();
            let low = chars.next();
            if let (Some(h), Some(l)) = (high, low) {
                let hex_str = format!("{}{}", h as char, l as char);
                if let Ok(decoded) = u8::from_str_radix(&hex_str, 16) {
                    result.push(decoded);
                    continue;
                }
            }
            // If decoding fails, keep the original characters
            result.push(b);
        } else if b == b'+' {
            // '+' is encoded space in URL queries
            result.push(b' ');
        } else {
            result.push(b);
        }
    }

    String::from_utf8(result)
}

/// Serve key detail endpoint: GET /api/keys/{key} (Scenario 5: Key Detail View)
/// Returns JSON with key, value, size, and flags
/// Returns 404 if key not found
/// Truncates values larger than MAX_VALUE_SIZE (1MB)
fn serve_api_key_detail(
    encoded_key: &str,
    store: &Arc<Store>,
) -> Response<std::io::Cursor<Vec<u8>>> {
    // URL-decode the key name to handle special characters
    let key_name = match url_decode(encoded_key) {
        Ok(k) => k,
        Err(_) => {
            let error = ErrorResponse { error: "Invalid key encoding".to_string() };
            let json = serde_json::to_string(&error).unwrap_or_else(|_| {
                r#"{"error":"Invalid key encoding"}"#.to_string()
            });
            return Response::from_string(json)
                .with_status_code(StatusCode(400))
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
        }
    };

    // Skip empty keys
    if key_name.is_empty() {
        let error = ErrorResponse { error: "Key cannot be empty".to_string() };
        let json = serde_json::to_string(&error).unwrap_or_else(|_| {
            r#"{"error":"Key cannot be empty"}"#.to_string()
        });
        return Response::from_string(json)
            .with_status_code(StatusCode(400))
            .with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
    }

    // Create a Slice from the key name for lookup
    let key_slice = Slice::from(key_name.as_str());

    // Look up the key in the store using the Request/Response API
    use crate::request::{GetterType, Request};
    use crate::response::Response as StoreResponse;

    let request = Request::Getter {
        getter: GetterType::Get,
        keys: vec![key_slice.clone()],
    };

    match store.apply(request) {
        Ok(StoreResponse::Get(items)) => {
            if items.is_empty() {
                // Key not found
                let error = ErrorResponse { error: "Key not found".to_string() };
                let json = serde_json::to_string(&error).unwrap_or_else(|_| {
                    r#"{"error":"Key not found"}"#.to_string()
                });
                return Response::from_string(json)
                    .with_status_code(StatusCode(404))
                    .with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    );
            }

            // Key found - extract details
            let item = &items[0];
            let value_bytes: &[u8] = item.data.as_ref();
            let value_size = value_bytes.len() as u64;

            // Truncate large values to prevent memory exhaustion (Risk Management)
            let value_str = if value_bytes.len() > MAX_VALUE_SIZE {
                let truncated = &value_bytes[..MAX_VALUE_SIZE];
                // Try to convert to UTF-8, fallback to lossy conversion
                let value = String::from_utf8_lossy(truncated).to_string();
                format!("{}... [truncated, showing first 1MB of {} bytes]", value, value_size)
            } else {
                // Try to convert to UTF-8, fallback to lossy conversion
                String::from_utf8_lossy(value_bytes).to_string()
            };

            let response = KeyDetailResponse {
                key: key_name,
                value: value_str,
                size: value_size,
                flags: item.flags,
            };

            let json = serde_json::to_string(&response).unwrap_or_else(|_| {
                r#"{"error":"Failed to serialize response"}"#.to_string()
            });

            Response::from_string(json).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            )
        }
        Ok(_) => {
            // Unexpected response type
            let error = ErrorResponse { error: "Unexpected response from store".to_string() };
            let json = serde_json::to_string(&error).unwrap_or_else(|_| {
                r#"{"error":"Unexpected response from store"}"#.to_string()
            });
            Response::from_string(json)
                .with_status_code(StatusCode(500))
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                )
        }
        Err(e) => {
            // Store error
            let error = ErrorResponse { error: format!("Store error: {}", e) };
            let json = serde_json::to_string(&error).unwrap_or_else(|_| {
                r#"{"error":"Store error"}"#.to_string()
            });
            Response::from_string(json)
                .with_status_code(StatusCode(500))
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        assert_eq!(DEFAULT_HTTP_PORT, 8080);
    }

    // =========================================================================
    // Scenario 5: Key Detail View Tests
    // =========================================================================

    #[test]
    fn test_url_decode_simple() {
        assert_eq!(url_decode("hello").unwrap(), "hello");
        assert_eq!(url_decode("test_key").unwrap(), "test_key");
    }

    #[test]
    fn test_url_decode_spaces() {
        assert_eq!(url_decode("hello%20world").unwrap(), "hello world");
        assert_eq!(url_decode("hello+world").unwrap(), "hello world");
    }

    #[test]
    fn test_url_decode_special_chars() {
        assert_eq!(url_decode("key%2Fwith%2Fslashes").unwrap(), "key/with/slashes");
        assert_eq!(url_decode("key%3Awith%3Acolons").unwrap(), "key:with:colons");
        assert_eq!(url_decode("key%3Dvalue").unwrap(), "key=value");
        assert_eq!(url_decode("key%26value").unwrap(), "key&value");
        assert_eq!(url_decode("key%3Fquery").unwrap(), "key?query");
    }

    #[test]
    fn test_url_decode_unicode() {
        // UTF-8 encoded characters
        assert_eq!(url_decode("%E4%B8%AD%E6%96%87").unwrap(), "中文");
        assert_eq!(url_decode("caf%C3%A9").unwrap(), "café");
    }

    #[test]
    fn test_url_decode_mixed() {
        assert_eq!(url_decode("hello%20%E4%B8%96%E7%95%8C").unwrap(), "hello 世界");
    }

    #[test]
    fn test_max_value_size() {
        assert_eq!(MAX_VALUE_SIZE, 1024 * 1024);
    }

    // =========================================================================
    // Scenario 3: Key Browser with Pagination Tests
    // =========================================================================

    #[test]
    fn test_parse_query_string_empty() {
        let params = parse_query_string(None);
        assert!(params.is_empty());
    }

    #[test]
    fn test_parse_query_string_single_param() {
        let params = parse_query_string(Some("offset=10"));
        assert_eq!(params.get("offset"), Some(&"10".to_string()));
    }

    #[test]
    fn test_parse_query_string_multiple_params() {
        let params = parse_query_string(Some("offset=10&limit=20"));
        assert_eq!(params.get("offset"), Some(&"10".to_string()));
        assert_eq!(params.get("limit"), Some(&"20".to_string()));
    }

    #[test]
    fn test_parse_query_string_with_search() {
        let params = parse_query_string(Some("offset=0&limit=20&search=test"));
        assert_eq!(params.get("offset"), Some(&"0".to_string()));
        assert_eq!(params.get("limit"), Some(&"20".to_string()));
        assert_eq!(params.get("search"), Some(&"test".to_string()));
    }
}
