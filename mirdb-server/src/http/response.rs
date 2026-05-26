/**
 * HTTP response builder utilities.
 *
 * This file is created by the first scenario builder.
 *
 * Expected exports:
 * - struct HttpResponse { status: u16, headers: HashMap<String, String>, body: Vec<u8> }
 * - fn ok_html(body: &str) -> HttpResponse
 * - fn ok_json(body: &str) -> HttpResponse
 * - fn not_found_html(body: &str) -> HttpResponse
 * - fn not_modified() -> HttpResponse
 * - fn with_cache_control(response: HttpResponse, max_age: u32) -> HttpResponse
 * - fn with_etag(response: HttpResponse, etag: &str) -> HttpResponse
 */

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

pub fn ok_html(body: &str) -> HttpResponse {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "text/html; charset=utf-8".to_string());
    HttpResponse {
        status: 200,
        headers,
        body: body.as_bytes().to_vec(),
    }
}

pub fn ok_json(body: &str) -> HttpResponse {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    HttpResponse {
        status: 200,
        headers,
        body: body.as_bytes().to_vec(),
    }
}

pub fn not_found_html(body: &str) -> HttpResponse {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "text/html; charset=utf-8".to_string());
    HttpResponse {
        status: 404,
        headers,
        body: body.as_bytes().to_vec(),
    }
}

pub fn not_modified() -> HttpResponse {
    HttpResponse {
        status: 304,
        headers: HashMap::new(),
        body: Vec::new(),
    }
}

pub fn with_cache_control(mut response: HttpResponse, max_age: u32) -> HttpResponse {
    response.headers.insert(
        "Cache-Control".to_string(),
        format!("max-age={}", max_age),
    );
    response
}

pub fn with_etag(mut response: HttpResponse, etag: &str) -> HttpResponse {
    response.headers.insert("ETag".to_string(), etag.to_string());
    response
}
