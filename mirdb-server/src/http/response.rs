use std::collections::HashMap;

pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Length".to_string(), body.len().to_string());
        HttpResponse {
            status,
            headers,
            body,
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
}

pub fn ok_html(body: &str) -> HttpResponse {
    HttpResponse::new(200, body.as_bytes().to_vec())
        .with_header("Content-Type", "text/html; charset=utf-8")
}

pub fn ok_json(body: &str) -> HttpResponse {
    HttpResponse::new(200, body.as_bytes().to_vec())
        .with_header("Content-Type", "application/json")
}

pub fn not_found_html(body: &str) -> HttpResponse {
    HttpResponse::new(404, body.as_bytes().to_vec())
        .with_header("Content-Type", "text/html; charset=utf-8")
}

pub fn not_modified() -> HttpResponse {
    HttpResponse::new(304, Vec::new())
}

pub fn with_cache_control(response: HttpResponse, max_age: u32) -> HttpResponse {
    response.with_header("Cache-Control", &format!("max-age={}", max_age))
}

pub fn with_etag(response: HttpResponse, etag: &str) -> HttpResponse {
    response.with_header("ETag", etag)
}
