//! HTTP Request Router
//! Owner: First builder (shared component)
//!
//! Expected exports:
//! - route(req: Request) -> Response
//! - Route matching for static files and API endpoints

/// HTTP method enum
#[derive(Debug, Clone, PartialEq)]
pub enum Method {
    Get,
    Post,
    Delete,
}

/// Route matching result
#[derive(Debug)]
pub enum Route {
    /// Serve static file at path
    Static(String),
    /// API: Get server status
    ApiStatus,
    /// API: List keys
    ApiKeysList,
    /// API: Get key by name
    ApiKeysGet(String),
    /// API: Set key
    ApiKeysSet,
    /// API: Delete key
    ApiKeysDelete(String),
    /// API: Get configuration
    ApiConfig,
    /// Not found
    NotFound,
}

/// Match request path to route
pub fn match_route(method: &Method, path: &str) -> Route {
    match (method, path) {
        (Method::Get, "/") => Route::Static("index.html".to_string()),
        (Method::Get, "/api/status") => Route::ApiStatus,
        (Method::Get, "/api/keys") => Route::ApiKeysList,
        (Method::Get, "/api/config") => Route::ApiConfig,
        (Method::Post, "/api/keys") => Route::ApiKeysSet,
        (Method::Get, p) if p.starts_with("/api/keys/") => {
            let key = p.trim_start_matches("/api/keys/");
            Route::ApiKeysGet(key.to_string())
        }
        (Method::Delete, p) if p.starts_with("/api/keys/") => {
            let key = p.trim_start_matches("/api/keys/");
            Route::ApiKeysDelete(key.to_string())
        }
        (Method::Get, p) => {
            // Static file request
            let file_path = p.trim_start_matches('/');
            Route::Static(file_path.to_string())
        }
        _ => Route::NotFound,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_homepage() {
        let route = match_route(&Method::Get, "/");
        match route {
            Route::Static(path) => assert_eq!(path, "index.html"),
            _ => panic!("Expected Static route"),
        }
    }

    #[test]
    fn test_route_static_css() {
        let route = match_route(&Method::Get, "/styles/main.css");
        match route {
            Route::Static(path) => assert_eq!(path, "styles/main.css"),
            _ => panic!("Expected Static route"),
        }
    }

    #[test]
    fn test_route_api_status() {
        let route = match_route(&Method::Get, "/api/status");
        assert!(matches!(route, Route::ApiStatus));
    }
}
