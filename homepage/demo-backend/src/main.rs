//! Demo Backend Server
//! Owner: Scenario 3 - Interactive Demo Functionality
//!
//! Entry point for the demo API server.
//! Initializes HTTP server with routes:
//! - POST /api/demo/execute - Execute Memcached command
//!
//! Uses modules:
//! - handler: Request handlers
//! - sanitizer: Input sanitization (Scenario 14)
//! - rate_limiter: Request rate limiting (Scenario 15)

mod handler;
mod sanitizer;
mod rate_limiter;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer, middleware::Logger};
use std::collections::HashMap;
use std::sync::Mutex;

/// Application state shared across handlers
pub struct AppState {
    /// In-memory key-value store for demo
    pub store: Mutex<HashMap<String, StoredValue>>,
}

/// Value stored in the demo store
#[derive(Clone, Debug)]
pub struct StoredValue {
    pub value: String,
    pub flags: u32,
    pub exptime: u64,
    pub bytes: usize,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let app_state = web::Data::new(AppState::new());
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".to_string());

    log::info!("Starting demo backend server at {}", bind_addr);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .app_data(app_state.clone())
            .wrap(cors)
            .wrap(Logger::default())
            .route("/api/demo/execute", web::post().to(handler::execute_command))
            .route("/health", web::get().to(handler::health_check))
    })
    .bind(&bind_addr)?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App};
    use serde_json::json;

    #[actix_rt::test]
    async fn test_health_check() {
        let app_state = web::Data::new(AppState::new());
        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .route("/health", web::get().to(handler::health_check)),
        )
        .await;

        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_set_command() {
        let app_state = web::Data::new(AppState::new());
        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .route("/api/demo/execute", web::post().to(handler::execute_command)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/api/demo/execute")
            .set_json(json!({"command": "SET mykey 0 0 5", "value": "hello"}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: handler::ExecuteResponse = test::read_body_json(resp).await;
        assert_eq!(body.output, "STORED");
        assert!(body.success);
    }

    #[actix_rt::test]
    async fn test_get_command() {
        let app_state = web::Data::new(AppState::new());

        // Pre-populate store
        {
            let mut store = app_state.store.lock().unwrap();
            store.insert(
                "testkey".to_string(),
                StoredValue {
                    value: "testvalue".to_string(),
                    flags: 0,
                    exptime: 0,
                    bytes: 9,
                },
            );
        }

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .route("/api/demo/execute", web::post().to(handler::execute_command)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/api/demo/execute")
            .set_json(json!({"command": "GET testkey"}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: handler::ExecuteResponse = test::read_body_json(resp).await;
        assert!(body.output.contains("VALUE testkey"));
        assert!(body.output.contains("testvalue"));
        assert!(body.success);
    }

    #[actix_rt::test]
    async fn test_delete_command() {
        let app_state = web::Data::new(AppState::new());

        // Pre-populate store
        {
            let mut store = app_state.store.lock().unwrap();
            store.insert(
                "deletekey".to_string(),
                StoredValue {
                    value: "value".to_string(),
                    flags: 0,
                    exptime: 0,
                    bytes: 5,
                },
            );
        }

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .route("/api/demo/execute", web::post().to(handler::execute_command)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/api/demo/execute")
            .set_json(json!({"command": "DELETE deletekey"}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: handler::ExecuteResponse = test::read_body_json(resp).await;
        assert_eq!(body.output, "DELETED");
        assert!(body.success);
    }
}
