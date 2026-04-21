//! Key-Value operation handlers.
//! Owner: Scenarios 5, 6, 7 - KV Operations via Web API
//!
//! Expected exports:
//! - set_kv(State<AppState>, Json<SetRequest>) -> Json<KvResponse>
//! - get_kv(State<AppState>, Query<GetParams>) -> Json<KvResponse>
//! - delete_kv(State<AppState>, Query<DeleteParams>) -> Json<KvResponse>
//!
//! Request/Response types:
//! - SetRequest: { key: String, value: String }
//! - GetParams: { key: String }
//! - DeleteParams: { key: String }
//! - KvResponse: { success: bool, key: String, value?: String, error?: String, message?: String }

use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::request::Request;
use crate::response::Response;
use crate::slice::Slice;
use crate::web::routes::AppState;

/// Query parameters for DELETE operation
#[derive(Debug, Deserialize)]
pub struct DeleteParams {
    pub key: String,
}

/// Response for KV operations with optional message field for DELETE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvDeleteResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// DELETE /api/kv/delete handler
///
/// Deletes a key from the store.
/// Returns success=true with message "DELETED" if the key was found and deleted.
/// Returns success=false with error "NOT_FOUND" if the key doesn't exist.
pub async fn delete_kv(
    State(state): State<AppState>,
    Query(params): Query<DeleteParams>,
) -> Json<KvDeleteResponse> {
    let key = Slice::from(params.key.as_str());

    let request = Request::Deleter {
        key,
        no_reply: false,
    };

    match state.store.apply(request) {
        Ok(Response::Deleted) => Json(KvDeleteResponse {
            success: true,
            message: Some("DELETED".to_string()),
            error: None,
        }),
        Ok(Response::NotFound) => Json(KvDeleteResponse {
            success: false,
            message: None,
            error: Some("NOT_FOUND".to_string()),
        }),
        Ok(_) => Json(KvDeleteResponse {
            success: false,
            message: None,
            error: Some("UNEXPECTED_RESPONSE".to_string()),
        }),
        Err(e) => Json(KvDeleteResponse {
            success: false,
            message: None,
            error: Some(format!("INTERNAL_ERROR: {}", e)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_delete_response_serialization() {
        let response = KvDeleteResponse {
            success: true,
            message: Some("DELETED".to_string()),
            error: None,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"message\":\"DELETED\""));
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_kv_delete_not_found_response_serialization() {
        let response = KvDeleteResponse {
            success: false,
            message: None,
            error: Some("NOT_FOUND".to_string()),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":false"));
        assert!(json.contains("\"error\":\"NOT_FOUND\""));
        assert!(!json.contains("message"));
    }
}
