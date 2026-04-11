//! Key Operations API Handler
//! Owner: Scenario 4 - Key Browser View Keys
//! Co-owners: Scenarios 5, 6, 7 (Set, Get, Delete)
//!
//! Expected exports:
//! - handle_list_keys(store: Arc<Store>, page: u32, limit: u32) -> Response
//! - handle_get_key(store: Arc<Store>, key: String) -> Response
//! - handle_set_key(store: Arc<Store>, body: SetKeyRequest) -> Response
//! - handle_delete_key(store: Arc<Store>, key: String) -> Response

use serde::{Deserialize, Serialize};

/// Request to set a key
#[derive(Debug, Deserialize)]
pub struct SetKeyRequest {
    pub key: String,
    pub value: String,
    pub flags: u32,
    pub ttl: u32,
}

/// Response for key listing
#[derive(Debug, Serialize)]
pub struct KeysListResponse {
    pub keys: Vec<KeyInfo>,
    pub page: u32,
    pub limit: u32,
    pub total: u32,
}

/// Key information
#[derive(Debug, Serialize)]
pub struct KeyInfo {
    pub key: String,
    pub size: u64,
    pub ttl: u32,
}

/// Single key value response
#[derive(Debug, Serialize)]
pub struct KeyValueResponse {
    pub key: String,
    pub value: String,
    pub flags: u32,
    pub ttl: u32,
}
