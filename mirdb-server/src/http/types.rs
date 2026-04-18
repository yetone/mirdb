//! JSON request and response types
//!
//! Owner: Scenario 10 (JSON API Response Format)
//!
//! Expected types:
//! - `StatusResponse` - Server status info (levels, memory, compaction)
//! - `KeyResponse` - Key value with metadata (value, flags, ttl, bytes)
//! - `SetKeyRequest` - Key set request body (key, value, flags?, ttl?)
//! - `SuccessResponse` - Generic success response {success: bool}
//! - `ErrorResponse` - Error response with message
//!
//! All types should derive Serialize/Deserialize

use serde::{Deserialize, Serialize};

/// Request to set a key-value pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetKeyRequest {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub flags: u32,
    #[serde(default)]
    pub ttl: u32,
}

/// Response for a key lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyResponse {
    pub value: String,
    pub flags: u32,
    pub ttl: u32,
    pub bytes: usize,
}

/// Generic success response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessResponse {
    pub success: bool,
}

/// Error response with message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Server status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub levels: Vec<LevelInfo>,
    pub memory: MemoryInfo,
    pub compaction: String,
}

/// Level information for status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelInfo {
    pub level: usize,
    pub files: usize,
}

/// Memory information for status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    pub used_bytes: usize,
    pub percentage: f64,
}

impl SuccessResponse {
    pub fn new(success: bool) -> Self {
        Self { success }
    }
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self { error: error.into() }
    }
}
