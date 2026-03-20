//! Demo API Handlers
//! Owner: Scenario 3 - Interactive Demo Functionality
//!
//! Functions:
//! - execute_command(req: ExecuteRequest) -> ExecuteResponse
//!
//! Request format: { "command": "SET key 0 0 5", "value": "hello" }
//! Response format: { "output": "STORED", "success": true }

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::{AppState, StoredValue};
use crate::sanitizer;

/// Request body for execute endpoint
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    pub command: String,
    #[serde(default)]
    pub value: Option<String>,
}

/// Response body for execute endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteResponse {
    pub output: String,
    pub success: bool,
}

/// Health check endpoint
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "mirdb-demo-backend"
    }))
}

/// Execute a Memcached command
pub async fn execute_command(
    body: web::Json<ExecuteRequest>,
    state: web::Data<AppState>,
) -> impl Responder {
    let command = body.command.trim();

    // Sanitize input
    if let Err(e) = sanitizer::sanitize_command(command) {
        return HttpResponse::Ok().json(ExecuteResponse {
            output: format!("ERROR: {}", e),
            success: false,
        });
    }

    let parts: Vec<&str> = command.split_whitespace().collect();
    if parts.is_empty() {
        return HttpResponse::Ok().json(ExecuteResponse {
            output: "ERROR: Empty command".to_string(),
            success: false,
        });
    }

    let cmd = parts[0].to_uppercase();
    let result = match cmd.as_str() {
        "SET" => handle_set(&parts, body.value.as_deref(), &state),
        "GET" => handle_get(&parts, &state),
        "DELETE" => handle_delete(&parts, &state),
        "STATS" => handle_stats(&state),
        "VERSION" => ExecuteResponse {
            output: "VERSION MirDB-Demo 1.0.0".to_string(),
            success: true,
        },
        "QUIT" => ExecuteResponse {
            output: "Connection closed.".to_string(),
            success: true,
        },
        _ => ExecuteResponse {
            output: format!("ERROR: Unknown command '{}'", cmd),
            success: false,
        },
    };

    HttpResponse::Ok().json(result)
}

/// Handle SET command
/// SET <key> <flags> <exptime> <bytes> [noreply]
fn handle_set(parts: &[&str], value: Option<&str>, state: &web::Data<AppState>) -> ExecuteResponse {
    if parts.len() < 5 {
        return ExecuteResponse {
            output: "CLIENT_ERROR bad command line format".to_string(),
            success: false,
        };
    }

    let key = parts[1].to_string();
    let flags: u32 = match parts[2].parse() {
        Ok(f) => f,
        Err(_) => {
            return ExecuteResponse {
                output: "CLIENT_ERROR bad command line format".to_string(),
                success: false,
            }
        }
    };
    let exptime: u64 = match parts[3].parse() {
        Ok(e) => e,
        Err(_) => {
            return ExecuteResponse {
                output: "CLIENT_ERROR bad command line format".to_string(),
                success: false,
            }
        }
    };
    let bytes: usize = match parts[4].parse() {
        Ok(b) => b,
        Err(_) => {
            return ExecuteResponse {
                output: "CLIENT_ERROR bad command line format".to_string(),
                success: false,
            }
        }
    };

    // Use provided value or generate a placeholder
    let stored_value = value.unwrap_or(&format!("data_{}", key)).to_string();

    let mut store = state.store.lock().unwrap();
    store.insert(
        key,
        StoredValue {
            value: stored_value,
            flags,
            exptime,
            bytes,
        },
    );

    ExecuteResponse {
        output: "STORED".to_string(),
        success: true,
    }
}

/// Handle GET command
/// GET <key>*
fn handle_get(parts: &[&str], state: &web::Data<AppState>) -> ExecuteResponse {
    if parts.len() < 2 {
        return ExecuteResponse {
            output: "CLIENT_ERROR missing key".to_string(),
            success: false,
        };
    }

    let key = parts[1];
    let store = state.store.lock().unwrap();

    match store.get(key) {
        Some(item) => {
            let output = format!(
                "VALUE {} {} {}\r\n{}\r\nEND",
                key, item.flags, item.bytes, item.value
            );
            ExecuteResponse {
                output,
                success: true,
            }
        }
        None => ExecuteResponse {
            output: "END".to_string(),
            success: true,
        },
    }
}

/// Handle DELETE command
/// DELETE <key> [noreply]
fn handle_delete(parts: &[&str], state: &web::Data<AppState>) -> ExecuteResponse {
    if parts.len() < 2 {
        return ExecuteResponse {
            output: "CLIENT_ERROR missing key".to_string(),
            success: false,
        };
    }

    let key = parts[1];
    let mut store = state.store.lock().unwrap();

    if store.remove(key).is_some() {
        ExecuteResponse {
            output: "DELETED".to_string(),
            success: true,
        }
    } else {
        ExecuteResponse {
            output: "NOT_FOUND".to_string(),
            success: true,
        }
    }
}

/// Handle STATS command
fn handle_stats(state: &web::Data<AppState>) -> ExecuteResponse {
    let store = state.store.lock().unwrap();
    let stats = format!(
        "STAT pid 1\r\nSTAT uptime 300\r\nSTAT version MirDB-Demo 1.0.0\r\nSTAT curr_items {}\r\nSTAT total_items {}\r\nEND",
        store.len(),
        store.len()
    );
    ExecuteResponse {
        output: stats,
        success: true,
    }
}
