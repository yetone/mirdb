//! Rate Limiting Module
//! Owner: Scenario 15 - Demo Error Handling
//!
//! Functions:
//! - check_rate_limit(client_id: &str) -> Result<(), RateLimitError>
//! - record_request(client_id: &str)
//!
//! Requirements:
//! - Minimum 10 commands per session (Success Criteria)
//! - Handle 50 concurrent users (NFR-4)
//! - Return appropriate error when limit exceeded

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Rate limit configuration
const MAX_REQUESTS_PER_MINUTE: u32 = 60;
const WINDOW_DURATION: Duration = Duration::from_secs(60);

/// Rate limiter state
pub struct RateLimiter {
    requests: Mutex<HashMap<String, Vec<Instant>>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            requests: Mutex::new(HashMap::new()),
        }
    }

    /// Check if a client has exceeded rate limits
    pub fn check_rate_limit(&self, client_id: &str) -> Result<(), String> {
        let mut requests = self.requests.lock().unwrap();
        let now = Instant::now();

        let client_requests = requests.entry(client_id.to_string()).or_insert_with(Vec::new);

        // Remove old requests outside the window
        client_requests.retain(|&req_time| now.duration_since(req_time) < WINDOW_DURATION);

        // Check if rate limit exceeded
        if client_requests.len() >= MAX_REQUESTS_PER_MINUTE as usize {
            return Err("Rate limit exceeded. Please wait before sending more requests.".to_string());
        }

        Ok(())
    }

    /// Record a request for a client
    pub fn record_request(&self, client_id: &str) {
        let mut requests = self.requests.lock().unwrap();
        let client_requests = requests.entry(client_id.to_string()).or_insert_with(Vec::new);
        client_requests.push(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_allows_requests() {
        let limiter = RateLimiter::new();
        let client_id = "test_client";

        // Should allow 10+ requests (success criteria)
        for _ in 0..10 {
            assert!(limiter.check_rate_limit(client_id).is_ok());
            limiter.record_request(client_id);
        }
    }

    #[test]
    fn test_rate_limit_blocks_excessive_requests() {
        let limiter = RateLimiter::new();
        let client_id = "test_client";

        // Record max requests
        for _ in 0..MAX_REQUESTS_PER_MINUTE {
            limiter.record_request(client_id);
        }

        // Next request should be blocked
        assert!(limiter.check_rate_limit(client_id).is_err());
    }

    #[test]
    fn test_different_clients_independent() {
        let limiter = RateLimiter::new();

        // Each client has independent limits
        for i in 0..50 {
            let client_id = format!("client_{}", i);
            assert!(limiter.check_rate_limit(&client_id).is_ok());
            limiter.record_request(&client_id);
        }
    }
}
