//! Statistics caching with TTL.
//! Owner: Scenario 2 - Database Statistics Display
//!
//! Purpose: Cache expensive statistics calculations to avoid
//! impacting Memcached protocol performance (NFR-2).
//!
//! Expected exports:
//! - `StatsCache` struct with 5-second TTL (configurable)
//! - `StatsCache::get_or_compute(&self, store: &Store) -> Stats`
//!   Returns cached stats or computes fresh if expired
//!
//! Stats fields:
//! - total_keys: u64
//! - memory_usage: u64 (bytes)
//! - storage_size: u64 (bytes)
//! - version: String
//! - uptime: Duration
//! - last_updated: SystemTime

use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;

/// Cached statistics data
#[derive(Clone, Serialize)]
pub struct Stats {
    pub total_keys: u64,
    pub memory_usage: u64,
    pub storage_size: u64,
    pub version: String,
    pub uptime_seconds: u64,
    pub last_updated: u64, // Unix timestamp
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            total_keys: 0,
            memory_usage: 0,
            storage_size: 0,
            version: "0.1.0".to_string(),
            uptime_seconds: 0,
            last_updated: 0,
        }
    }
}

/// Statistics cache with TTL
pub struct StatsCache {
    cache: RwLock<Option<(Stats, Instant)>>,
    ttl: Duration,
}

impl StatsCache {
    /// Create a new stats cache with the given TTL
    pub fn new(ttl_seconds: u64) -> Self {
        StatsCache {
            cache: RwLock::new(None),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get cached stats or compute fresh ones
    pub fn get_or_compute<F>(&self, compute: F) -> Stats
    where
        F: FnOnce() -> Stats,
    {
        // Check if cache is valid
        {
            let cache = self.cache.read().unwrap();
            if let Some((stats, cached_at)) = cache.as_ref() {
                if cached_at.elapsed() < self.ttl {
                    return stats.clone();
                }
            }
        }

        // Cache miss or expired, compute new stats
        let stats = compute();

        // Update cache
        {
            let mut cache = self.cache.write().unwrap();
            *cache = Some((stats.clone(), Instant::now()));
        }

        stats
    }
}

impl Default for StatsCache {
    fn default() -> Self {
        StatsCache::new(5) // 5 second TTL by default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_cache_returns_computed_value() {
        let cache = StatsCache::new(5);
        let stats = cache.get_or_compute(|| Stats {
            total_keys: 42,
            ..Default::default()
        });
        assert_eq!(stats.total_keys, 42);
    }

    #[test]
    fn test_cache_returns_cached_value() {
        let cache = StatsCache::new(5);
        let mut call_count = 0;

        let _ = cache.get_or_compute(|| {
            call_count += 1;
            Stats {
                total_keys: call_count,
                ..Default::default()
            }
        });

        let stats = cache.get_or_compute(|| {
            call_count += 1;
            Stats {
                total_keys: call_count,
                ..Default::default()
            }
        });

        // Should return cached value (1), not recompute (2)
        assert_eq!(stats.total_keys, 1);
    }
}
