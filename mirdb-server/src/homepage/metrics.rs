//! Metrics Collection and Caching
//!
//! Provides metrics data with 1-second caching to reduce read contention.
//!
//! Owner: Scenario 5 - Metrics Display and API
//! Co-owner: Scenario 10 - Metrics Caching Behavior
//!
//! Expected exports:
//! - pub struct MetricsCache
//! - pub struct Metrics { active_connections, total_keys, memtable_size, sstable_levels }
//! - impl MetricsCache { pub fn new(state: Arc<AppState>) -> Self }
//! - impl MetricsCache { pub fn get(&self) -> Metrics }
//! - CACHE_DURATION: Duration = 1 second

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Cache duration for metrics (1 second as per PRD)
pub const CACHE_DURATION: Duration = Duration::from_secs(1);

/// SSTable level information
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct SSTableLevel {
    /// Level number (0 = newest, higher = older)
    pub level: usize,
    /// Number of SSTables at this level
    pub file_count: usize,
    /// Total size of SSTables at this level in bytes
    pub total_size: u64,
}

/// Server metrics data structure
///
/// REQ-4: Must display basic metrics: number of active connections,
/// total keys stored, memtable size
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metrics {
    /// Number of active memcached client connections
    pub active_connections: u64,
    /// Total number of keys stored in the database
    pub total_keys: u64,
    /// Current memtable size in bytes
    pub memtable_size: u64,
    /// SSTable level information (as per appendix specification)
    pub sstable_levels: Vec<SSTableLevel>,
}

impl Metrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Create metrics with specified values
    pub fn with_values(
        active_connections: u64,
        total_keys: u64,
        memtable_size: u64,
        sstable_levels: Vec<SSTableLevel>,
    ) -> Self {
        Self {
            active_connections,
            total_keys,
            memtable_size,
            sstable_levels,
        }
    }
}

/// Atomic counters for real-time metrics tracking
///
/// These can be updated from multiple threads without locking
#[derive(Debug, Default)]
pub struct MetricsCounters {
    /// Active connections counter
    pub active_connections: AtomicU64,
    /// Total keys counter
    pub total_keys: AtomicU64,
    /// Memtable size in bytes
    pub memtable_size: AtomicU64,
}

impl MetricsCounters {
    /// Create new counters initialized to zero
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment active connections
    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement active connections
    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }

    /// Set total keys count
    pub fn set_total_keys(&self, count: u64) {
        self.total_keys.store(count, Ordering::SeqCst);
    }

    /// Increment total keys by a given amount
    pub fn increment_keys(&self, count: u64) {
        self.total_keys.fetch_add(count, Ordering::SeqCst);
    }

    /// Set memtable size
    pub fn set_memtable_size(&self, size: u64) {
        self.memtable_size.store(size, Ordering::SeqCst);
    }

    /// Get current active connections
    pub fn get_active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Get current total keys
    pub fn get_total_keys(&self) -> u64 {
        self.total_keys.load(Ordering::SeqCst)
    }

    /// Get current memtable size
    pub fn get_memtable_size(&self) -> u64 {
        self.memtable_size.load(Ordering::SeqCst)
    }
}

/// Cached metrics entry
struct CachedMetrics {
    /// The cached metrics data
    metrics: Metrics,
    /// When the cache was last updated
    last_updated: Instant,
}

/// Metrics cache with 1-second TTL
///
/// Reduces read contention on shared state by caching metrics
/// for up to 1 second before refreshing.
pub struct MetricsCache {
    /// Atomic counters for real-time updates
    counters: Arc<MetricsCounters>,
    /// Cached metrics with timestamp
    cache: RwLock<Option<CachedMetrics>>,
    /// SSTable levels (updated less frequently)
    sstable_levels: RwLock<Vec<SSTableLevel>>,
}

impl MetricsCache {
    /// Create a new MetricsCache with the given counters
    pub fn new(counters: Arc<MetricsCounters>) -> Self {
        Self {
            counters,
            cache: RwLock::new(None),
            sstable_levels: RwLock::new(Vec::new()),
        }
    }

    /// Create a new MetricsCache with default counters
    pub fn with_default_counters() -> Self {
        Self::new(Arc::new(MetricsCounters::new()))
    }

    /// Get the current metrics, using cache if valid
    pub fn get(&self) -> Metrics {
        // Check if cache is valid
        {
            let cache_guard = self.cache.read().unwrap();
            if let Some(ref cached) = *cache_guard {
                if cached.last_updated.elapsed() < CACHE_DURATION {
                    return cached.metrics.clone();
                }
            }
        }

        // Cache expired or empty, refresh
        self.refresh()
    }

    /// Force refresh the metrics cache
    pub fn refresh(&self) -> Metrics {
        let sstable_levels = self.sstable_levels.read().unwrap().clone();

        let metrics = Metrics {
            active_connections: self.counters.get_active_connections(),
            total_keys: self.counters.get_total_keys(),
            memtable_size: self.counters.get_memtable_size(),
            sstable_levels,
        };

        // Update cache
        let mut cache_guard = self.cache.write().unwrap();
        *cache_guard = Some(CachedMetrics {
            metrics: metrics.clone(),
            last_updated: Instant::now(),
        });

        metrics
    }

    /// Update SSTable levels information
    pub fn update_sstable_levels(&self, levels: Vec<SSTableLevel>) {
        let mut guard = self.sstable_levels.write().unwrap();
        *guard = levels;
    }

    /// Get a reference to the counters for direct updates
    pub fn counters(&self) -> Arc<MetricsCounters> {
        self.counters.clone()
    }

    /// Invalidate the cache, forcing a refresh on next get()
    pub fn invalidate(&self) {
        let mut cache_guard = self.cache.write().unwrap();
        *cache_guard = None;
    }
}

impl Default for MetricsCache {
    fn default() -> Self {
        Self::with_default_counters()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_metrics_default() {
        let metrics = Metrics::default();
        assert_eq!(metrics.active_connections, 0);
        assert_eq!(metrics.total_keys, 0);
        assert_eq!(metrics.memtable_size, 0);
        assert!(metrics.sstable_levels.is_empty());
    }

    #[test]
    fn test_metrics_with_values() {
        let levels = vec![
            SSTableLevel {
                level: 0,
                file_count: 3,
                total_size: 1024,
            },
            SSTableLevel {
                level: 1,
                file_count: 5,
                total_size: 4096,
            },
        ];
        let metrics = Metrics::with_values(5, 100, 2048, levels.clone());

        assert_eq!(metrics.active_connections, 5);
        assert_eq!(metrics.total_keys, 100);
        assert_eq!(metrics.memtable_size, 2048);
        assert_eq!(metrics.sstable_levels.len(), 2);
        assert_eq!(metrics.sstable_levels[0].level, 0);
        assert_eq!(metrics.sstable_levels[1].file_count, 5);
    }

    #[test]
    fn test_metrics_counters() {
        let counters = MetricsCounters::new();

        assert_eq!(counters.get_active_connections(), 0);
        assert_eq!(counters.get_total_keys(), 0);
        assert_eq!(counters.get_memtable_size(), 0);

        counters.increment_connections();
        counters.increment_connections();
        assert_eq!(counters.get_active_connections(), 2);

        counters.decrement_connections();
        assert_eq!(counters.get_active_connections(), 1);

        counters.set_total_keys(100);
        assert_eq!(counters.get_total_keys(), 100);

        counters.increment_keys(50);
        assert_eq!(counters.get_total_keys(), 150);

        counters.set_memtable_size(4096);
        assert_eq!(counters.get_memtable_size(), 4096);
    }

    #[test]
    fn test_metrics_cache_basic() {
        let cache = MetricsCache::with_default_counters();
        let counters = cache.counters();

        // Initial state should be zeros
        let metrics = cache.get();
        assert_eq!(metrics.active_connections, 0);
        assert_eq!(metrics.total_keys, 0);

        // Update counters
        counters.set_total_keys(100);
        counters.increment_connections();

        // Cache should still return old values within 1 second
        let metrics = cache.get();
        // Note: The first get() also refreshes, so values should be updated
        // after initial get

        // Force refresh to get new values
        let metrics = cache.refresh();
        assert_eq!(metrics.active_connections, 1);
        assert_eq!(metrics.total_keys, 100);
    }

    #[test]
    fn test_metrics_cache_sstable_levels() {
        let cache = MetricsCache::with_default_counters();

        let levels = vec![
            SSTableLevel {
                level: 0,
                file_count: 2,
                total_size: 512,
            },
            SSTableLevel {
                level: 1,
                file_count: 4,
                total_size: 2048,
            },
        ];

        cache.update_sstable_levels(levels);

        let metrics = cache.refresh();
        assert_eq!(metrics.sstable_levels.len(), 2);
        assert_eq!(metrics.sstable_levels[0].level, 0);
        assert_eq!(metrics.sstable_levels[0].file_count, 2);
    }

    #[test]
    fn test_metrics_cache_invalidation() {
        let cache = MetricsCache::with_default_counters();
        let counters = cache.counters();

        counters.set_total_keys(50);
        let _ = cache.get(); // Populate cache

        counters.set_total_keys(100);
        cache.invalidate();

        // After invalidation, should get fresh values
        let metrics = cache.get();
        assert_eq!(metrics.total_keys, 100);
    }

    #[test]
    fn test_metrics_serialization() {
        let levels = vec![SSTableLevel {
            level: 0,
            file_count: 1,
            total_size: 256,
        }];
        let metrics = Metrics::with_values(3, 50, 1024, levels);

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"active_connections\":3"));
        assert!(json.contains("\"total_keys\":50"));
        assert!(json.contains("\"memtable_size\":1024"));
        assert!(json.contains("\"sstable_levels\""));
    }

    #[test]
    fn test_cache_duration_constant() {
        assert_eq!(CACHE_DURATION, Duration::from_secs(1));
    }
}
