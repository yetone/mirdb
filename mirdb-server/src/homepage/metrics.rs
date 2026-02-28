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

    // ============================================================================
    // Scenario 10 - Metrics Caching Behavior Tests
    // ============================================================================

    /// Test Case 1: Verify that metrics are cached within 1 second
    ///
    /// Input: Request metrics, add 10 keys, immediately request metrics again
    /// Expected: Second request may return cached value (same key count)
    #[test]
    fn test_scenario10_cache_returns_stale_value_within_cache_duration() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = MetricsCache::new(counters.clone());

        // Set initial key count
        counters.set_total_keys(0);

        // First request to populate cache
        let initial_metrics = cache.get();
        assert_eq!(initial_metrics.total_keys, 0, "Initial key count should be 0");

        // Add 10 keys to simulate state change
        counters.increment_keys(10);
        assert_eq!(
            counters.get_total_keys(),
            10,
            "Counter should show 10 keys after increment"
        );

        // Immediately request metrics again (within 1 second)
        let cached_metrics = cache.get();

        // The cached value should still show 0 keys because we're within cache duration
        assert_eq!(
            cached_metrics.total_keys, 0,
            "Second request within cache duration should return cached value (0 keys)"
        );
    }

    /// Test Case 2: Verify that cache expires after 1 second
    ///
    /// Input: Request metrics, add 10 keys, wait 1.5 seconds, request metrics
    /// Expected: Response reflects updated key count after cache expiration
    #[test]
    fn test_scenario10_cache_updates_after_expiration() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = MetricsCache::new(counters.clone());

        // Set initial key count
        counters.set_total_keys(0);

        // First request to populate cache
        let initial_metrics = cache.get();
        assert_eq!(initial_metrics.total_keys, 0, "Initial key count should be 0");

        // Add 10 keys to simulate state change
        counters.increment_keys(10);

        // Wait for cache to expire (1.5 seconds > 1 second cache duration)
        thread::sleep(Duration::from_millis(1500));

        // Request metrics after cache expiration
        let updated_metrics = cache.get();

        // Now we should see the updated value
        assert_eq!(
            updated_metrics.total_keys, 10,
            "After cache expiration, metrics should reflect updated key count (10)"
        );
    }

    /// Test Case 3: Verify minimal read lock contention with rapid requests
    ///
    /// Input: Make 100 rapid metrics requests within 1 second
    /// Expected: All requests return same cached value, minimal read lock contention
    #[test]
    fn test_scenario10_rapid_requests_return_same_cached_value() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = Arc::new(MetricsCache::new(counters.clone()));

        // Set initial values
        counters.set_total_keys(42);
        counters.increment_connections();
        counters.set_memtable_size(1024);

        // First request to populate cache
        let initial_metrics = cache.get();
        let initial_key_count = initial_metrics.total_keys;

        // Make 100 rapid requests
        let mut all_same = true;
        let mut request_count = 0;

        for _ in 0..100 {
            let metrics = cache.get();
            request_count += 1;

            // All should return the same cached value
            if metrics.total_keys != initial_key_count {
                all_same = false;
                break;
            }
        }

        assert_eq!(request_count, 100, "Should have made 100 requests");
        assert!(
            all_same,
            "All 100 rapid requests should return the same cached value"
        );
    }

    /// Additional test: Verify concurrent access to cache is thread-safe
    #[test]
    fn test_scenario10_concurrent_cache_access() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = Arc::new(MetricsCache::new(counters.clone()));

        // Set initial values
        counters.set_total_keys(100);

        // Populate cache
        let _ = cache.get();

        // Spawn multiple threads to read from cache concurrently
        let mut handles = vec![];

        for _ in 0..10 {
            let cache_clone = cache.clone();
            let handle = thread::spawn(move || {
                let mut results = vec![];
                for _ in 0..10 {
                    let metrics = cache_clone.get();
                    results.push(metrics.total_keys);
                }
                results
            });
            handles.push(handle);
        }

        // Collect all results
        let mut all_results = vec![];
        for handle in handles {
            all_results.extend(handle.join().unwrap());
        }

        // All results should be 100 (the cached value)
        assert_eq!(all_results.len(), 100);
        assert!(
            all_results.iter().all(|&v| v == 100),
            "All concurrent reads should return the same cached value"
        );
    }

    /// Test: Verify force refresh bypasses cache
    #[test]
    fn test_scenario10_force_refresh_bypasses_cache() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = MetricsCache::new(counters.clone());

        // Set initial values and populate cache
        counters.set_total_keys(25);
        let initial = cache.get();
        assert_eq!(initial.total_keys, 25);

        // Update counters
        counters.set_total_keys(75);

        // Normal get should return cached value
        let cached = cache.get();
        assert_eq!(cached.total_keys, 25, "Normal get should return cached value");

        // Force refresh should return fresh value
        let refreshed = cache.refresh();
        assert_eq!(
            refreshed.total_keys, 75,
            "Force refresh should bypass cache and return fresh value"
        );

        // Subsequent get should return the new cached value
        let subsequent = cache.get();
        assert_eq!(
            subsequent.total_keys, 75,
            "After refresh, cache should contain new value"
        );
    }

    /// Test: Verify all metric fields are properly cached
    #[test]
    fn test_scenario10_all_metrics_fields_are_cached() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = MetricsCache::new(counters.clone());

        // Set initial values
        counters.set_total_keys(100);
        counters.increment_connections();
        counters.increment_connections();
        counters.set_memtable_size(4096);

        // Populate cache
        let initial = cache.get();
        assert_eq!(initial.total_keys, 100);
        assert_eq!(initial.active_connections, 2);
        assert_eq!(initial.memtable_size, 4096);

        // Modify all counters
        counters.set_total_keys(500);
        counters.increment_connections();
        counters.set_memtable_size(8192);

        // Cache should return old values for all fields
        let cached = cache.get();
        assert_eq!(
            cached.total_keys, 100,
            "total_keys should be cached"
        );
        assert_eq!(
            cached.active_connections, 2,
            "active_connections should be cached"
        );
        assert_eq!(
            cached.memtable_size, 4096,
            "memtable_size should be cached"
        );
    }

    /// Test: Verify rapid state changes followed by cache expiration
    #[test]
    fn test_scenario10_rapid_state_changes_then_cache_expiration() {
        let counters = Arc::new(MetricsCounters::new());
        let cache = MetricsCache::new(counters.clone());

        // Initial state
        counters.set_total_keys(0);
        let _ = cache.get();

        // Rapid state changes
        for i in 1..=10 {
            counters.increment_keys(1);
            let metrics = cache.get();
            // All within cache window, should return 0
            assert_eq!(
                metrics.total_keys, 0,
                "Iteration {}: Should return cached value (0) during rapid changes",
                i
            );
        }

        // Wait for cache to expire
        thread::sleep(Duration::from_millis(1100));

        // Now should see the final value
        let final_metrics = cache.get();
        assert_eq!(
            final_metrics.total_keys, 10,
            "After cache expiration, should show final accumulated value"
        );
    }
}
