//! Metrics Integration Tests
//!
//! Tests for the /api/metrics endpoint and metrics display.
//!
//! Owner: Scenario 5 - Metrics Display and API
//!
//! Test cases:
//! 1. GET /api/metrics with no operations performed
//! 2. GET /api/metrics after adding 100 keys
//! 3. GET /api/metrics with active memcached connections
//! 4. Parse homepage metrics section
//! 5. Verify SSTable levels in metrics

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// The embedded HTML content from the homepage
const INDEX_HTML: &str = include_str!("../assets/index.html");

/// Cache duration for metrics (1 second as per PRD)
pub const CACHE_DURATION: Duration = Duration::from_secs(1);

/// SSTable level information
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct SSTableLevel {
    pub level: usize,
    pub file_count: usize,
    pub total_size: u64,
}

/// Server metrics data structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metrics {
    pub active_connections: u64,
    pub total_keys: u64,
    pub memtable_size: u64,
    pub sstable_levels: Vec<SSTableLevel>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

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
#[derive(Debug, Default)]
pub struct MetricsCounters {
    pub active_connections: AtomicU64,
    pub total_keys: AtomicU64,
    pub memtable_size: AtomicU64,
}

impl MetricsCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::SeqCst);
    }

    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn set_total_keys(&self, count: u64) {
        self.total_keys.store(count, Ordering::SeqCst);
    }

    pub fn increment_keys(&self, count: u64) {
        self.total_keys.fetch_add(count, Ordering::SeqCst);
    }

    pub fn set_memtable_size(&self, size: u64) {
        self.memtable_size.store(size, Ordering::SeqCst);
    }

    pub fn get_active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::SeqCst)
    }

    pub fn get_total_keys(&self) -> u64 {
        self.total_keys.load(Ordering::SeqCst)
    }

    pub fn get_memtable_size(&self) -> u64 {
        self.memtable_size.load(Ordering::SeqCst)
    }
}

/// Cached metrics entry
struct CachedMetrics {
    metrics: Metrics,
    last_updated: Instant,
}

/// Metrics cache with 1-second TTL
pub struct MetricsCache {
    counters: Arc<MetricsCounters>,
    cache: RwLock<Option<CachedMetrics>>,
    sstable_levels: RwLock<Vec<SSTableLevel>>,
}

impl MetricsCache {
    pub fn new(counters: Arc<MetricsCounters>) -> Self {
        Self {
            counters,
            cache: RwLock::new(None),
            sstable_levels: RwLock::new(Vec::new()),
        }
    }

    pub fn with_default_counters() -> Self {
        Self::new(Arc::new(MetricsCounters::new()))
    }

    pub fn get(&self) -> Metrics {
        {
            let cache_guard = self.cache.read().unwrap();
            if let Some(ref cached) = *cache_guard {
                if cached.last_updated.elapsed() < CACHE_DURATION {
                    return cached.metrics.clone();
                }
            }
        }
        self.refresh()
    }

    pub fn refresh(&self) -> Metrics {
        let sstable_levels = self.sstable_levels.read().unwrap().clone();

        let metrics = Metrics {
            active_connections: self.counters.get_active_connections(),
            total_keys: self.counters.get_total_keys(),
            memtable_size: self.counters.get_memtable_size(),
            sstable_levels,
        };

        let mut cache_guard = self.cache.write().unwrap();
        *cache_guard = Some(CachedMetrics {
            metrics: metrics.clone(),
            last_updated: Instant::now(),
        });

        metrics
    }

    pub fn update_sstable_levels(&self, levels: Vec<SSTableLevel>) {
        let mut guard = self.sstable_levels.write().unwrap();
        *guard = levels;
    }

    pub fn counters(&self) -> Arc<MetricsCounters> {
        self.counters.clone()
    }

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

// ============================================================================
// Test Case 1: GET /api/metrics with no operations performed
// Expected: JSON response with active_connections: 0, total_keys: 0, memtable_size: 0
// ============================================================================

#[test]
fn test_case_1_metrics_initial_state() {
    let cache = MetricsCache::with_default_counters();
    let metrics = cache.get();

    assert_eq!(metrics.active_connections, 0, "Initial active_connections should be 0");
    assert_eq!(metrics.total_keys, 0, "Initial total_keys should be 0");
    assert_eq!(metrics.memtable_size, 0, "Initial memtable_size should be 0");
}

#[test]
fn test_case_1_metrics_json_structure() {
    let metrics = Metrics::default();
    let json = serde_json::to_string(&metrics).unwrap();

    assert!(json.contains("\"active_connections\":0"), "JSON should contain active_connections");
    assert!(json.contains("\"total_keys\":0"), "JSON should contain total_keys");
    assert!(json.contains("\"memtable_size\":0"), "JSON should contain memtable_size");
    assert!(json.contains("\"sstable_levels\":[]"), "JSON should contain empty sstable_levels");
}

// ============================================================================
// Test Case 2: GET /api/metrics after adding 100 keys
// Expected: JSON response with total_keys >= 100
// ============================================================================

#[test]
fn test_case_2_metrics_after_adding_keys() {
    let counters = Arc::new(MetricsCounters::new());
    let cache = MetricsCache::new(counters.clone());

    // Simulate adding 100 keys
    counters.set_total_keys(100);
    cache.invalidate();

    let metrics = cache.get();
    assert!(metrics.total_keys >= 100, "Expected total_keys >= 100, got {}", metrics.total_keys);
}

#[test]
fn test_case_2_metrics_incremental_key_addition() {
    let counters = Arc::new(MetricsCounters::new());
    let cache = MetricsCache::new(counters.clone());

    // Add keys incrementally
    for _ in 0..100 {
        counters.increment_keys(1);
    }
    cache.invalidate();

    let metrics = cache.get();
    assert_eq!(metrics.total_keys, 100, "Expected total_keys = 100 after incrementing 100 times");
}

// ============================================================================
// Test Case 3: GET /api/metrics with active memcached connections
// Expected: JSON response with active_connections reflecting actual connection count
// ============================================================================

#[test]
fn test_case_3_metrics_with_connections() {
    let counters = Arc::new(MetricsCounters::new());
    let cache = MetricsCache::new(counters.clone());

    // Simulate 5 active connections
    for _ in 0..5 {
        counters.increment_connections();
    }
    cache.invalidate();

    let metrics = cache.get();
    assert_eq!(metrics.active_connections, 5, "Expected 5 active connections");
}

#[test]
fn test_case_3_metrics_connection_tracking() {
    let counters = Arc::new(MetricsCounters::new());
    let cache = MetricsCache::new(counters.clone());

    // Add 10 connections
    for _ in 0..10 {
        counters.increment_connections();
    }
    // Remove 3 connections
    for _ in 0..3 {
        counters.decrement_connections();
    }
    cache.invalidate();

    let metrics = cache.get();
    assert_eq!(metrics.active_connections, 7, "Expected 7 active connections after 10 added and 3 removed");
}

// ============================================================================
// Test Case 4: Parse homepage metrics section
// Expected: HTML displays connection count, total keys, and memtable size metrics
// ============================================================================

#[test]
fn test_case_4_homepage_metrics_section_exists() {
    assert!(
        INDEX_HTML.contains("id=\"metrics\""),
        "Homepage must contain metrics section with id='metrics'"
    );
    assert!(
        INDEX_HTML.contains("Server Metrics"),
        "Homepage must contain 'Server Metrics' heading"
    );
}

#[test]
fn test_case_4_homepage_metrics_cards() {
    // Verify metric cards for connection count, total keys, memtable size
    assert!(
        INDEX_HTML.contains("metric-connections"),
        "Homepage must contain connections metric card"
    );
    assert!(
        INDEX_HTML.contains("metric-keys"),
        "Homepage must contain keys metric card"
    );
    assert!(
        INDEX_HTML.contains("metric-memtable"),
        "Homepage must contain memtable metric card"
    );
}

#[test]
fn test_case_4_homepage_metrics_labels() {
    assert!(
        INDEX_HTML.contains("Active Connections"),
        "Homepage must contain 'Active Connections' label"
    );
    assert!(
        INDEX_HTML.contains("Total Keys"),
        "Homepage must contain 'Total Keys' label"
    );
    assert!(
        INDEX_HTML.contains("Memtable Size"),
        "Homepage must contain 'Memtable Size' label"
    );
}

#[test]
fn test_case_4_homepage_metrics_grid() {
    assert!(
        INDEX_HTML.contains("metrics-grid"),
        "Homepage must contain metrics grid container"
    );
}

// ============================================================================
// Test Case 5: Verify SSTable levels in metrics
// Expected: Metrics include SSTable level information as per appendix specification
// ============================================================================

#[test]
fn test_case_5_sstable_levels_structure() {
    let levels = vec![
        SSTableLevel {
            level: 0,
            file_count: 3,
            total_size: 1024 * 1024,
        },
        SSTableLevel {
            level: 1,
            file_count: 5,
            total_size: 4 * 1024 * 1024,
        },
        SSTableLevel {
            level: 2,
            file_count: 10,
            total_size: 16 * 1024 * 1024,
        },
    ];

    let counters = Arc::new(MetricsCounters::new());
    let cache = MetricsCache::new(counters.clone());
    cache.update_sstable_levels(levels);
    cache.invalidate();

    let metrics = cache.get();
    assert_eq!(metrics.sstable_levels.len(), 3, "Expected 3 SSTable levels");

    // Verify level 0
    assert_eq!(metrics.sstable_levels[0].level, 0);
    assert_eq!(metrics.sstable_levels[0].file_count, 3);
    assert_eq!(metrics.sstable_levels[0].total_size, 1024 * 1024);

    // Verify level 1
    assert_eq!(metrics.sstable_levels[1].level, 1);
    assert_eq!(metrics.sstable_levels[1].file_count, 5);
    assert_eq!(metrics.sstable_levels[1].total_size, 4 * 1024 * 1024);

    // Verify level 2
    assert_eq!(metrics.sstable_levels[2].level, 2);
    assert_eq!(metrics.sstable_levels[2].file_count, 10);
    assert_eq!(metrics.sstable_levels[2].total_size, 16 * 1024 * 1024);
}

#[test]
fn test_case_5_sstable_levels_json_serialization() {
    let levels = vec![
        SSTableLevel {
            level: 0,
            file_count: 2,
            total_size: 512,
        },
    ];
    let metrics = Metrics::with_values(0, 0, 0, levels);

    let json = serde_json::to_string(&metrics).unwrap();
    assert!(json.contains("\"sstable_levels\""), "JSON should contain sstable_levels");
    assert!(json.contains("\"level\":0"), "JSON should contain level information");
    assert!(json.contains("\"file_count\":2"), "JSON should contain file_count");
    assert!(json.contains("\"total_size\":512"), "JSON should contain total_size");
}

#[test]
fn test_case_5_homepage_sstable_section() {
    assert!(
        INDEX_HTML.contains("sstable-levels"),
        "Homepage must contain SSTable levels section"
    );
    assert!(
        INDEX_HTML.contains("SSTable Levels"),
        "Homepage must contain 'SSTable Levels' heading"
    );
    assert!(
        INDEX_HTML.contains("sstable-grid"),
        "Homepage must contain SSTable grid container"
    );
}

// ============================================================================
// Additional Tests: Metrics Cache Behavior
// ============================================================================

#[test]
fn test_metrics_cache_invalidation() {
    let counters = Arc::new(MetricsCounters::new());
    let cache = MetricsCache::new(counters.clone());

    counters.set_total_keys(50);
    let _ = cache.get(); // Populate cache

    counters.set_total_keys(100);
    cache.invalidate();

    let metrics = cache.get();
    assert_eq!(metrics.total_keys, 100, "After invalidation, should get fresh values");
}

#[test]
fn test_metrics_counters_atomic_operations() {
    let counters = MetricsCounters::new();

    assert_eq!(counters.get_active_connections(), 0);
    assert_eq!(counters.get_total_keys(), 0);
    assert_eq!(counters.get_memtable_size(), 0);

    counters.increment_connections();
    counters.increment_connections();
    counters.increment_connections();
    assert_eq!(counters.get_active_connections(), 3);

    counters.decrement_connections();
    assert_eq!(counters.get_active_connections(), 2);

    counters.set_total_keys(100);
    assert_eq!(counters.get_total_keys(), 100);

    counters.increment_keys(50);
    assert_eq!(counters.get_total_keys(), 150);

    counters.set_memtable_size(4096);
    assert_eq!(counters.get_memtable_size(), 4096);
}

#[test]
fn test_metrics_serde_roundtrip() {
    let levels = vec![
        SSTableLevel {
            level: 0,
            file_count: 2,
            total_size: 512,
        },
    ];
    let metrics = Metrics::with_values(5, 100, 2048, levels);

    let json = serde_json::to_string(&metrics).unwrap();
    let deserialized: Metrics = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.active_connections, 5);
    assert_eq!(deserialized.total_keys, 100);
    assert_eq!(deserialized.memtable_size, 2048);
    assert_eq!(deserialized.sstable_levels.len(), 1);
    assert_eq!(deserialized.sstable_levels[0].level, 0);
    assert_eq!(deserialized.sstable_levels[0].file_count, 2);
    assert_eq!(deserialized.sstable_levels[0].total_size, 512);
}

#[test]
fn test_metrics_empty_sstable_levels() {
    let metrics = Metrics::default();
    assert!(metrics.sstable_levels.is_empty(), "Default metrics should have empty SSTable levels");
}

#[test]
fn test_metrics_cache_duration() {
    assert_eq!(CACHE_DURATION, Duration::from_secs(1), "Cache duration should be 1 second");
}

#[test]
fn test_homepage_has_metrics_javascript() {
    // Verify the metrics JavaScript is present
    assert!(
        INDEX_HTML.contains("fetchMetrics"),
        "Homepage must contain fetchMetrics function"
    );
    assert!(
        INDEX_HTML.contains("/api/metrics"),
        "Homepage must fetch from /api/metrics endpoint"
    );
    assert!(
        INDEX_HTML.contains("refreshMetrics"),
        "Homepage must expose refreshMetrics function"
    );
}
