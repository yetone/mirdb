//! Static Asset Serving
//! Owner: Scenario 19 - Static Asset Serving
//! Co-owner: Scenario 14 - Performance Page Load
//!
//! Expected exports:
//! - serve_static(path: &str) -> Response
//! - Proper MIME types and caching headers
//! - rust-embed integration for binary embedding
//!
//! Performance Optimization (Scenario 14):
//! - ETag generation for cache validation
//! - Cache-Control headers with appropriate max-age
//! - Timing utilities for performance monitoring
//! - Asset size tracking for optimization metrics

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Get MIME type for file extension
pub fn get_mime_type(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "json" => "application/json",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

// ============================================================================
// Performance Optimization - Scenario 14
// ============================================================================

/// Cache control settings for different asset types
/// Performance optimization for NFR-1 (page load within 2 seconds)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CachePolicy {
    /// No caching (dynamic content)
    NoCache,
    /// Short cache for frequently changing assets (5 minutes)
    Short,
    /// Medium cache for semi-static assets (1 hour)
    Medium,
    /// Long cache for static assets (1 day)
    Long,
    /// Immutable cache for versioned assets (1 year)
    Immutable,
}

impl CachePolicy {
    /// Get Cache-Control header value for this policy
    pub fn to_header_value(&self) -> &'static str {
        match self {
            CachePolicy::NoCache => "no-cache, no-store, must-revalidate",
            CachePolicy::Short => "public, max-age=300",
            CachePolicy::Medium => "public, max-age=3600",
            CachePolicy::Long => "public, max-age=86400",
            CachePolicy::Immutable => "public, max-age=31536000, immutable",
        }
    }

    /// Get max-age in seconds
    pub fn max_age_seconds(&self) -> u64 {
        match self {
            CachePolicy::NoCache => 0,
            CachePolicy::Short => 300,
            CachePolicy::Medium => 3600,
            CachePolicy::Long => 86400,
            CachePolicy::Immutable => 31536000,
        }
    }
}

/// Determine cache policy based on file path and MIME type
/// HTML files get shorter cache, static assets get longer cache
pub fn get_cache_policy(path: &str) -> CachePolicy {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        // HTML pages - short cache to get fresh content
        "html" => CachePolicy::Short,
        // CSS and JS - medium cache, could be updated
        "css" | "js" => CachePolicy::Medium,
        // Images and fonts - long cache, rarely change
        "png" | "jpg" | "jpeg" | "svg" | "ico" | "woff" | "woff2" => CachePolicy::Long,
        // JSON API responses - no cache by default
        "json" => CachePolicy::NoCache,
        // Everything else - short cache
        _ => CachePolicy::Short,
    }
}

/// Generate a simple ETag from content bytes using a fast hash
/// ETag enables conditional requests (If-None-Match) for efficient caching
pub fn generate_etag(content: &[u8]) -> String {
    // Use a simple hash based on content length and first/last bytes
    // For production, consider using a proper hash like xxhash or crc32
    let len = content.len();
    let hash = if len == 0 {
        0u64
    } else {
        let first = content.get(0).copied().unwrap_or(0) as u64;
        let last = content.get(len.saturating_sub(1)).copied().unwrap_or(0) as u64;
        let mid = content.get(len / 2).copied().unwrap_or(0) as u64;
        // Simple hash combining length and sample bytes
        len as u64 ^ (first << 16) ^ (last << 8) ^ mid
    };
    format!("\"{}\"", format!("{:x}", hash))
}

/// Check if ETag matches for conditional request handling
pub fn etag_matches(request_etag: &str, content_etag: &str) -> bool {
    // Handle weak ETags (W/"...") by comparing the quoted part
    fn normalize(etag: &str) -> &str {
        let etag = etag.trim();
        if etag.starts_with("W/") {
            &etag[2..]
        } else {
            etag
        }
    }
    normalize(request_etag) == normalize(content_etag)
}

/// Performance timing context for measuring request durations
/// Used to verify NFR-1 compliance (page load within 2 seconds)
#[derive(Debug, Clone)]
pub struct PerformanceTimer {
    start: Instant,
    name: String,
}

impl PerformanceTimer {
    /// Start a new performance timer
    pub fn start(name: &str) -> Self {
        PerformanceTimer {
            start: Instant::now(),
            name: name.to_string(),
        }
    }

    /// Get elapsed duration
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Get elapsed time in milliseconds
    pub fn elapsed_ms(&self) -> u128 {
        self.elapsed().as_millis()
    }

    /// Check if elapsed time is within threshold
    pub fn within_threshold(&self, threshold_ms: u128) -> bool {
        self.elapsed_ms() <= threshold_ms
    }

    /// Get timer name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stop timer and return elapsed milliseconds
    pub fn stop(&self) -> u128 {
        self.elapsed_ms()
    }
}

/// Performance thresholds from NFR requirements
pub mod thresholds {
    /// NFR-1: Homepage shall render within 2 seconds (2000ms)
    pub const PAGE_LOAD_MS: u128 = 2000;
    /// Static assets combined should load within 1000ms
    pub const STATIC_ASSETS_MS: u128 = 1000;
    /// NFR-4: API calls shall complete within 500ms
    pub const API_CALL_MS: u128 = 500;
}

/// Asset metadata for performance monitoring
#[derive(Debug, Clone)]
pub struct AssetMetadata {
    pub path: String,
    pub size_bytes: usize,
    pub mime_type: &'static str,
    pub cache_policy: CachePolicy,
    pub etag: String,
}

impl AssetMetadata {
    /// Create metadata for an asset
    pub fn new(path: &str, content: &[u8]) -> Self {
        AssetMetadata {
            path: path.to_string(),
            size_bytes: content.len(),
            mime_type: get_mime_type(path),
            cache_policy: get_cache_policy(path),
            etag: generate_etag(content),
        }
    }

    /// Check if asset is compressible (text-based content)
    pub fn is_compressible(&self) -> bool {
        matches!(
            self.mime_type,
            "text/html; charset=utf-8"
                | "text/css; charset=utf-8"
                | "application/javascript; charset=utf-8"
                | "application/json"
                | "image/svg+xml"
        )
    }

    /// Estimate compressed size (rough approximation)
    /// Text assets typically compress to ~30% of original size with gzip
    pub fn estimated_compressed_size(&self) -> usize {
        if self.is_compressible() {
            (self.size_bytes as f64 * 0.3) as usize
        } else {
            self.size_bytes
        }
    }
}

/// Static assets embedded in binary
/// This is a placeholder - in production, use rust-embed
pub struct StaticAssets {
    files: HashMap<String, &'static [u8]>,
}

impl StaticAssets {
    /// Create new static assets store
    pub fn new() -> Self {
        StaticAssets {
            files: HashMap::new(),
        }
    }

    /// Get file content by path
    pub fn get(&self, path: &str) -> Option<&[u8]> {
        self.files.get(path).copied()
    }

    /// Get total size of all assets in bytes
    pub fn total_size(&self) -> usize {
        self.files.values().map(|v| v.len()).sum()
    }

    /// Get number of assets
    pub fn count(&self) -> usize {
        self.files.len()
    }
}

impl Default for StaticAssets {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_type_html() {
        assert_eq!(get_mime_type("index.html"), "text/html; charset=utf-8");
    }

    #[test]
    fn test_mime_type_css() {
        assert_eq!(get_mime_type("styles/main.css"), "text/css; charset=utf-8");
    }

    #[test]
    fn test_mime_type_js() {
        assert_eq!(
            get_mime_type("scripts/main.js"),
            "application/javascript; charset=utf-8"
        );
    }

    #[test]
    fn test_mime_type_unknown() {
        assert_eq!(get_mime_type("file.xyz"), "application/octet-stream");
    }

    // ========================================================================
    // Performance Optimization Tests - Scenario 14
    // ========================================================================

    #[test]
    fn test_cache_policy_header_values() {
        assert_eq!(
            CachePolicy::NoCache.to_header_value(),
            "no-cache, no-store, must-revalidate"
        );
        assert_eq!(CachePolicy::Short.to_header_value(), "public, max-age=300");
        assert_eq!(CachePolicy::Medium.to_header_value(), "public, max-age=3600");
        assert_eq!(CachePolicy::Long.to_header_value(), "public, max-age=86400");
        assert_eq!(
            CachePolicy::Immutable.to_header_value(),
            "public, max-age=31536000, immutable"
        );
    }

    #[test]
    fn test_cache_policy_max_age() {
        assert_eq!(CachePolicy::NoCache.max_age_seconds(), 0);
        assert_eq!(CachePolicy::Short.max_age_seconds(), 300);
        assert_eq!(CachePolicy::Medium.max_age_seconds(), 3600);
        assert_eq!(CachePolicy::Long.max_age_seconds(), 86400);
        assert_eq!(CachePolicy::Immutable.max_age_seconds(), 31536000);
    }

    #[test]
    fn test_get_cache_policy_for_html() {
        assert_eq!(get_cache_policy("index.html"), CachePolicy::Short);
        assert_eq!(get_cache_policy("/pages/about.html"), CachePolicy::Short);
    }

    #[test]
    fn test_get_cache_policy_for_css_js() {
        assert_eq!(get_cache_policy("styles/main.css"), CachePolicy::Medium);
        assert_eq!(get_cache_policy("scripts/app.js"), CachePolicy::Medium);
    }

    #[test]
    fn test_get_cache_policy_for_images() {
        assert_eq!(get_cache_policy("logo.png"), CachePolicy::Long);
        assert_eq!(get_cache_policy("photo.jpg"), CachePolicy::Long);
        assert_eq!(get_cache_policy("icon.svg"), CachePolicy::Long);
    }

    #[test]
    fn test_get_cache_policy_for_fonts() {
        assert_eq!(get_cache_policy("font.woff"), CachePolicy::Long);
        assert_eq!(get_cache_policy("font.woff2"), CachePolicy::Long);
    }

    #[test]
    fn test_get_cache_policy_for_json() {
        assert_eq!(get_cache_policy("data.json"), CachePolicy::NoCache);
    }

    #[test]
    fn test_generate_etag_empty_content() {
        let etag = generate_etag(&[]);
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
        assert_eq!(etag, "\"0\"");
    }

    #[test]
    fn test_generate_etag_consistent() {
        let content = b"Hello, World!";
        let etag1 = generate_etag(content);
        let etag2 = generate_etag(content);
        assert_eq!(etag1, etag2, "ETags should be consistent for same content");
    }

    #[test]
    fn test_generate_etag_different_content() {
        let content1 = b"Hello";
        let content2 = b"World";
        let etag1 = generate_etag(content1);
        let etag2 = generate_etag(content2);
        assert_ne!(etag1, etag2, "Different content should produce different ETags");
    }

    #[test]
    fn test_etag_matches_same() {
        let etag = "\"abc123\"";
        assert!(etag_matches(etag, etag));
    }

    #[test]
    fn test_etag_matches_weak() {
        let strong = "\"abc123\"";
        let weak = "W/\"abc123\"";
        assert!(etag_matches(weak, strong));
        assert!(etag_matches(strong, weak));
    }

    #[test]
    fn test_etag_matches_different() {
        assert!(!etag_matches("\"abc\"", "\"xyz\""));
    }

    #[test]
    fn test_performance_timer_basic() {
        let timer = PerformanceTimer::start("test");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = timer.elapsed_ms();
        assert!(elapsed >= 10, "Timer should measure at least 10ms");
        assert_eq!(timer.name(), "test");
    }

    #[test]
    fn test_performance_timer_within_threshold() {
        let timer = PerformanceTimer::start("test");
        // No sleep - should be very fast
        assert!(
            timer.within_threshold(1000),
            "Timer should be within 1000ms threshold"
        );
    }

    #[test]
    fn test_performance_thresholds_values() {
        // NFR-1: Page load within 2 seconds
        assert_eq!(thresholds::PAGE_LOAD_MS, 2000);
        // Static assets combined within 1 second
        assert_eq!(thresholds::STATIC_ASSETS_MS, 1000);
        // NFR-4: API calls within 500ms
        assert_eq!(thresholds::API_CALL_MS, 500);
    }

    #[test]
    fn test_asset_metadata_basic() {
        let content = b"body { color: #333; }";
        let metadata = AssetMetadata::new("styles/main.css", content);

        assert_eq!(metadata.path, "styles/main.css");
        assert_eq!(metadata.size_bytes, content.len());
        assert_eq!(metadata.mime_type, "text/css; charset=utf-8");
        assert_eq!(metadata.cache_policy, CachePolicy::Medium);
        assert!(!metadata.etag.is_empty());
    }

    #[test]
    fn test_asset_metadata_compressible() {
        let html_content = b"<html></html>";
        let css_content = b"body {}";
        let js_content = b"console.log()";
        let png_content = b"\x89PNG";

        assert!(AssetMetadata::new("index.html", html_content).is_compressible());
        assert!(AssetMetadata::new("style.css", css_content).is_compressible());
        assert!(AssetMetadata::new("app.js", js_content).is_compressible());
        assert!(!AssetMetadata::new("image.png", png_content).is_compressible());
    }

    #[test]
    fn test_asset_metadata_estimated_compressed_size() {
        let content = vec![0u8; 1000]; // 1000 bytes
        let metadata = AssetMetadata::new("styles/main.css", &content);

        // CSS is compressible, should estimate ~30% of original
        let compressed = metadata.estimated_compressed_size();
        assert!(
            compressed < metadata.size_bytes,
            "Compressed size should be less than original"
        );
        assert_eq!(compressed, 300); // 30% of 1000

        // PNG is not compressible
        let png_metadata = AssetMetadata::new("image.png", &content);
        assert_eq!(
            png_metadata.estimated_compressed_size(),
            png_metadata.size_bytes
        );
    }

    #[test]
    fn test_static_assets_total_size() {
        let assets = StaticAssets::new();
        assert_eq!(assets.total_size(), 0);
        assert_eq!(assets.count(), 0);
    }

    #[test]
    fn test_performance_timer_stop() {
        let timer = PerformanceTimer::start("test");
        let elapsed = timer.stop();
        assert!(elapsed < 100, "Stop should return elapsed time quickly");
    }

    // ========================================================================
    // Integration-style Performance Tests - Scenario 14
    // These tests verify that static asset operations meet performance targets
    // ========================================================================

    #[test]
    fn test_mime_type_lookup_performance() {
        // MIME type lookup should be very fast (< 1ms for 1000 lookups)
        let timer = PerformanceTimer::start("mime_lookup");
        for _ in 0..1000 {
            let _ = get_mime_type("index.html");
            let _ = get_mime_type("styles/main.css");
            let _ = get_mime_type("scripts/app.js");
            let _ = get_mime_type("images/logo.png");
        }
        assert!(
            timer.within_threshold(100),
            "4000 MIME lookups should complete within 100ms, took {}ms",
            timer.elapsed_ms()
        );
    }

    #[test]
    fn test_etag_generation_performance() {
        // ETag generation should be fast even for larger content
        let content = vec![0u8; 100_000]; // 100KB
        let timer = PerformanceTimer::start("etag_gen");
        for _ in 0..100 {
            let _ = generate_etag(&content);
        }
        assert!(
            timer.within_threshold(50),
            "100 ETag generations for 100KB content should complete within 50ms, took {}ms",
            timer.elapsed_ms()
        );
    }

    #[test]
    fn test_cache_policy_lookup_performance() {
        // Cache policy lookup should be very fast
        let paths = [
            "index.html",
            "styles/main.css",
            "scripts/app.js",
            "images/logo.png",
            "fonts/roboto.woff2",
        ];
        let timer = PerformanceTimer::start("cache_policy");
        for _ in 0..1000 {
            for path in &paths {
                let _ = get_cache_policy(path);
            }
        }
        assert!(
            timer.within_threshold(50),
            "5000 cache policy lookups should complete within 50ms, took {}ms",
            timer.elapsed_ms()
        );
    }

    #[test]
    fn test_asset_metadata_creation_performance() {
        // Metadata creation should be fast
        let content = vec![0u8; 10_000]; // 10KB
        let timer = PerformanceTimer::start("metadata_creation");
        for _ in 0..100 {
            let _ = AssetMetadata::new("styles/main.css", &content);
        }
        assert!(
            timer.within_threshold(20),
            "100 metadata creations should complete within 20ms, took {}ms",
            timer.elapsed_ms()
        );
    }
}
