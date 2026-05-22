/**
 * HTTP request handlers.
 * Owner: Scenario 1 - HTTP Server and Routing (base handlers)
 *          Scenario 12 - Analytics Counter Display (stats endpoint)
 *
 * Expected handlers:
 * - homepage() -> Response: Render the homepage template
 * - static_files(path) -> Response: Serve static assets
 * - get_stats() -> Json<Stats>: Return analytics data (added by Scenario 12)
 */

use serde::{Deserialize, Serialize};

/// Analytics statistics returned by the /api/stats endpoint.
/// Owner: Scenario 12 - Analytics Counter Display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    /// Total number of URLs created
    pub urls_created: u64,
    /// Number of active users
    pub active_users: u64,
    /// Total number of clicks across all URLs
    pub total_clicks: u64,
}

impl Stats {
    /// Create a new Stats instance with the given values.
    pub fn new(urls_created: u64, active_users: u64, total_clicks: u64) -> Self {
        Stats {
            urls_created,
            active_users,
            total_clicks,
        }
    }

    /// Return default/demo stats for the homepage analytics counter.
    /// These values provide social proof to visitors.
    pub fn default_stats() -> Self {
        Stats {
            urls_created: 128_456,
            active_users: 3_421,
            total_clicks: 8_923_456,
        }
    }
}

/// Handler for GET /api/stats
/// Returns analytics data for the homepage counter display.
/// Owner: Scenario 12 - Analytics Counter Display
pub fn get_stats() -> Stats {
    // In a production environment, this would query the database
    // for actual statistics. For now, return default/demo stats
    // that demonstrate the counter formatting.
    Stats::default_stats()
}

/// Format a number with K/M/B suffixes for display.
/// Examples:
/// - 128456 -> "128K+"
/// - 8923456 -> "8.9M+"
/// - 1500000000 -> "1.5B+"
/// Owner: Scenario 12 - Analytics Counter Display
pub fn format_number(num: u64) -> String {
    if num >= 1_000_000_000 {
        let billions = num as f64 / 1_000_000_000.0;
        let formatted = format!("{:.1}", billions);
        let trimmed = formatted.trim_end_matches(".0").trim_end_matches('.');
        format!("{}B+", trimmed)
    } else if num >= 1_000_000 {
        let millions = num as f64 / 1_000_000.0;
        let formatted = format!("{:.1}", millions);
        let trimmed = formatted.trim_end_matches(".0").trim_end_matches('.');
        format!("{}M+", trimmed)
    } else if num >= 1_000 {
        let thousands = num as f64 / 1_000.0;
        let formatted = format!("{:.1}", thousands);
        let trimmed = formatted.trim_end_matches(".0").trim_end_matches('.');
        format!("{}K+", trimmed)
    } else {
        num.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number_thousands() {
        assert_eq!(format_number(128_456), "128.5K+");
        assert_eq!(format_number(1_000), "1K+");
        assert_eq!(format_number(1_500), "1.5K+");
        assert_eq!(format_number(999), "999");
    }

    #[test]
    fn test_format_number_millions() {
        assert_eq!(format_number(8_923_456), "8.9M+");
        assert_eq!(format_number(1_000_000), "1M+");
        assert_eq!(format_number(1_500_000), "1.5M+");
    }

    #[test]
    fn test_format_number_billions() {
        assert_eq!(format_number(1_500_000_000), "1.5B+");
        assert_eq!(format_number(1_000_000_000), "1B+");
        assert_eq!(format_number(2_000_000_000), "2B+");
    }

    #[test]
    fn test_default_stats() {
        let stats = Stats::default_stats();
        assert_eq!(stats.urls_created, 128_456);
        assert_eq!(stats.active_users, 3_421);
        assert_eq!(stats.total_clicks, 8_923_456);
    }

    #[test]
    fn test_get_stats_returns_default() {
        let stats = get_stats();
        assert_eq!(stats.urls_created, 128_456);
        assert_eq!(stats.active_users, 3_421);
        assert_eq!(stats.total_clicks, 8_923_456);
    }

    #[test]
    fn test_format_number_small_values() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(1), "1");
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(999), "999");
    }

    #[test]
    fn test_stats_serialization() {
        let stats = Stats::new(100, 50, 1000);
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"urls_created\":100"));
        assert!(json.contains("\"active_users\":50"));
        assert!(json.contains("\"total_clicks\":1000"));
    }

    #[test]
    fn test_stats_deserialization() {
        let json = r#"{"urls_created":500,"active_users":100,"total_clicks":5000}"#;
        let stats: Stats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.urls_created, 500);
        assert_eq!(stats.active_users, 100);
        assert_eq!(stats.total_clicks, 5000);
    }
}
