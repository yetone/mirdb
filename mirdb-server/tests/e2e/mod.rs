//! End-to-end UI tests for homepage.
//!
//! Uses tower's oneshot testing to verify HTML content and structure
//! of the MirDB homepage UI.
//!
//! Submodules:
//! - homepage_ui_test: Layout, branding, navigation
//! - dashboard_test: Metrics cards, configuration panel, auto-refresh
//! - try_it_out_test: Interactive forms for GET, SET, DELETE operations
//! - responsive_test: Responsive design tests for desktop, tablet, and mobile viewports

mod homepage_ui_test;
mod dashboard_test;
mod try_it_out_test;
mod responsive_test;

// Other e2e test modules will be added by their respective scenarios:
// mod theme_test: moved to tests/theme_test.rs as standalone test
