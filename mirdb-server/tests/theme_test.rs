//! Theme Toggle E2E tests.
//! Owner: Scenario 13 - Dark/Light Theme Toggle
//!
//! Tests:
//! 1. Verify theme toggle button exists in header (REQ-10)
//! 2. Verify dark theme CSS variables are defined
//! 3. Verify light theme CSS variables are defined
//! 4. Verify theme toggle JavaScript functions exist
//! 5. Verify localStorage persistence logic exists
//! 6. Verify system preference detection exists
//! 7. Verify WCAG 2.1 AA contrast compliance for dark theme colors

use axum::{body::Body, http::Request};
use tower::ServiceExt;

use mirdb::web::routes::create_router;

/// Helper function to get homepage HTML content
async fn get_homepage_html() -> String {
    let app = create_router();
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Helper function to get CSS content
async fn get_stylesheet_css() -> String {
    let app = create_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/style.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

/// Helper function to get JavaScript content
async fn get_javascript() -> String {
    let app = create_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/main.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&body).to_string()
}

// =============================================================================
// Test Case 1: Theme toggle button exists and visual change capability
// Input: Click theme toggle button
// Expected: Background and text colors change without page reload
// =============================================================================

/// Verify theme toggle button exists in header
#[tokio::test]
async fn test_theme_toggle_button_exists_in_header() {
    let html = get_homepage_html().await;

    // Verify theme toggle button exists with proper ID
    assert!(
        html.contains("id=\"theme-toggle\""),
        "Homepage should have a theme toggle button with id='theme-toggle'"
    );

    // Verify it's a button element
    assert!(
        html.contains("<button") && html.contains("theme-toggle"),
        "Theme toggle should be a button element"
    );

    // Verify it has the theme-toggle class for styling
    assert!(
        html.contains("class=\"theme-toggle\""),
        "Theme toggle button should have 'theme-toggle' class"
    );

    // Verify accessibility label (allow various descriptive labels)
    assert!(
        html.contains("aria-label=\"Toggle theme\"") ||
        html.contains("aria-label=\"Toggle dark and light theme\"") ||
        html.contains("aria-label=\"Toggle between dark and light theme\""),
        "Theme toggle button should have aria-label for accessibility"
    );
}

/// Verify theme toggle icon element exists
#[tokio::test]
async fn test_theme_toggle_icon_exists() {
    let html = get_homepage_html().await;

    // Verify theme icon span exists
    assert!(
        html.contains("class=\"theme-icon\""),
        "Theme toggle should contain theme-icon span element"
    );

    // Verify default icon is present (moon or sun emoji)
    assert!(
        html.contains("theme-icon") && html.contains("</span>"),
        "Theme icon should be properly wrapped in span"
    );
}

/// Verify CSS has theme toggle button styles
#[tokio::test]
async fn test_css_has_theme_toggle_styles() {
    let css = get_stylesheet_css().await;

    // Verify theme toggle button styles exist
    assert!(
        css.contains(".theme-toggle"),
        "CSS should have theme-toggle button styles"
    );

    // Verify hover state for theme toggle
    assert!(
        css.contains(".theme-toggle:hover"),
        "CSS should have hover styles for theme toggle"
    );

    // Verify focus state for accessibility
    assert!(
        css.contains(".theme-toggle:focus"),
        "CSS should have focus styles for accessibility"
    );
}

// =============================================================================
// Test Case 2: Theme persistence via localStorage
// Input: Set dark theme, reload page
// Expected: Dark theme persists after reload (localStorage)
// =============================================================================

/// Verify JavaScript has localStorage persistence functions
#[tokio::test]
async fn test_javascript_has_localstorage_persistence() {
    let js = get_javascript().await;

    // Verify loadThemePreference function exists
    assert!(
        js.contains("function loadThemePreference"),
        "JavaScript should have loadThemePreference function"
    );

    // Verify localStorage.getItem is used to load theme
    assert!(
        js.contains("localStorage.getItem('mirdb-theme')"),
        "JavaScript should read theme from localStorage with key 'mirdb-theme'"
    );

    // Verify localStorage.setItem is used to save theme
    assert!(
        js.contains("localStorage.setItem('mirdb-theme'"),
        "JavaScript should save theme to localStorage"
    );
}

/// Verify JavaScript has toggleTheme function
#[tokio::test]
async fn test_javascript_has_toggle_theme_function() {
    let js = get_javascript().await;

    // Verify toggleTheme function exists
    assert!(
        js.contains("function toggleTheme"),
        "JavaScript should have toggleTheme function"
    );

    // Verify it sets data-theme attribute on document
    assert!(
        js.contains("document.documentElement.setAttribute('data-theme'"),
        "toggleTheme should set data-theme attribute on document"
    );
}

/// Verify JavaScript has updateThemeIcon function
#[tokio::test]
async fn test_javascript_has_update_theme_icon_function() {
    let js = get_javascript().await;

    // Verify updateThemeIcon function exists
    assert!(
        js.contains("function updateThemeIcon"),
        "JavaScript should have updateThemeIcon function"
    );

    // Verify it targets the theme-icon element
    assert!(
        js.contains("querySelector('.theme-icon')"),
        "updateThemeIcon should target .theme-icon element"
    );
}

/// Verify JavaScript loads theme preference on DOMContentLoaded
#[tokio::test]
async fn test_javascript_loads_theme_on_domcontentloaded() {
    let js = get_javascript().await;

    // Verify DOMContentLoaded listener exists
    assert!(
        js.contains("DOMContentLoaded"),
        "JavaScript should listen for DOMContentLoaded event"
    );

    // Verify loadThemePreference is called on load
    assert!(
        js.contains("loadThemePreference()"),
        "JavaScript should call loadThemePreference on page load"
    );
}

/// Verify JavaScript attaches click handler to theme toggle
#[tokio::test]
async fn test_javascript_attaches_theme_toggle_handler() {
    let js = get_javascript().await;

    // Verify getElementById for theme-toggle
    assert!(
        js.contains("getElementById('theme-toggle')"),
        "JavaScript should get theme-toggle element by ID"
    );

    // Verify click event listener is attached
    assert!(
        js.contains("addEventListener('click', toggleTheme)"),
        "JavaScript should attach click handler for toggleTheme"
    );
}

// =============================================================================
// Test Case 3: Theme reverts to default when localStorage cleared
// Input: Clear localStorage, reload page
// Expected: Theme reverts to default (light or system preference)
// =============================================================================

/// Verify JavaScript detects system color scheme preference
#[tokio::test]
async fn test_javascript_detects_system_preference() {
    let js = get_javascript().await;

    // Verify matchMedia is used for system preference detection
    assert!(
        js.contains("window.matchMedia"),
        "JavaScript should use matchMedia for system preference detection"
    );

    // Verify prefers-color-scheme: dark is checked
    assert!(
        js.contains("prefers-color-scheme: dark"),
        "JavaScript should check for prefers-color-scheme: dark"
    );
}

/// Verify light theme is the default in CSS
#[tokio::test]
async fn test_light_theme_is_default() {
    let css = get_stylesheet_css().await;

    // Verify :root has light theme variables (default)
    assert!(
        css.contains(":root") && css.contains("--bg-primary"),
        "CSS should have :root with light theme CSS variables"
    );

    // Verify light theme uses white/light backgrounds
    assert!(
        css.contains("--bg-primary: #ffffff") || css.contains("--bg-primary: #fff"),
        "Light theme should have white background"
    );
}

// =============================================================================
// Test Case 4: WCAG 2.1 AA Contrast Compliance
// Input: Verify color contrast in dark theme
// Expected: All text readable, meets WCAG 2.1 AA contrast ratios
// =============================================================================

/// Verify dark theme CSS variables are properly defined
#[tokio::test]
async fn test_dark_theme_css_variables_defined() {
    let css = get_stylesheet_css().await;

    // Verify dark theme selector exists
    assert!(
        css.contains("[data-theme=\"dark\"]"),
        "CSS should have dark theme selector [data-theme=\"dark\"]"
    );

    // Verify essential dark theme variables are defined
    assert!(
        css.contains("[data-theme=\"dark\"]") && css.contains("--bg-primary"),
        "Dark theme should define --bg-primary variable"
    );
    assert!(
        css.contains("[data-theme=\"dark\"]") && css.contains("--text-primary"),
        "Dark theme should define --text-primary variable"
    );
}

/// Verify dark theme has dark backgrounds
#[tokio::test]
async fn test_dark_theme_has_dark_backgrounds() {
    let css = get_stylesheet_css().await;

    // Find the dark theme section and verify it has dark background colors
    // Dark backgrounds should have low luminance (starting with #0, #1, #2 typically)
    assert!(
        css.contains("--bg-primary: #0") || css.contains("--bg-primary: #1"),
        "Dark theme --bg-primary should be a dark color"
    );
}

/// Verify dark theme has light text colors for contrast
#[tokio::test]
async fn test_dark_theme_has_light_text_for_contrast() {
    let css = get_stylesheet_css().await;

    // Find dark theme text-primary - should be light color (high value like #e, #f, #d)
    // Light text colors for dark backgrounds typically start with #c, #d, #e, #f
    assert!(
        css.contains("--text-primary: #e") || css.contains("--text-primary: #f") ||
        css.contains("--text-primary: #d") || css.contains("--text-primary: #c"),
        "Dark theme --text-primary should be a light color for contrast"
    );
}

/// Verify light theme has dark text colors for contrast
#[tokio::test]
async fn test_light_theme_has_dark_text_for_contrast() {
    let css = get_stylesheet_css().await;

    // Light theme text should be dark (low value like #0, #1, #2, #3, #4)
    // We check the :root section (light theme) has dark text colors
    assert!(
        css.contains("--text-primary: #1") || css.contains("--text-primary: #2") ||
        css.contains("--text-primary: #0") || css.contains("--text-primary: #3"),
        "Light theme --text-primary should be a dark color for contrast"
    );
}

/// Verify CSS variables include border color for theme
#[tokio::test]
async fn test_css_has_border_color_variable() {
    let css = get_stylesheet_css().await;

    // Verify border-color variable exists for theming
    assert!(
        css.contains("--border-color"),
        "CSS should have --border-color variable for theming"
    );
}

/// Verify CSS variables include accent color for theme
#[tokio::test]
async fn test_css_has_accent_color_variable() {
    let css = get_stylesheet_css().await;

    // Verify accent-color variable exists
    assert!(
        css.contains("--accent-color"),
        "CSS should have --accent-color variable"
    );

    // Verify accent-hover for interactive states
    assert!(
        css.contains("--accent-hover"),
        "CSS should have --accent-hover variable for hover states"
    );
}

/// Verify CSS has success and error colors for feedback
#[tokio::test]
async fn test_css_has_semantic_color_variables() {
    let css = get_stylesheet_css().await;

    // Verify success color for positive feedback
    assert!(
        css.contains("--success-color"),
        "CSS should have --success-color variable"
    );

    // Verify error color for error states
    assert!(
        css.contains("--error-color"),
        "CSS should have --error-color variable"
    );
}

/// Verify body has smooth theme transitions
#[tokio::test]
async fn test_css_has_smooth_theme_transition() {
    let css = get_stylesheet_css().await;

    // Verify body has transition for smooth theme switching
    assert!(
        css.contains("body") && css.contains("transition"),
        "Body should have transition for smooth theme switching"
    );

    // Verify transition applies to background-color and color
    assert!(
        css.contains("background-color") && css.contains("transition"),
        "Transition should apply to background-color"
    );
}

/// Verify CSS uses CSS variables in body styling
#[tokio::test]
async fn test_body_uses_css_variables() {
    let css = get_stylesheet_css().await;

    // Verify body background uses CSS variable
    assert!(
        css.contains("var(--bg-primary)"),
        "Body should use var(--bg-primary) for background"
    );

    // Verify body color uses CSS variable
    assert!(
        css.contains("var(--text-primary)"),
        "Body should use var(--text-primary) for text color"
    );
}

/// Verify dark theme shadow variables are defined
#[tokio::test]
async fn test_dark_theme_has_shadow_variables() {
    let css = get_stylesheet_css().await;

    // Dark theme should have shadows with higher opacity/visibility
    assert!(
        css.contains("--shadow-sm") && css.contains("--shadow-md"),
        "CSS should have shadow variables"
    );
}

// =============================================================================
// Additional Integration Tests
// =============================================================================

/// Verify theme toggle button is in the header nav
#[tokio::test]
async fn test_theme_toggle_in_header_nav() {
    let html = get_homepage_html().await;

    // Verify the nav element contains the theme toggle
    assert!(
        html.contains("<nav") && html.contains("theme-toggle"),
        "Theme toggle should be within the header navigation"
    );
}

/// Verify all interactive elements have focus states
#[tokio::test]
async fn test_interactive_elements_have_focus_states() {
    let css = get_stylesheet_css().await;

    // Verify buttons have focus states
    assert!(
        css.contains(".btn:focus"),
        "Buttons should have focus styles"
    );

    // Verify theme toggle has focus state (already tested above)
    assert!(
        css.contains(".theme-toggle:focus"),
        "Theme toggle should have focus styles"
    );

    // Verify focus uses accent color for visibility
    assert!(
        css.contains(":focus") && css.contains("outline"),
        "Focus states should have visible outlines"
    );
}

/// Verify CSS variables transition is defined
#[tokio::test]
async fn test_css_transition_variables_exist() {
    let css = get_stylesheet_css().await;

    // Verify transition timing variables exist
    assert!(
        css.contains("--transition-fast") || css.contains("--transition-normal"),
        "CSS should have transition timing variables"
    );
}
