//! Integration Tests for Theme Toggle Functionality
//!
//! Scenario 6 - Theme Toggle Functionality
//!
//! These tests verify that the homepage supports dark/light theme toggling:
//! - Test Case 1: Page contains a clickable theme toggle button/switch
//! - Test Case 2: Page has CSS for switching to dark mode with appropriate colors
//! - Test Case 3: Page has CSS for switching to light mode with appropriate colors
//! - Test Case 4: Theme preference persistence via localStorage is implemented

/// The embedded HTML content from the homepage
const INDEX_HTML: &str = include_str!("../../mirdb-server/assets/index.html");

/// The embedded CSS content
const STYLE_CSS: &str = include_str!("../../mirdb-server/assets/css/style.css");

/// The embedded JavaScript content
const MAIN_JS: &str = include_str!("../../mirdb-server/assets/js/main.js");

// ============================================================================
// Test Case 1: Page contains a clickable theme toggle button/switch
// ============================================================================

#[test]
fn test_case_1_theme_toggle_element_exists() {
    // Verify the HTML contains a theme toggle button with proper id
    assert!(
        INDEX_HTML.contains("id=\"theme-toggle\"") || INDEX_HTML.contains("id='theme-toggle'"),
        "HTML must contain theme toggle button with id='theme-toggle'"
    );
}

#[test]
fn test_case_1_theme_toggle_is_button() {
    // Verify the theme toggle is a button element (clickable)
    assert!(
        INDEX_HTML.contains("<button") && INDEX_HTML.contains("theme-toggle"),
        "Theme toggle must be a button element"
    );
}

#[test]
fn test_case_1_theme_toggle_has_aria_label() {
    // Verify accessibility - button should have aria-label
    assert!(
        INDEX_HTML.contains("aria-label") && INDEX_HTML.contains("theme"),
        "Theme toggle button must have aria-label for accessibility"
    );
}

#[test]
fn test_case_1_theme_toggle_has_icon() {
    // Verify the toggle has a theme icon span
    assert!(
        INDEX_HTML.contains("theme-icon"),
        "Theme toggle must have a theme-icon element for visual indicator"
    );
}

#[test]
fn test_case_1_theme_toggle_has_css_styles() {
    // Verify CSS contains styles for the theme toggle
    assert!(
        STYLE_CSS.contains(".theme-toggle"),
        "CSS must have styles for .theme-toggle class"
    );
}

#[test]
fn test_case_1_theme_toggle_has_click_handler() {
    // Verify JavaScript sets up click handler for theme toggle
    assert!(
        MAIN_JS.contains("getElementById") && MAIN_JS.contains("theme-toggle"),
        "JavaScript must reference theme-toggle element by id"
    );
    assert!(
        MAIN_JS.contains("addEventListener") && MAIN_JS.contains("click"),
        "JavaScript must add click event listener for toggle"
    );
}

// ============================================================================
// Test Case 2: Page switches to dark mode with appropriate color scheme
// ============================================================================

#[test]
fn test_case_2_dark_theme_css_exists() {
    // Verify CSS has dark theme styles using data-theme attribute
    assert!(
        STYLE_CSS.contains("[data-theme=\"dark\"]") || STYLE_CSS.contains("[data-theme='dark']"),
        "CSS must have dark theme styles using data-theme attribute"
    );
}

#[test]
fn test_case_2_dark_theme_has_dark_background() {
    // Verify dark theme has appropriate dark background color
    // Dark backgrounds should have low lightness values
    let dark_section = STYLE_CSS.split("[data-theme=\"dark\"]")
        .nth(1)
        .or_else(|| STYLE_CSS.split("[data-theme='dark']").nth(1))
        .unwrap_or("");

    // Check for typical dark background colors (hex starting with low values)
    assert!(
        dark_section.contains("#1") || dark_section.contains("#0") ||
        dark_section.contains("--color-bg") ||
        STYLE_CSS.contains("--color-bg: #1") || STYLE_CSS.contains("--color-bg: #0"),
        "Dark theme must define a dark background color"
    );
}

#[test]
fn test_case_2_dark_theme_has_light_text() {
    // Verify dark theme has light text color for readability
    let css_lower = STYLE_CSS.to_lowercase();

    // Check that dark theme section redefines text colors
    assert!(
        css_lower.contains("data-theme") && css_lower.contains("--color-text"),
        "Dark theme must define text colors for readability"
    );
}

#[test]
fn test_case_2_js_can_apply_dark_theme() {
    // Verify JavaScript has function to apply dark theme
    assert!(
        MAIN_JS.contains("data-theme") || MAIN_JS.contains("setAttribute"),
        "JavaScript must be able to set data-theme attribute for dark mode"
    );
    assert!(
        MAIN_JS.contains("dark") && MAIN_JS.contains("applyTheme"),
        "JavaScript must have applyTheme function that can set dark theme"
    );
}

#[test]
fn test_case_2_toggle_function_exists() {
    // Verify JavaScript has toggle function
    assert!(
        MAIN_JS.contains("toggleTheme") || MAIN_JS.contains("toggle"),
        "JavaScript must have a function to toggle between themes"
    );
}

// ============================================================================
// Test Case 3: Page switches to light mode with appropriate color scheme
// ============================================================================

#[test]
fn test_case_3_light_theme_is_default() {
    // Verify light theme is the default (variables in :root without data-theme)
    assert!(
        STYLE_CSS.contains(":root {") || STYLE_CSS.contains(":root{"),
        "CSS must have :root selector for default (light) theme"
    );

    // Verify :root has light background color (high value hex like #fff or #f)
    let root_section: String = STYLE_CSS.lines()
        .skip_while(|line| !line.contains(":root"))
        .take_while(|line| !line.contains("[data-theme"))
        .collect();

    assert!(
        root_section.contains("#fff") || root_section.contains("#FFF") ||
        root_section.contains("#ffffff") || root_section.contains("#FFFFFF") ||
        root_section.contains("--color-bg: #f"),
        "Default :root must define a light background color"
    );
}

#[test]
fn test_case_3_light_theme_has_dark_text() {
    // Verify light theme has dark text for readability
    let root_section: String = STYLE_CSS.lines()
        .skip_while(|line| !line.contains(":root"))
        .take_while(|line| !line.contains("[data-theme"))
        .collect();

    assert!(
        root_section.contains("--color-text") &&
        (root_section.contains("#3") || root_section.contains("#2") ||
         root_section.contains("#1") || root_section.contains("#0")),
        "Light theme must have dark text color for readability"
    );
}

#[test]
fn test_case_3_js_can_remove_dark_theme() {
    // Verify JavaScript can switch back to light mode by removing data-theme
    assert!(
        MAIN_JS.contains("removeAttribute") || MAIN_JS.contains("remove"),
        "JavaScript must be able to remove dark theme attribute for light mode"
    );
}

#[test]
fn test_case_3_light_constant_defined() {
    // Verify JavaScript defines light theme constant
    assert!(
        MAIN_JS.contains("light") && MAIN_JS.contains("THEME_LIGHT"),
        "JavaScript must define THEME_LIGHT constant"
    );
}

// ============================================================================
// Test Case 4: Theme preference is preserved via localStorage
// ============================================================================

#[test]
fn test_case_4_local_storage_key_defined() {
    // Verify JavaScript uses localStorage for persistence
    assert!(
        MAIN_JS.contains("localStorage"),
        "JavaScript must use localStorage for theme persistence"
    );
}

#[test]
fn test_case_4_save_preference_function() {
    // Verify JavaScript has function to save theme preference
    assert!(
        MAIN_JS.contains("saveThemePreference") || MAIN_JS.contains("setItem"),
        "JavaScript must have function to save theme preference"
    );
    assert!(
        MAIN_JS.contains("localStorage.setItem"),
        "JavaScript must call localStorage.setItem to persist theme"
    );
}

#[test]
fn test_case_4_load_preference_on_init() {
    // Verify JavaScript loads stored preference on initialization
    assert!(
        MAIN_JS.contains("getItem") && MAIN_JS.contains("localStorage"),
        "JavaScript must call localStorage.getItem to load saved theme"
    );
    assert!(
        MAIN_JS.contains("initTheme") || MAIN_JS.contains("init"),
        "JavaScript must have initialization function for theme"
    );
}

#[test]
fn test_case_4_theme_key_is_consistent() {
    // Verify JavaScript uses a consistent storage key
    assert!(
        MAIN_JS.contains("THEME_KEY") || MAIN_JS.contains("mirdb-theme") ||
        MAIN_JS.contains("theme-preference"),
        "JavaScript must define a consistent localStorage key for theme"
    );
}

#[test]
fn test_case_4_preference_saved_on_toggle() {
    // Verify that toggling also saves the preference
    // The toggle function should call save
    assert!(
        MAIN_JS.contains("toggleTheme") && MAIN_JS.contains("save"),
        "Toggle function must save the new theme preference"
    );
}

#[test]
fn test_case_4_handles_storage_errors() {
    // Verify JavaScript handles localStorage errors gracefully (try/catch)
    assert!(
        MAIN_JS.contains("try") && MAIN_JS.contains("catch"),
        "JavaScript must handle localStorage errors gracefully with try/catch"
    );
}

// ============================================================================
// Additional Tests - System Theme Detection
// ============================================================================

#[test]
fn test_system_theme_detection() {
    // Verify JavaScript can detect system color scheme preference
    assert!(
        MAIN_JS.contains("matchMedia") && MAIN_JS.contains("prefers-color-scheme"),
        "JavaScript should detect system color scheme preference"
    );
}

#[test]
fn test_theme_transition_css() {
    // Verify CSS has smooth transitions for theme changes
    assert!(
        STYLE_CSS.contains("transition") && STYLE_CSS.contains("background-color"),
        "CSS should have smooth transitions for theme changes"
    );
}

// ============================================================================
// Icon Update Tests
// ============================================================================

#[test]
fn test_toggle_icon_updates() {
    // Verify JavaScript updates the toggle icon based on current theme
    assert!(
        MAIN_JS.contains("updateToggleIcon") || MAIN_JS.contains("theme-icon"),
        "JavaScript must update the toggle button icon when theme changes"
    );
}

#[test]
fn test_icon_constants_defined() {
    // Verify icons are defined for both themes
    assert!(
        MAIN_JS.contains("ICON_") || MAIN_JS.contains("sun") || MAIN_JS.contains("moon") ||
        MAIN_JS.contains("\u{263E}") || MAIN_JS.contains("\u{2600}"),
        "JavaScript must define icon constants for theme states"
    );
}
