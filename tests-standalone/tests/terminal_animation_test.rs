//! Integration tests for Terminal Demo Animation (Scenario 7)
//!
//! These tests verify that the terminal animation elements exist and function correctly
//! as specified in the scenario requirements:
//! - Terminal animation container element exists in hero section
//! - Animation shows set, get, and delete commands with realistic responses
//! - Animation configuration supports looping

use std::fs;
use std::path::PathBuf;

fn get_html_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.push("mirdb-server/assets/index.html");
    p
}

fn get_js_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.push("mirdb-server/assets/console.js");
    p
}

fn get_css_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.push("mirdb-server/assets/styles.css");
    p
}

fn read_homepage_html() -> String {
    let path = get_html_path();
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read index.html from {:?}: {}", path, e))
}

fn read_console_js() -> String {
    let path = get_js_path();
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read console.js from {:?}: {}", path, e))
}

fn read_styles_css() -> String {
    let path = get_css_path();
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read styles.css from {:?}: {}", path, e))
}

// ============================================================================
// Test Case 1: Terminal animation container element exists (E2E)
// ============================================================================

#[test]
fn test_terminal_demo_container_exists_in_hero() {
    let html = read_homepage_html();
    assert!(
        html.contains("terminal-demo"),
        "Hero section should contain terminal-demo element"
    );
}

#[test]
fn test_terminal_animation_output_element_exists() {
    let html = read_homepage_html();
    assert!(
        html.contains("terminal-animation-output"),
        "Terminal demo should contain terminal-animation-output element"
    );
}

#[test]
fn test_terminal_window_structure_exists() {
    let html = read_homepage_html();
    assert!(
        html.contains("terminal-window"),
        "Terminal demo should contain terminal-window element"
    );
    assert!(
        html.contains("terminal-header"),
        "Terminal window should contain terminal-header element"
    );
    assert!(
        html.contains("terminal-body"),
        "Terminal window should contain terminal-body element"
    );
}

#[test]
fn test_terminal_has_macos_style_buttons() {
    let html = read_homepage_html();
    assert!(
        html.contains("terminal-buttons"),
        "Terminal header should contain macOS-style buttons"
    );
}

#[test]
fn test_terminal_has_aria_accessibility() {
    let html = read_homepage_html();
    // Check for aria-live for screen reader support
    assert!(
        html.contains("aria-live") && html.contains("terminal-animation"),
        "Terminal animation should have aria-live attribute for accessibility"
    );
}

// ============================================================================
// Test Case 2: Animation shows set, get, and delete commands (E2E)
// ============================================================================

#[test]
fn test_animation_config_contains_set_command() {
    let js = read_console_js();
    assert!(
        js.contains("set ") || js.contains("'set"),
        "Animation should include SET command demonstration"
    );
}

#[test]
fn test_animation_config_contains_get_command() {
    let js = read_console_js();
    assert!(
        js.contains("get ") || js.contains("'get"),
        "Animation should include GET command demonstration"
    );
}

#[test]
fn test_animation_config_contains_delete_command() {
    let js = read_console_js();
    assert!(
        js.contains("delete ") || js.contains("'delete"),
        "Animation should include DELETE command demonstration"
    );
}

#[test]
fn test_animation_config_contains_stored_response() {
    let js = read_console_js();
    assert!(
        js.contains("STORED"),
        "Animation should show STORED response for SET commands"
    );
}

#[test]
fn test_animation_config_contains_deleted_response() {
    let js = read_console_js();
    assert!(
        js.contains("DELETED"),
        "Animation should show DELETED response for DELETE commands"
    );
}

#[test]
fn test_animation_config_contains_value_response() {
    let js = read_console_js();
    assert!(
        js.contains("VALUE"),
        "Animation should show VALUE response for GET commands"
    );
}

#[test]
fn test_animation_config_contains_end_response() {
    let js = read_console_js();
    assert!(
        js.contains("END"),
        "Animation should show END marker for GET responses"
    );
}

// ============================================================================
// Test Case 3: Animation loops continuously (behavior verification)
// ============================================================================

#[test]
fn test_animation_has_loop_configuration() {
    let js = read_console_js();
    // Check for loop-related code
    assert!(
        js.contains("while") || js.contains("Loop") || js.contains("loop"),
        "Animation should have looping mechanism"
    );
}

#[test]
fn test_animation_has_pause_between_loops() {
    let js = read_console_js();
    assert!(
        js.contains("pauseBetweenLoops") || js.contains("pause"),
        "Animation should have pause between loops for smooth restart"
    );
}

#[test]
fn test_animation_has_typing_effect() {
    let js = read_console_js();
    assert!(
        js.contains("typeText") || js.contains("typing") || js.contains("typingSpeed"),
        "Animation should have typing effect functionality"
    );
}

#[test]
fn test_animation_has_init_function() {
    let js = read_console_js();
    assert!(
        js.contains("initTerminalAnimation"),
        "Animation should have initialization function"
    );
}

#[test]
fn test_animation_auto_starts_on_dom_ready() {
    let js = read_console_js();
    assert!(
        js.contains("DOMContentLoaded") && js.contains("initTerminalAnimation"),
        "Animation should auto-start when DOM is ready"
    );
}

// ============================================================================
// CSS Animation Styles Tests
// ============================================================================

#[test]
fn test_terminal_window_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-window"),
        "CSS should have terminal-window styles"
    );
}

#[test]
fn test_terminal_header_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-header"),
        "CSS should have terminal-header styles"
    );
}

#[test]
fn test_terminal_body_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-body"),
        "CSS should have terminal-body styles"
    );
}

#[test]
fn test_terminal_line_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-line"),
        "CSS should have terminal-line styles"
    );
}

#[test]
fn test_cursor_blink_animation_exists() {
    let css = read_styles_css();
    assert!(
        css.contains("blink") || css.contains("@keyframes"),
        "CSS should have cursor blink animation"
    );
}

#[test]
fn test_terminal_prompt_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-prompt"),
        "CSS should have terminal-prompt styles for commands"
    );
}

#[test]
fn test_terminal_response_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-response"),
        "CSS should have terminal-response styles for responses"
    );
}

#[test]
fn test_terminal_animation_output_styles_exist() {
    let css = read_styles_css();
    assert!(
        css.contains(".terminal-animation-output"),
        "CSS should have terminal-animation-output styles"
    );
}
