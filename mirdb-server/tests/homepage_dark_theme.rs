/**
 * Dark theme support tests for the MirDB homepage.
 * Owner: Scenario 10 - Dark Theme Support
 *
 * Validates that the homepage supports dark theme:
 * - CSS contains prefers-color-scheme media query
 * - Colors are defined as CSS custom properties
 * - Dark mode uses a dark background color
 */

use std::fs;

const CSS_PATH: &str = "static/styles.css";

fn read_css() -> String {
    fs::read_to_string(CSS_PATH).expect("Failed to read static/styles.css")
}

#[test]
fn test_css_contains_prefers_color_scheme_dark() {
    let css = read_css();

    assert!(
        css.contains("@media (prefers-color-scheme: dark)"),
        "CSS must contain @media (prefers-color-scheme: dark) for dark theme support"
    );
}

#[test]
fn test_css_contains_custom_properties() {
    let css = read_css();

    // Check for :root custom properties
    assert!(
        css.contains(":root"),
        "CSS must define custom properties in :root for theme switching"
    );

    // Check for common theme-related custom properties
    let has_bg_property = css.contains("--bg-color") || css.contains("--background-color");
    let has_text_property = css.contains("--text-color") || css.contains("--foreground-color");

    assert!(
        has_bg_property,
        "CSS must define a background color custom property (e.g., --bg-color or --background-color)"
    );

    assert!(
        has_text_property,
        "CSS must define a text color custom property (e.g., --text-color or --foreground-color)"
    );
}

#[test]
fn test_dark_mode_has_dark_background() {
    let css = read_css();

    // Find the dark mode media query block
    let dark_media_start = css
        .find("@media (prefers-color-scheme: dark)")
        .expect("Dark mode media query not found");

    // Extract the media query block by finding matching braces
    let after_media = &css[dark_media_start..];
    let open_brace = after_media.find('{').expect("Media query missing opening brace");
    let body_start = dark_media_start + open_brace + 1;

    let mut depth: i32 = 1;
    let mut end_idx: Option<usize> = None;
    for (i, ch) in css[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(body_start + i);
                    break;
                }
            }
            _ => {}
        }
    }

    let end = end_idx.expect("Media query block not properly closed");
    let dark_block = &css[body_start..end];

    // Check for a dark background color in the dark mode block
    let dark_colors = ["#121212", "#1a1a1a", "#000000", "#0d0d0d", "#111111"];
    let has_dark_bg = dark_colors.iter().any(|color| dark_block.contains(color));

    assert!(
        has_dark_bg,
        "Dark mode must use a dark background color (e.g., #121212, #1a1a1a, #000000). Found dark block: {}",
        &dark_block[..dark_block.len().min(500)]
    );
}

#[test]
fn test_dark_mode_has_light_text() {
    let css = read_css();

    // Find the dark mode media query block
    let dark_media_start = css
        .find("@media (prefers-color-scheme: dark)")
        .expect("Dark mode media query not found");

    let after_media = &css[dark_media_start..];
    let open_brace = after_media.find('{').expect("Media query missing opening brace");
    let body_start = dark_media_start + open_brace + 1;

    let mut depth: i32 = 1;
    let mut end_idx: Option<usize> = None;
    for (i, ch) in css[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(body_start + i);
                    break;
                }
            }
            _ => {}
        }
    }

    let end = end_idx.expect("Media query block not properly closed");
    let dark_block = &css[body_start..end];

    // Check for light text colors in the dark mode block
    let light_colors = ["#ffffff", "#f5f5f5", "#e0e0e0", "#eeeeee", "#fff"];
    let has_light_text = light_colors.iter().any(|color| dark_block.contains(color));

    assert!(
        has_light_text,
        "Dark mode must use light text colors (e.g., #ffffff, #f5f5f5, #e0e0e0) for readability"
    );
}
