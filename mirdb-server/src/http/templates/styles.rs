/**
 * Shared CSS styles for all pages.
 * Owner: Scenario 10 - Responsive Design and Accessibility
 *
 * Expected exports:
 * - fn global_styles() -> &'static str
 *   Returns the CSS string embedded in all pages.
 *
 * Style requirements:
 * - System fonts (font-family: -system-ui, sans-serif)
 * - Responsive design (media queries for mobile)
 * - High contrast colors (4.5:1 ratio minimum)
 * - Clean, minimalist design
 * - Navigation hover effects
 * - No external dependencies
 */

/// Returns the global CSS styles embedded in every page.
///
/// These styles provide:
/// - System font stack for fast loading
/// - Responsive layout with mobile breakpoints
/// - High contrast color scheme for accessibility
/// - Focus-visible indicators for keyboard navigation
/// - Clean, minimalist design
pub fn global_styles() -> &'static str {
    r#"
/* Reset and base styles */
*, *::before, *::after {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

html {
    font-size: 16px;
    -webkit-text-size-adjust: 100%;
    -ms-text-size-adjust: 100%;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji";
    line-height: 1.6;
    color: #1a1a2e;
    background-color: #f8f9fa;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
}

/* Focus styles for keyboard navigation */
*:focus {
    outline: 2px solid #2563eb;
    outline-offset: 2px;
}

*:focus:not(:focus-visible) {
    outline: none;
}

*:focus-visible {
    outline: 2px solid #2563eb;
    outline-offset: 2px;
}

/* Skip link for keyboard users */
.skip-link {
    position: absolute;
    top: -40px;
    left: 0;
    background: #1a1a2e;
    color: #ffffff;
    padding: 8px 16px;
    text-decoration: none;
    z-index: 100;
}

.skip-link:focus {
    top: 0;
}

/* Container */
.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
    width: 100%;
}

/* Header */
header {
    background-color: #1a1a2e;
    color: #ffffff;
    padding: 16px 0;
    border-bottom: 3px solid #2563eb;
}

header .container {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 16px;
}

.logo {
    display: flex;
    align-items: center;
    gap: 12px;
    color: #ffffff;
    text-decoration: none;
    font-weight: 700;
    font-size: 1.5rem;
}

.logo img {
    width: 32px;
    height: 32px;
    border-radius: 4px;
}

.logo-text {
    color: #ffffff;
}

/* Navigation */
nav ul {
    list-style: none;
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
}

nav a {
    color: #e2e8f0;
    text-decoration: none;
    padding: 8px 16px;
    border-radius: 4px;
    font-weight: 500;
    transition: background-color 0.2s ease, color 0.2s ease;
}

nav a:hover,
nav a:focus {
    background-color: #2563eb;
    color: #ffffff;
}

/* Main content */
main {
    flex: 1;
    padding: 40px 0;
}

main .container {
    background-color: #ffffff;
    border-radius: 8px;
    padding: 32px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

/* Headings */
h1 {
    font-size: 2rem;
    font-weight: 700;
    color: #1a1a2e;
    margin-bottom: 24px;
    line-height: 1.3;
}

h2 {
    font-size: 1.5rem;
    font-weight: 600;
    color: #1e293b;
    margin-top: 32px;
    margin-bottom: 16px;
    line-height: 1.4;
}

h3 {
    font-size: 1.25rem;
    font-weight: 600;
    color: #334155;
    margin-top: 24px;
    margin-bottom: 12px;
    line-height: 1.4;
}

/* Paragraphs and text */
p {
    margin-bottom: 16px;
    color: #334155;
}

/* Links */
a {
    color: #2563eb;
    text-decoration: underline;
    text-underline-offset: 2px;
}

a:hover,
a:focus {
    color: #1d4ed8;
}

/* Lists */
ul, ol {
    margin-bottom: 16px;
    padding-left: 24px;
}

li {
    margin-bottom: 8px;
    color: #334155;
}

/* Code blocks */
code {
    font-family: "SF Mono", Monaco, "Cascadia Code", "Roboto Mono", Consolas, "Courier New", monospace;
    background-color: #f1f5f9;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 0.875em;
    color: #1e293b;
}

pre {
    background-color: #1a1a2e;
    color: #e2e8f0;
    padding: 20px;
    border-radius: 8px;
    overflow-x: auto;
    margin-bottom: 24px;
}

pre code {
    background-color: transparent;
    color: inherit;
    padding: 0;
    font-size: 0.875rem;
}

/* Sections */
section {
    margin-bottom: 32px;
}

section:last-child {
    margin-bottom: 0;
}

/* Footer */
footer {
    background-color: #1a1a2e;
    color: #e2e8f0;
    padding: 24px 0;
    text-align: center;
}

footer p {
    color: #94a3b8;
    margin-bottom: 8px;
}

footer a {
    color: #60a5fa;
}

footer a:hover,
footer a:focus {
    color: #93c5fd;
}

/* Tables */
table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 24px;
}

th, td {
    padding: 12px 16px;
    text-align: left;
    border-bottom: 1px solid #e2e8f0;
}

th {
    font-weight: 600;
    color: #1a1a2e;
    background-color: #f1f5f9;
}

td {
    color: #334155;
}

/* Cards */
.card {
    background-color: #f8f9fa;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 24px;
    margin-bottom: 16px;
}

.card h3 {
    margin-top: 0;
}

/* Responsive design - Mobile breakpoint */
@media (max-width: 768px) {
    html {
        font-size: 15px;
    }

    .container {
        padding: 0 16px;
    }

    header .container {
        flex-direction: column;
        align-items: flex-start;
    }

    nav ul {
        flex-direction: column;
        width: 100%;
        gap: 4px;
    }

    nav a {
        display: block;
        width: 100%;
    }

    main {
        padding: 24px 0;
    }

    main .container {
        padding: 20px;
    }

    h1 {
        font-size: 1.5rem;
    }

    h2 {
        font-size: 1.25rem;
    }

    h3 {
        font-size: 1.1rem;
    }

    pre {
        padding: 16px;
        font-size: 0.8rem;
    }
}

/* Print styles */
@media print {
    header, footer, nav {
        display: none;
    }

    main .container {
        box-shadow: none;
        padding: 0;
    }

    body {
        background-color: #ffffff;
        color: #000000;
    }
}
"#
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_styles_is_non_empty() {
        let styles = global_styles();
        assert!(!styles.is_empty(), "global_styles must return non-empty CSS");
    }

    #[test]
    fn test_system_font_stack() {
        let styles = global_styles();
        assert!(
            styles.contains("-apple-system"),
            "Must include -apple-system for macOS/iOS"
        );
        assert!(
            styles.contains("BlinkMacSystemFont"),
            "Must include BlinkMacSystemFont for Chrome on macOS"
        );
        assert!(
            styles.contains("Segoe UI"),
            "Must include Segoe UI for Windows"
        );
        assert!(
            styles.contains("Roboto"),
            "Must include Roboto for Android"
        );
        assert!(
            styles.contains("sans-serif"),
            "Must include generic sans-serif fallback"
        );
    }

    #[test]
    fn test_no_external_font_loading() {
        let styles = global_styles();
        assert!(
            !styles.contains("@font-face"),
            "Must not use @font-face for external font loading"
        );
        assert!(
            !styles.contains("googleapis.com"),
            "Must not reference Google Fonts or external font sources"
        );
        assert!(
            !styles.contains("fonts.googleapis.com"),
            "Must not reference Google Fonts API"
        );
        assert!(
            !styles.contains("cdn"),
            "Must not reference CDN-hosted resources"
        );
    }

    #[test]
    fn test_responsive_media_queries() {
        let styles = global_styles();
        assert!(
            styles.contains("@media (max-width: 768px)"),
            "Must include mobile breakpoint media query"
        );
        assert!(
            styles.contains("@media print"),
            "Must include print media query"
        );
    }

    #[test]
    fn test_high_contrast_colors() {
        let styles = global_styles();
        // Body text should be dark on light background
        assert!(
            styles.contains("color: #1a1a2e") || styles.contains("color: #334155"),
            "Must define dark text colors for high contrast"
        );
        assert!(
            styles.contains("background-color: #f8f9fa") || styles.contains("background-color: #ffffff"),
            "Must define light background colors"
        );
    }

    #[test]
    fn test_focus_indicators() {
        let styles = global_styles();
        assert!(
            styles.contains(":focus"),
            "Must define focus styles for keyboard navigation"
        );
        assert!(
            styles.contains("outline:"),
            "Focus styles must use outline for visibility"
        );
    }

    #[test]
    fn test_hover_effects_on_links() {
        let styles = global_styles();
        assert!(
            styles.contains("a:hover") || styles.contains("nav a:hover"),
            "Must include hover effects on links"
        );
    }

    #[test]
    fn test_no_external_dependencies() {
        let styles = global_styles();
        assert!(
            !styles.contains("@import"),
            "Must not use @import for external stylesheets"
        );
        assert!(
            !styles.contains("url("),
            "Must not use url() for external resources"
        );
    }

    #[test]
    fn test_viewport_aware_styles() {
        let styles = global_styles();
        assert!(
            styles.contains("max-width"),
            "Must use max-width for responsive container"
        );
        assert!(
            styles.contains("flex-wrap"),
            "Must use flex-wrap for responsive layouts"
        );
    }

    #[test]
    fn test_accessible_code_blocks() {
        let styles = global_styles();
        assert!(
            styles.contains("overflow-x: auto"),
            "Code blocks must handle overflow for accessibility"
        );
    }
}
