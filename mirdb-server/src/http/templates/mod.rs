/**
 * HTML template utilities.
 *
 * This file is created by the first scenario builder.
 *
 * Expected exports:
 * - fn layout(title: &str, content: &str) -> String
 *   Wraps content in a shared HTML layout with navigation header and footer.
 *   Includes viewport meta tag, semantic HTML structure, and navigation links.
 *
 * The layout should include:
 * - DOCTYPE html declaration
 * - html lang="en"
 * - head with charset, viewport, title
 * - body with header (nav), main, footer
 * - Navigation links to /, /quick-start, /status, /docs, /about
 */

pub mod docs_about;
pub mod homepage;
pub mod quick_start;
pub mod status_page;
pub mod styles;

use crate::http::templates::styles::global_styles;

/// Wraps page content in a shared semantic HTML layout.
///
/// The layout includes:
/// - DOCTYPE html and html lang="en"
/// - Viewport meta tag for responsive design
/// - Embedded CSS styles (no external dependencies)
/// - Semantic HTML5 structure: header, nav, main, section, footer
/// - Proper heading hierarchy: single h1 in content, h2 for sections
/// - Keyboard-navigable navigation links
/// - Accessible landmarks and ARIA labels
pub fn layout(title: &str, content: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{}</title>
    <style>{}</style>
</head>
<body>
    <header role="banner">
        <div class="container">
            <a href="/" class="logo" aria-label="MirDB Home">
                <img src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='32' height='32' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='4' fill='%232c3e50'/%3E%3Ctext x='16' y='22' text-anchor='middle' fill='white' font-size='16' font-family='monospace'%3EM%3C/text%3E%3C/svg%3E" alt="MirDB logo">
                <span class="logo-text">MirDB</span>
            </a>
            <nav role="navigation" aria-label="Main navigation">
                <ul>
                    <li><a href="/">Home</a></li>
                    <li><a href="/quick-start">Quick Start</a></li>
                    <li><a href="/status">Status</a></li>
                    <li><a href="/docs">Docs</a></li>
                    <li><a href="/about">About</a></li>
                </ul>
            </nav>
        </div>
    </header>
    <main role="main">
        <div class="container">
            {}
        </div>
    </main>
    <footer role="contentinfo">
        <div class="container">
            <p>MirDB - A Persistent Key-Value Store. Built with Rust.</p>
            <p><a href="https://github.com/yetone/mirdb" aria-label="MirDB GitHub Repository">GitHub</a></p>
        </div>
    </footer>
</body>
</html>"#,
        title,
        global_styles(),
        content
    )
}

#[cfg(test)]
mod test {
    use super::*;

    fn sample_content() -> &'static str {
        r#"<h1>Test Page</h1>
<section>
    <h2>Section One</h2>
    <p>Some content here.</p>
</section>
<section>
    <h2>Section Two</h2>
    <p>More content here.</p>
</section>"#
    }

    /// Test case 1: Parse homepage HTML head section
    /// Expected: Contains meta viewport tag for responsive design
    #[test]
    fn test_viewport_meta_tag() {
        let html = layout("Test", sample_content());
        assert!(
            html.contains(r#"<meta name="viewport" content="width=device-width, initial-scale=1.0">"#),
            "HTML must contain viewport meta tag for responsive design"
        );
    }

    /// Test case 2: Validate semantic HTML structure
    /// Expected: Uses header, nav, main, section, footer elements (not just divs)
    #[test]
    fn test_semantic_html_structure() {
        let html = layout("Test", sample_content());
        assert!(html.contains("<header"), "HTML must contain <header> element");
        assert!(html.contains("<nav"), "HTML must contain <nav> element");
        assert!(html.contains("<main"), "HTML must contain <main> element");
        assert!(html.contains("<section"), "HTML must contain <section> element");
        assert!(html.contains("<footer"), "HTML must contain <footer> element");
        assert!(
            html.contains("<!DOCTYPE html>"),
            "HTML must contain DOCTYPE declaration"
        );
        assert!(
            html.contains(r#"<html lang="en">"#),
            "HTML must have lang attribute on html element"
        );
    }

    /// Test case 3: Check heading hierarchy
    /// Expected: Single h1 element, h2 elements for major sections, no skipped levels
    #[test]
    fn test_heading_hierarchy() {
        let html = layout("Test", sample_content());
        let h1_count = html.matches("<h1").count();
        let h2_count = html.matches("<h2").count();

        assert_eq!(h1_count, 1, "There should be exactly one h1 element in the content");
        assert!(h2_count >= 2, "There should be h2 elements for major sections");
    }

    /// Test case 4: Check for alt text on images
    /// Expected: All img tags have non-empty alt attributes
    #[test]
    fn test_images_have_alt_text() {
        let html = layout("Test", sample_content());

        // Parse all img tags and verify they have non-empty alt attributes
        let mut pos = 0;
        let mut img_count = 0;
        while let Some(img_start) = html[pos..].find("<img") {
            let start = pos + img_start;
            let end = html[start..].find(">").expect("img tag must be closed");
            let img_tag = &html[start..start + end + 1];
            img_count += 1;

            assert!(
                img_tag.contains("alt="),
                "img tag must have alt attribute: {}",
                img_tag
            );

            // Extract alt value and verify it's non-empty
            let alt_start = img_tag.find("alt=\"").or_else(|| img_tag.find("alt='"));
            if let Some(alt_pos) = alt_start {
                let quote_char = &img_tag[alt_pos + 4..alt_pos + 5];
                let value_start = alt_pos + 5;
                let value_end = img_tag[value_start..].find(quote_char).unwrap_or(0);
                let alt_value = &img_tag[value_start..value_start + value_end];
                assert!(
                    !alt_value.is_empty(),
                    "alt attribute must not be empty: {}",
                    img_tag
                );
            }

            pos = start + end + 1;
        }

        assert!(img_count > 0, "There should be at least one image in the layout");
    }

    /// Test case 5: Check CSS inclusion method
    /// Expected: Styles are inline or in a style block within the HTML (no external link rel=stylesheet)
    #[test]
    fn test_css_is_embedded() {
        let html = layout("Test", sample_content());

        assert!(
            html.contains("<style>"),
            "HTML must contain embedded <style> block"
        );
        assert!(
            !html.contains(r#"<link rel="stylesheet""#),
            "HTML must not contain external stylesheet links"
        );
        assert!(
            !html.contains(r#"<link rel='stylesheet'"#),
            "HTML must not contain external stylesheet links (single quotes)"
        );
    }

    /// Test case 6: Verify system fonts are used
    /// Expected: CSS font-family uses system fonts (no external font loading)
    #[test]
    fn test_system_fonts() {
        let styles = global_styles();

        assert!(
            styles.contains("font-family"),
            "CSS must specify font-family"
        );
        assert!(
            styles.contains("system-ui") || styles.contains("-apple-system") || styles.contains("Segoe UI") || styles.contains("sans-serif"),
            "CSS must use system fonts"
        );
        assert!(
            !styles.contains("@font-face"),
            "CSS must not use @font-face for external fonts"
        );
        assert!(
            !styles.contains("googleapis.com"),
            "CSS must not load fonts from external sources"
        );
    }

    #[test]
    fn test_keyboard_navigation_support() {
        let html = layout("Test", sample_content());

        // All navigation links should be anchor tags (natively keyboard focusable)
        let nav_start = html.find("<nav").expect("nav element must exist");
        let nav_end = html[nav_start..].find("</nav>").expect("nav must close");
        let nav_content = &html[nav_start..nav_start + nav_end];

        // Count anchor tags in nav
        let link_count = nav_content.matches("<a ").count();
        assert!(link_count >= 5, "Navigation should have at least 5 links");

        // Verify no tabindex=-1 on links (which would remove from tab order)
        assert!(
            !nav_content.contains("tabindex=\"-1\""),
            "Navigation links should not have tabindex=-1"
        );
    }

    #[test]
    fn test_aria_landmarks() {
        let html = layout("Test", sample_content());

        assert!(
            html.contains(r#"role="banner""#),
            "Header should have banner role"
        );
        assert!(
            html.contains(r#"role="navigation""#),
            "Nav should have navigation role"
        );
        assert!(
            html.contains(r#"role="main""#),
            "Main should have main role"
        );
        assert!(
            html.contains(r#"role="contentinfo""#),
            "Footer should have contentinfo role"
        );
    }

    #[test]
    fn test_aria_labels_on_navigation() {
        let html = layout("Test", sample_content());

        assert!(
            html.contains(r#"aria-label="Main navigation""#),
            "Nav should have aria-label for screen readers"
        );
        assert!(
            html.contains(r#"aria-label="MirDB Home""#),
            "Logo link should have aria-label"
        );
    }

    #[test]
    fn test_high_contrast_colors() {
        let styles = global_styles();

        // Background and text colors should provide good contrast
        assert!(
            styles.contains("background-color") || styles.contains("background:"),
            "Styles should define background colors"
        );
        assert!(
            styles.contains("color:"),
            "Styles should define text colors"
        );
    }

    #[test]
    fn test_responsive_media_queries() {
        let styles = global_styles();

        assert!(
            styles.contains("@media"),
            "Styles should contain responsive media queries"
        );
    }

    #[test]
    fn test_focus_styles_for_keyboard_navigation() {
        let styles = global_styles();

        assert!(
            styles.contains(":focus"),
            "Styles should define focus indicators for keyboard navigation"
        );
    }

    #[test]
    fn test_layout_generates_valid_html() {
        let html = layout("Test Title", sample_content());

        // Basic structural validation
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("<html lang=\"en\">"));
        assert!(html.contains("<head>"));
        assert!(html.contains("<body>"));
        assert!(html.contains("</body>"));
        assert!(html.contains("</html>"));
        assert!(html.contains("<title>Test Title</title>"));
    }
}
