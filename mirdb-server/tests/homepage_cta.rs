/**
 * CTA validation tests for the MirDB homepage.
 * Owner: Scenario 4 - Primary Call-to-Action
 *
 * Validates that the homepage has a prominent CTA button
 * above the fold with clear, action-oriented text and a valid link.
 */

use std::fs;

const HTML_PATH: &str = "static/index.html";

fn read_html() -> String {
    fs::read_to_string(HTML_PATH).expect("Failed to read index.html")
}

/// Extract content between two marker strings (exclusive of markers)
fn extract_between(html: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start = html.find(start_marker)?;
    let after_start = start + start_marker.len();
    let end = html[after_start..].find(end_marker)?;
    Some(html[after_start..after_start + end].to_string())
}

/// Extract the hero section content from the HTML
fn get_hero_section(html: &str) -> Option<String> {
    extract_between(html, "<section id=\"hero\">", "</section>")
}

#[test]
fn test_cta_element_exists_in_hero() {
    let html = read_html();
    let hero = get_hero_section(&html).expect("Hero section not found");

    // Check for anchor or button element with cta-button class or button role
    let has_cta_anchor = hero.contains("<a") && hero.contains("cta-button");
    let has_cta_button = hero.contains("<button") && hero.contains("cta-button");
    let has_button_role = hero.contains("role=\"button\"") || hero.contains("role='button'");

    assert!(
        has_cta_anchor || has_cta_button || has_button_role,
        "Hero section must contain a CTA anchor/button with cta-button class or button role"
    );
}

#[test]
fn test_cta_text_is_action_oriented() {
    let html = read_html();
    let hero = get_hero_section(&html).expect("Hero section not found");

    // Find the CTA element text - look for action-oriented text
    let action_words = [
        "Get Started",
        "Learn More",
        "Try MirDB",
        "View Documentation",
        "Get Started",
        "Start Now",
        "Try It",
        "Explore",
        "Discover",
        "Launch",
    ];

    let has_action_text = action_words.iter().any(|word| hero.contains(word));

    assert!(
        has_action_text,
        "CTA must have action-oriented text (e.g., 'Get Started', 'Learn More', 'Try MirDB')"
    );
}

#[test]
fn test_cta_text_is_non_empty() {
    let html = read_html();
    let hero = get_hero_section(&html).expect("Hero section not found");

    // Find anchor or button in hero and check it has non-empty text content
    // Extract the element and its text
    let cta_start = hero.find("<a").or_else(|| hero.find("<button"))
        .expect("No CTA anchor or button found in hero");

    // Find the closing tag
    let remaining = &hero[cta_start..];
    let tag_end = remaining.find('>').expect("Unclosed CTA tag") + 1;
    let after_tag = &remaining[tag_end..];

    // Find closing tag
    let closing_anchor = after_tag.find("</a>");
    let closing_button = after_tag.find("</button>");

    let content = if let Some(end) = closing_anchor {
        &after_tag[..end]
    } else if let Some(end) = closing_button {
        &after_tag[..end]
    } else {
        panic!("CTA element has no closing tag");
    };

    let trimmed = content.trim();
    assert!(
        !trimmed.is_empty(),
        "CTA element must have non-empty text content"
    );
}

#[test]
fn test_cta_has_valid_href() {
    let html = read_html();
    let hero = get_hero_section(&html).expect("Hero section not found");

    // Find an anchor tag with cta-button class or button role
    let cta_anchor_pos = hero.find("cta-button")
        .or_else(|| hero.find("role=\"button\""))
        .expect("No CTA element found");

    // Walk backwards to find the opening <a tag
    let before_cta = &hero[..cta_anchor_pos];
    let anchor_start = before_cta.rfind("<a ").expect("CTA element is not an anchor tag");

    let anchor_tag = &hero[anchor_start..cta_anchor_pos + 50];

    // Extract href attribute
    let href_start = anchor_tag.find("href=").expect("CTA anchor missing href attribute");
    let after_href = &anchor_tag[href_start + 5..];

    // Handle quoted href values
    let quote = after_href.chars().next().expect("Empty href value");
    let delimiter = match quote {
        '"' => '"',
        '\'' => '\'',
        _ => {
            // Unquoted - find next whitespace
            let end = after_href.find(|c: char| c.is_whitespace()).unwrap_or(after_href.len());
            let href_value = &after_href[..end];
            assert!(!href_value.is_empty(), "CTA href must not be empty");
            assert!(
                href_value.starts_with("http://")
                    || href_value.starts_with("https://")
                    || href_value.starts_with("/"),
                "CTA href must be a valid URL, got: {}",
                href_value
            );
            return;
        }
    };

    let after_quote = &after_href[1..];
    let end = after_quote.find(delimiter).expect("Unclosed href quote");
    let href_value = &after_quote[..end];

    assert!(!href_value.is_empty(), "CTA href must not be empty");
    assert!(
        href_value.starts_with("http://")
            || href_value.starts_with("https://")
            || href_value.starts_with("/"),
        "CTA href must be a valid URL starting with http://, https://, or /, got: {}",
        href_value
    );
}

#[test]
fn test_cta_is_above_the_fold() {
    let html = read_html();

    // Find hero section start and features section start
    let hero_start = html.find("<section id=\"hero\">").expect("Hero section not found");
    let features_start = html.find("<section id=\"features\">").expect("Features section not found");

    // Find CTA element within hero section (before features)
    let hero_section = &html[hero_start..features_start];

    let cta_in_hero = hero_section.contains("cta-button")
        || hero_section.contains("role=\"button\"");

    assert!(
        cta_in_hero,
        "CTA element must appear within the hero section before the features section"
    );
}

#[test]
fn test_cta_is_most_prominent_interactive_element() {
    let html = read_html();
    let hero = get_hero_section(&html).expect("Hero section not found");

    // Check that the CTA has styling that makes it stand out
    // The cta-button class should have visual prominence
    assert!(
        hero.contains("cta-button"),
        "CTA must have a prominent styling class like 'cta-button'"
    );
}
