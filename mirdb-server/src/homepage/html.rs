/**
 * HTML template generation for the MirDB homepage.
 * Owner: Shared - First Builder
 *
 * This module generates or embeds the homepage HTML.
 * Uses include_str! or string literals to serve the page.
 *
 * Expected exports:
 * - homepage_html() -> String: Returns the complete homepage HTML document
 * - hero_section() -> String: Returns the hero section HTML fragment
 * - features_section() -> String: Returns the features section HTML fragment
 */

/// Returns the complete homepage HTML document
pub fn homepage_html() -> String {
    include_str!("../../static/index.html").to_string()
}

/// Returns the hero section HTML fragment
pub fn hero_section() -> String {
    r#"
<section id="hero">
    <h1>Welcome to MirDB</h1>
    <p>A persistent key-value store with memcached protocol support</p>
</section>
"#.to_string()
}

/// Returns the features section HTML fragment
pub fn features_section() -> String {
    r#"
<section id="features">
    <div class="feature">
        <h3>High Performance</h3>
        <p>Built on efficient data structures for maximum throughput</p>
    </div>
    <div class="feature">
        <h3>Persistent Storage</h3>
        <p>Your data survives restarts with durable LSM-tree storage</p>
    </div>
    <div class="feature">
        <h3>Memcached Compatible</h3>
        <p>Drop-in replacement compatible with existing memcached clients</p>
    </div>
</section>
"#.to_string()
}
