/**
 * Homepage HTML template.
 * Owner: Scenario 3 - Homepage Route and Content
 *
 * Expected exports:
 * - fn render() -> String
 *   Returns the complete homepage HTML string wrapped in the shared layout.
 *
 * Content requirements:
 * - MirDB product name and tagline
 * - ASCII art logo in monospace font
 * - Feature cards (Memcached protocol, persistence, LSM-tree, etc.)
 * - Quick teaser link to /quick-start
 * - Footer with GitHub link
 */

use super::layout;

pub fn render() -> String {
    let content = r#"
<h1>Welcome to MirDB</h1>
<section>
    <h2>A Persistent Key-Value Store</h2>
    <p>MirDB is a high-performance persistent key-value database built on LSM-tree storage with Memcached protocol compatibility.</p>
</section>
<section>
    <h2>Key Features</h2>
    <ul>
        <li>Memcached protocol compatibility</li>
        <li>LSM-tree based persistent storage</li>
        <li>High performance with skip-list memtable</li>
        <li>Background compaction</li>
    </ul>
</section>
<section>
    <h2>Get Started</h2>
    <p>Check out the <a href="/quick-start">Quick Start Guide</a> to begin using MirDB.</p>
</section>
"#;
    layout("MirDB - Persistent Key-Value Store", content)
}
