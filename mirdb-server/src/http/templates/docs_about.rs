/**
 * Documentation and about page HTML templates.
 * Owner: Scenario 7 - Documentation and About Pages
 *
 * Expected exports:
 * - fn render_docs() -> String
 *   Returns the documentation index HTML.
 * - fn render_about() -> String
 *   Returns the about page HTML with project info.
 *
 * Content requirements:
 * - Docs: Table of contents, links to sections
 * - About: Project description, author info, license, GitHub link
 */

use super::layout;

pub fn render_docs() -> String {
    let content = r#"
<h1>Documentation</h1>
<section>
    <h2>Table of Contents</h2>
    <ul>
        <li><a href="/quick-start">Quick Start Guide</a></li>
        <li><a href="/status">Server Status</a></li>
        <li><a href="/about">About MirDB</a></li>
    </ul>
</section>
"#;
    layout("Documentation - MirDB", content)
}

pub fn render_about() -> String {
    let content = r#"
<h1>About MirDB</h1>
<section>
    <h2>Project Information</h2>
    <p>MirDB is a persistent key-value store built in Rust.</p>
    <p>Author: yetone &lt;yetoneful@gmail.com&gt;</p>
    <p>License: MIT</p>
    <p><a href="https://github.com/yetone/mirdb">GitHub Repository</a></p>
</section>
"#;
    layout("About - MirDB", content)
}
