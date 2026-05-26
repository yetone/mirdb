/**
 * Quick start guide HTML template.
 * Owner: Scenario 4 - Quick Start Guide Page
 *
 * Expected exports:
 * - fn render() -> String
 *   Returns the complete quick start HTML string wrapped in the shared layout.
 *
 * Content requirements:
 * - Installation instructions (cargo install, build from source)
 * - Configuration file example (mirdb.toml)
 * - Basic operations: set, get, delete with code examples
 * - Link to full documentation (/docs)
 */

use super::layout;

pub fn render() -> String {
    let content = r#"
<h1>Quick Start Guide</h1>
<section>
    <h2>Installation</h2>
    <p>Build from source using Cargo:</p>
    <pre><code>cargo build --release</code></pre>
</section>
<section>
    <h2>Configuration</h2>
    <p>Create a configuration file:</p>
    <pre><code>addr = "0.0.0.0:11211"
work_dir = "/tmp/mirdb"</code></pre>
</section>
<section>
    <h2>Basic Operations</h2>
    <p>Use any Memcached client to interact with MirDB:</p>
    <pre><code>set mykey 0 0 5
hello
get mykey
delete mykey</code></pre>
</section>
"#;
    layout("Quick Start - MirDB", content)
}
