/**
 * Status dashboard HTML template.
 * Owner: Scenario 5 - Status Dashboard Page
 *
 * Expected exports:
 * - fn render() -> String
 *   Returns the complete status dashboard HTML string wrapped in the shared layout.
 *
 * Content requirements:
 * - Server Status card: version, uptime, PID
 * - Database Statistics card: key count, memory usage
 * - Configuration Summary card: data_dir, memcached_port, http_port
 */

use super::layout;

pub fn render() -> String {
    let content = r#"
<h1>Server Status</h1>
<section>
    <h2>Server Information</h2>
    <p>Version: 0.0.1</p>
    <p>Status: Running</p>
</section>
<section>
    <h2>Database Statistics</h2>
    <p>Keys stored: 0</p>
    <p>Memory usage: 0 bytes</p>
</section>
"#;
    layout("Status - MirDB", content)
}
