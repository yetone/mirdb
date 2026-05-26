use super::layout;

pub fn render(stats: &super::super::super::store::ServerStats) -> String {
    layout("MirDB - Status", &format!(
        r#"
<h1>Server Status</h1>
<div class="card">
<h2>Server Info</h2>
<p>Version: {}</p>
<p>Uptime: {} seconds</p>
<p>Keys Stored: {}</p>
<p>Memory Usage: {} bytes</p>
</div>
<div class="card">
<h2>Configuration</h2>
<p>Data Directory: {}</p>
<p>Memcached Port: {}</p>
<p>HTTP Port: {}</p>
</div>
"#,
        stats.version,
        stats.uptime_seconds,
        stats.keys_stored,
        stats.memory_usage_bytes,
        stats.config.data_dir,
        stats.config.memcached_port,
        stats.config.http_port,
    ))
}
