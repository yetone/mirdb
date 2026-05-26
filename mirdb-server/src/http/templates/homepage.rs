use super::layout;

pub fn render() -> String {
    layout("MirDB - Home", r#"
<h1>MirDB</h1>
<p>A persistent key-value store with Memcached protocol support.</p>
<h2>Features</h2>
<ul>
<li>Memcached protocol compatibility</li>
<li>LSM-tree based persistence</li>
<li>Skip-list memtable</li>
<li>Write-ahead log for durability</li>
</ul>
<p><a href="/quick-start">Get Started</a></p>
"#)
}
