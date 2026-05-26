use super::layout;

pub fn render_docs() -> String {
    layout("MirDB - Documentation", r#"
<h1>Documentation</h1>
<p>Welcome to the MirDB documentation.</p>
"#)
}

pub fn render_about() -> String {
    layout("MirDB - About", r#"
<h1>About MirDB</h1>
<p>A persistent key-value store written in Rust.</p>
<p><a href="https://github.com/yetone/mirdb">GitHub</a></p>
"#)
}
