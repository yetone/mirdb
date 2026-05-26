use super::layout;

pub fn render() -> String {
    layout("MirDB - Quick Start", r#"
<h1>Quick Start</h1>
<h2>Installation</h2>
<pre>cargo install mirdb</pre>
<h2>Basic Operations</h2>
<pre>
set mykey 0 0 5
hello
get mykey
</pre>
"#)
}
