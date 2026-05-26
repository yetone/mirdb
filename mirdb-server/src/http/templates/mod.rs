pub mod docs_about;
pub mod homepage;
pub mod quick_start;
pub mod status_page;
pub mod styles;

pub fn layout(title: &str, content: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{}</title>
<style>
{}</style>
</head>
<body>
<header>
<nav>
<a href="/">Home</a>
<a href="/quick-start">Quick Start</a>
<a href="/status">Status</a>
<a href="/docs">Docs</a>
<a href="/about">About</a>
</nav>
</header>
<main>
{}
</main>
<footer>
<p>MirDB - A persistent key-value store</p>
</footer>
</body>
</html>"#,
        title,
        styles::global_styles(),
        content
    )
}
