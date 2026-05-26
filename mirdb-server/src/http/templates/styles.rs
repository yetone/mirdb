pub fn global_styles() -> &'static str {
    r#"
body { font-family: -system-ui, sans-serif; margin: 0; padding: 0; }
header { background: #333; color: white; padding: 1rem; }
nav a { color: white; margin-right: 1rem; text-decoration: none; }
nav a:hover { text-decoration: underline; }
main { padding: 2rem; max-width: 800px; margin: 0 auto; }
footer { background: #f5f5f5; padding: 1rem; text-align: center; margin-top: 2rem; }
@media (max-width: 600px) { main { padding: 1rem; } }
"#
}
