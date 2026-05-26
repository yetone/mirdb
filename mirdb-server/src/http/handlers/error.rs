use crate::http::response::{not_found_html, HttpResponse};

pub fn handle_404() -> HttpResponse {
    not_found_html(r#"
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>404 - Not Found</title></head>
<body><h1>404 - Not Found</h1><p>The requested page was not found.</p></body>
</html>
"#)
}

pub fn handle_405() -> HttpResponse {
    HttpResponse::new(405, b"Method Not Allowed".to_vec())
        .with_header("Content-Type", "text/plain")
}
