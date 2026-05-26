use crate::http::response::{ok_html, HttpResponse};

pub fn handle_docs() -> HttpResponse {
    ok_html(&super::super::templates::docs_about::render_docs())
}

pub fn handle_about() -> HttpResponse {
    ok_html(&super::super::templates::docs_about::render_about())
}
