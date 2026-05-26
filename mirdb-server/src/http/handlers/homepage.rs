use crate::http::response::{ok_html, HttpResponse};

pub fn handle() -> HttpResponse {
    ok_html(&super::super::templates::homepage::render())
}
