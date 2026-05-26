/**
 * Documentation and about page handlers.
 * Owner: Scenario 7 - Documentation and About Pages
 *
 * Expected exports:
 * - fn handle_docs() -> HttpResponse
 *   Returns HTTP 200 with documentation index HTML.
 * - fn handle_about() -> HttpResponse
 *   Returns HTTP 200 with about page HTML.
 *
 * Dependencies:
 * - templates::docs_about::render_docs()
 * - templates::docs_about::render_about()
 * - response::ok_html()
 */

use crate::http::response::{ok_html, HttpResponse};

pub fn handle_docs() -> HttpResponse {
    let html = crate::http::templates::docs_about::render_docs();
    ok_html(&html)
}

pub fn handle_about() -> HttpResponse {
    let html = crate::http::templates::docs_about::render_about();
    ok_html(&html)
}
