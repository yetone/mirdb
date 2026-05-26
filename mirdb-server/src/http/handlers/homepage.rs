/**
 * Homepage route handler.
 * Owner: Scenario 3 - Homepage Route and Content
 *
 * Expected exports:
 * - fn handle() -> HttpResponse
 *   Returns HTTP 200 with the homepage HTML content.
 *
 * Dependencies:
 * - templates::homepage::render()
 * - response::ok_html()
 */

use crate::http::response::{ok_html, HttpResponse};

pub fn handle() -> HttpResponse {
    let html = crate::http::templates::homepage::render();
    ok_html(&html)
}
