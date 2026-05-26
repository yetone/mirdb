/**
 * Quick start guide route handler.
 * Owner: Scenario 4 - Quick Start Guide Page
 *
 * Expected exports:
 * - fn handle() -> HttpResponse
 *   Returns HTTP 200 with the quick start guide HTML.
 *
 * Dependencies:
 * - templates::quick_start::render()
 * - response::ok_html()
 */

use crate::http::response::{ok_html, HttpResponse};

pub fn handle() -> HttpResponse {
    let html = crate::http::templates::quick_start::render();
    ok_html(&html)
}
