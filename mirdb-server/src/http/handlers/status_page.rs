/**
 * Status dashboard page handler.
 * Owner: Scenario 5 - Status Dashboard Page
 *
 * Expected exports:
 * - fn handle(store: Arc<Store>) -> HttpResponse
 *   Returns HTTP 200 with the status dashboard HTML.
 *
 * Dependencies:
 * - templates::status_page::render(stats)
 * - response::ok_html()
 * - store::stats() for retrieving metrics
 */

use std::sync::Arc;

use crate::http::response::{ok_html, HttpResponse};
use crate::store::Store;

pub fn handle(_store: Arc<Store>) -> HttpResponse {
    let html = crate::http::templates::status_page::render();
    ok_html(&html)
}
