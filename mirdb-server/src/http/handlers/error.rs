/**
 * Error handlers.
 * Owner: Scenario 11 - Error Handling
 *
 * Expected exports:
 * - fn handle_404() -> HttpResponse
 *   Returns HTTP 404 with a user-friendly HTML error page.
 * - fn handle_405() -> HttpResponse
 *   Returns HTTP 405 Method Not Allowed.
 *
 * Dependencies:
 * - response::not_found_html()
 */

use crate::http::response::{not_found_html, HttpResponse};

pub fn handle_404() -> HttpResponse {
    let html = crate::http::templates::layout(
        "404 - Not Found",
        "<h1>404 - Page Not Found</h1><p>The requested page could not be found.</p>",
    );
    not_found_html(&html)
}

pub fn handle_405() -> HttpResponse {
    HttpResponse {
        status: 405,
        headers: std::collections::HashMap::new(),
        body: b"Method Not Allowed".to_vec(),
    }
}
