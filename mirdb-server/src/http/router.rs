/**
 * HTTP route dispatcher.
 * Owner: Scenario 8 - Navigation and Routing
 *
 * Expected exports:
 * - dispatch(req: &Request, store: Arc<Store>) -> Response
 *   Routes incoming HTTP requests to the appropriate handler based on path and method.
 *
 * Routes:
 *   GET /           -> homepage::handle()
 *   GET /quick-start -> quick_start::handle()
 *   GET /status     -> status_page::handle()
 *   GET /docs       -> docs_about::handle_docs()
 *   GET /about      -> docs_about::handle_about()
 *   GET /api/status -> api_status::handle(store)
 *   *   *           -> error::handle_404()
 */

use std::sync::Arc;

use crate::http::handlers;
use crate::http::response::HttpResponse;
use crate::store::Store;

pub fn dispatch(path: &str, _store: Arc<Store>) -> HttpResponse {
    match path {
        "/" => handlers::homepage::handle(),
        "/quick-start" => handlers::quick_start::handle(),
        "/status" => handlers::status_page::handle(_store),
        "/docs" => handlers::docs_about::handle_docs(),
        "/about" => handlers::docs_about::handle_about(),
        "/api/status" => handlers::api_status::handle(_store),
        _ => handlers::error::handle_404(),
    }
}
