use std::sync::Arc;

use crate::http::handlers;
use crate::http::response::HttpResponse;
use crate::store::Store;

pub fn dispatch(path: &str, _method: &str, store: Arc<Store>) -> HttpResponse {
    match path {
        "/" => handlers::homepage::handle(),
        "/quick-start" => handlers::quick_start::handle(),
        "/status" => handlers::status_page::handle(store),
        "/docs" => handlers::docs_about::handle_docs(),
        "/about" => handlers::docs_about::handle_about(),
        "/api/status" => handlers::api_status::handle(store),
        _ => handlers::error::handle_404(),
    }
}
