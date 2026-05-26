use std::sync::Arc;

use crate::http::response::{ok_html, HttpResponse};
use crate::store::Store;

pub fn handle(store: Arc<Store>) -> HttpResponse {
    let stats = store.stats();
    ok_html(&super::super::templates::status_page::render(&stats))
}
