use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::http::response::{ok_json, HttpResponse};
use crate::store::Store;

struct CachedStatus {
    json: String,
    timestamp: Instant,
}

static STATUS_CACHE: RwLock<Option<CachedStatus>> = RwLock::new(None);
const CACHE_TTL: Duration = Duration::from_secs(1);

pub fn handle(store: Arc<Store>) -> HttpResponse {
    {
        if let Ok(cache) = STATUS_CACHE.read() {
            if let Some(ref cached) = *cache {
                if cached.timestamp.elapsed() < CACHE_TTL {
                    return ok_json(&cached.json)
                        .with_header("X-Cache", "HIT");
                }
            }
        }
    }

    let stats = store.stats();
    let json = match serde_json::to_string(&stats) {
        Ok(j) => j,
        Err(_) => return HttpResponse::new(500, b"Internal Server Error".to_vec()),
    };

    {
        if let Ok(mut cache) = STATUS_CACHE.write() {
            *cache = Some(CachedStatus {
                json: json.clone(),
                timestamp: Instant::now(),
            });
        }
    }

    ok_json(&json)
        .with_header("X-Cache", "MISS")
}

pub fn clear_cache() {
    if let Ok(mut cache) = STATUS_CACHE.write() {
        *cache = None;
    }
}
