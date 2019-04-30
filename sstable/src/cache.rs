use lru::LruCache;

pub type CacheKey = [u8; 16];
pub type CacheID = u64;

pub struct Cache<T> {
    inner: LruCache<CacheKey, T>,
    id: u64,
}

impl<T> Cache<T> {
    pub fn new(capacity: usize) -> Cache<T> {
        assert!(capacity > 0);
        Cache {
            inner: LruCache::new(capacity),
            id: 0,
        }
    }

    pub fn new_cache_id(&mut self) -> CacheID {
        self.id += 1;
        self.id
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn cap(&self) -> usize {
        self.inner.cap()
    }

    pub fn insert(&mut self, key: CacheKey, elem: T) {
        self.inner.put(key, elem)
    }

    pub fn get(&mut self, key: &CacheKey) -> Option<&T> {
        self.inner.get(key)
    }
}
