use crate::memtable::Memtable;
use std::borrow::Borrow;

pub struct DataManager<K: Ord + Clone, V: Clone> {
    mutable: Memtable<K, V>,
    immutable: Option<Memtable<K, V>>,
}

unsafe impl<K: Ord + Clone, V: Clone> Sync for DataManager<K, V> {}
unsafe impl<K: Ord + Clone, V: Clone> Send for DataManager<K, V> {}

impl<K: Ord + Clone, V: Clone> DataManager<K, V> {
    pub fn new(max_height: usize, memtable_size: usize) -> Self {
        DataManager {
            mutable: Memtable::new(memtable_size, max_height),
            immutable: None,
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let r = self.mutable.insert(k, v);
        if self.mutable.is_full() {
            if let Some(_immutable) = &self.immutable {
                // TODO:: to sstable
            } else {
                self.immutable = Some(self.mutable.clone());
            }
            self.mutable.clear();
        }
        r
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
        where K: Borrow<Q>,
              Q: Ord {
        let mut r = self.mutable.get(k);
        if r.is_none()  {
            if let Some(immutable) = &self.immutable {
                r = immutable.get(k);
            }
        }
        r
    }

    pub fn remove<Q: ?Sized>(&self, k: &Q) -> Option<&V>
        where K: Borrow<Q>,
              Q: Ord {
        let r = self.get(k);
        if !r.is_none() {
            // TODO: dummy removed
        }
        r
    }
}