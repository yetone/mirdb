use skip_list::SkipList;
use std::borrow::Borrow;
use crate::types::Table;
use skip_list::SkipListIter;
use crate::options::Options;
use serde::Serialize;
use std::path::Path;
use crate::error::MyResult;
use sstable::TableReader;
use sstable::TableBuilder;
use bincode::serialize;
use crate::sstable_builder::skiplist_to_sstable;

#[derive(Clone)]
pub struct Memtable<K: Ord + Clone, V: Clone> {
    max_size: usize,
    size_: usize,
    map: SkipList<K, V>,
}

impl<K: Ord + Clone, V: Clone> Memtable<K, V> {
    pub fn new(max_size: usize, max_height: usize) -> Self {
        let map = SkipList::new(max_height);
        Memtable {
            max_size,
            size_: 0,
            map
        }
    }

    pub fn iter(&self) -> SkipListIter<K, V> {
        self.map.iter()
    }
}

impl<K: Ord + Clone + Borrow<[u8]>, V: Clone + Serialize> Memtable<K, Option<V>> {
    pub fn build_sstable(&self, opt: &Options, path: &Path) -> MyResult<Option<(String, TableReader)>> {
        skiplist_to_sstable(&self.map, opt, path)
    }
}

impl<K: Ord + Clone, V: Clone> Table<K, V> for Memtable<K, V> {

    fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
        where K: Borrow<Q>,
              Q: Ord {
        self.map.get(k)
    }

    fn get_mut<Q: ?Sized>(&self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Ord {
        self.map.get_mut(k)
    }

    fn insert(&mut self, k: K, v: V) -> Option<V> {
        let k_size = ::std::mem::size_of_val(&k);
        self.size_ += ::std::mem::size_of_val(&v);
        let r = self.map.insert(k, v);
        if let Some(ref old_v) = r {
            self.size_ -= ::std::mem::size_of_val(old_v);
        } else {
            self.size_ += k_size;
        }
        r
    }

    fn clear(&mut self) {
        self.size_ = 0;
        self.map.clear()
    }

    fn is_full(&self) -> bool {
        self.max_size <= self.size_
    }

    fn size(&self) -> usize {
        self.size_
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_get() {
        let mut table = Memtable::new(::std::mem::size_of_val(&1) * 6, 10);
        table.insert(1, 2);
        table.insert(1, 3);
        table.insert(1, 4);
        assert!(!table.is_full());
        table.insert(1, 5);
        table.insert(1, 6);
        table.insert(1, 7);
        table.insert(2, 2);
        assert!(!table.is_full());
        table.insert(3, 3);
        assert!(table.is_full());
    }
}
