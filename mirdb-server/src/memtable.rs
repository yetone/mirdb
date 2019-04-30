use std::borrow::Borrow;
use std::path::Path;

use skip_list::SkipList;
use skip_list::SkipListIter;
use sstable::TableBuilder;
use sstable::TableReader;

use crate::error::MyResult;
use crate::options::Options;
use crate::slice::Slice;
use crate::sstable_builder::skiplist_to_sstable;
use crate::types::Table;

#[derive(Clone)]
pub struct Memtable<K: Ord + Clone, V: Clone> {
    max_size_: usize,
    size_: usize,
    map_: SkipList<K, V>,
}

impl<K: Ord + Clone, V: Clone> Memtable<K, V> {
    pub fn new(max_size: usize, max_height: usize) -> Self {
        let map = SkipList::new(max_height);
        Memtable {
            max_size_: max_size,
            size_: 0,
            map_: map,
        }
    }

    pub fn iter(&self) -> SkipListIter<K, V> {
        self.map_.iter()
    }

    pub fn length(&self) -> usize {
        self.map_.length()
    }
}

impl Memtable<Slice, Slice> {
    pub fn build_sstable(
        &self,
        opt: &Options,
        path: &Path,
    ) -> MyResult<Option<(String, TableReader)>> {
        skiplist_to_sstable(&self.map_, opt, path)
    }
}

impl<K: Ord + Clone, V: Clone> Table<K, V> for Memtable<K, V> {
    fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.map_.get(k)
    }

    fn get_mut<Q: ?Sized>(&self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.map_.get_mut(k)
    }

    fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.map_.insert(k, v)
    }

    fn clear(&mut self) {
        self.size_ = 0;
        self.map_.clear()
    }

    #[inline]
    fn is_full(&self) -> bool {
        false
    }

    fn size(&self) -> usize {
        unimplemented!()
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
        table.insert(1, 5);
        table.insert(1, 6);
        table.insert(1, 7);
        table.insert(2, 2);
        table.insert(3, 3);
        assert_eq!(Some(&7), table.get(&1));
        assert_eq!(Some(&2), table.get(&2));
        assert_eq!(Some(&3), table.get(&3));
        let mut table = Memtable::new(::std::mem::size_of_val(&1) * 6, 10);
        table.insert(Slice::from("b"), Slice::from("b"));
        table.insert(Slice::from("a"), Slice::from("a"));
        table.insert(Slice::from("c"), Slice::from("c"));
        table.insert(Slice::from("a"), Slice::from("d"));
        assert_eq!(Some(&Slice::from("d")), table.get(&Slice::from("a")));
        assert_eq!(Some(&Slice::from("b")), table.get(&Slice::from("b")));
        assert_eq!(Some(&Slice::from("c")), table.get(&Slice::from("c")));
    }
}
