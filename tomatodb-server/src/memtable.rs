use skip_list::SkipList;
use std::borrow::Borrow;
use crate::types::Table;
use skip_list::SkipListIter;

#[derive(Clone)]
pub struct Memtable<K: Ord + Clone, V: Clone> {
    max_size: usize,
    map: SkipList<K, V>,
}

impl<K: Ord + Clone, V: Clone> Memtable<K, V> {
    pub fn new(max_size: usize, max_height: usize) -> Self {
        let map = SkipList::new(max_height);
        Memtable {
            max_size,
            map
        }
    }
    pub fn iter(&self) -> SkipListIter<K, V> {
        self.map.iter()
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
        self.map.insert(k, v)
    }

    fn clear(&mut self) {
        self.map.clear()
    }

    fn is_full(&self) -> bool {
        self.max_size <= self.map.length()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_get() {
        let mut table = Memtable::new(3, 10);
        table.insert(1, 2);
        assert!(!table.is_full());
        table.insert(1, 2);
        table.insert(1, 2);
        table.insert(1, 2);
        assert!(!table.is_full());
        table.insert(2, 2);
        table.insert(3, 2);
        assert!(table.is_full());
    }
}
