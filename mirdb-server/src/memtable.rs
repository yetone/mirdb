use skip_list::SkipList;
use std::borrow::Borrow;

#[derive(Clone)]
pub struct Memtable<K: Ord + Clone, V: Clone> {
    max_size: usize,
    map: SkipList<K, V>,
}

impl<K: Ord + Clone, V: Clone> Memtable<K, V> {
    pub fn new(max_size: usize, max_height: usize) -> Self {
        let map = SkipList::new(max_height);
        Memtable {
            max_size, map
        }
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
        where K: Borrow<Q>,
              Q: Ord {
        self.map.get(k)
    }

    pub fn get_mut<Q: ?Sized>(&self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Ord {
        self.map.get_mut(k)
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.map.insert(k, v)
    }

    pub fn clear(&mut self) {
        self.map.clear()
    }

    pub fn is_full(&self) -> bool {
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
    }
}
