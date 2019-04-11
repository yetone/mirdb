use crate::memtable::Memtable;
use crate::types::Table;
use std::borrow::Borrow;
use crate::error::MyResult;
use crate::options::Options;

#[derive(Clone)]
pub struct MemtableList<K: Ord + Clone, V: Clone> {
    max_table_count_: usize,
    per_table_max_size_: usize,
    per_table_max_height_: usize,
    tables_: Vec<Memtable<K, V>>,
    opt_: Options,
}

impl<K: Ord + Clone, V: Clone> MemtableList<K, V> {
    pub fn new(opt: Options, max_table_count: usize, per_table_max_size: usize, per_table_max_height: usize) -> Self {
        let tables_ = Vec::with_capacity(max_table_count);
        MemtableList {
            max_table_count_: max_table_count,
            per_table_max_size_: per_table_max_size,
            per_table_max_height_: per_table_max_height,
            opt_: opt,
            tables_,
        }
    }

    pub fn push(&mut self, table: Memtable<K, V>) {
        self.tables_.push(table);
    }

    pub fn iter(&mut self) -> MemtableListIter<K, V> {
        MemtableListIter::new(&self.tables_)
    }
}

impl<K: Ord + Clone, V: Clone> Table<K, V> for MemtableList<K, V> {
    fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
        where K: Borrow<Q>,
              Q: Ord {
        for table in &self.tables_ {
            let r = table.get(k);
            if r.is_some() {
                return r;
            }
        }
        None
    }

    fn get_mut<Q: ?Sized>(&self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Ord {
        for table in &self.tables_ {
            let r = table.get_mut(k);
            if r.is_some() {
                return r;
            }
        }
        None
    }

    fn insert(&mut self, k: K, v: V) -> Option<V> {
        assert!(!self.is_full());

        if self.tables_.len() == 0 {
            self.tables_.push(Memtable::new(self.per_table_max_size_, self.per_table_max_height_));
        }

        for table in &mut self.tables_ {
            if !table.is_full() {
                return table.insert(k, v)
            }
        }

        assert!(false, "not access here!");
        None
    }

    fn clear(&mut self) {
        for table in &mut self.tables_ {
            table.clear();
        }
        self.tables_ = Vec::with_capacity(self.max_table_count_);
    }

    fn is_full(&self) -> bool {
        self.tables_.len() == self.max_table_count_ && self.tables_.iter().all(|x| x.is_full())
    }

    fn size(&self) -> usize {
        self.tables_.iter().map(|x| x.size()).sum()
    }
}

pub struct MemtableListIter<'a, K: Ord + Clone, V: Clone> {
    current_: usize,
    tables_: &'a Vec<Memtable<K, V>>,
}

impl<'a, K: Ord + Clone, V: Clone> MemtableListIter<'a, K, V> {
    pub fn new(tables: &'a Vec<Memtable<K, V>>) -> Self {
        MemtableListIter {
            current_: 0,
            tables_: tables,
        }
    }
}

impl<'a, K: Ord + Clone, V: Clone> Iterator for MemtableListIter<'a, K, V> {
    type Item = &'a Memtable<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ >= self.tables_.len() {
            None
        } else {
            let v = &self.tables_[self.current_];
            self.current_ += 1;
            Some(v)
        }
    }
}