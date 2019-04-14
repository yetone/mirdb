use std::borrow::Borrow;
use std::collections::LinkedList;
use std::collections::linked_list;

use crate::error::MyResult;
use crate::memtable::Memtable;
use crate::options::Options;
use crate::types::Table;

#[derive(Clone)]
pub struct MemtableList<K: Ord + Clone, V: Clone> {
    max_table_count_: usize,
    per_table_max_size_: usize,
    per_table_max_height_: usize,
    tables_: LinkedList<Memtable<K, V>>,
    opt_: Options,
}

impl<K: Ord + Clone, V: Clone> MemtableList<K, V> {
    pub fn new(opt: Options, max_table_count: usize, per_table_max_size: usize, per_table_max_height: usize) -> Self {
        let tables_ = LinkedList::new();
        MemtableList {
            max_table_count_: max_table_count,
            per_table_max_size_: per_table_max_size,
            per_table_max_height_: per_table_max_height,
            opt_: opt,
            tables_,
        }
    }

    pub fn add(&mut self, table: Memtable<K, V>) {
        self.tables_.push_front(table);
    }

    pub fn consume(&mut self) -> Option<Memtable<K, V>> {
        self.tables_.pop_back()
    }

    pub fn tables_iter(&self) -> linked_list::Iter<Memtable<K, V>> {
        self.tables_.iter()
    }

    pub fn table_count(&self) -> usize {
        self.tables_.len()
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
            self.tables_.push_back(Memtable::new(self.per_table_max_size_, self.per_table_max_height_));
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
        self.tables_.clear();
    }

    fn is_full(&self) -> bool {
        self.tables_.len() >= self.max_table_count_
    }

    fn size(&self) -> usize {
        unimplemented!()
    }
}
