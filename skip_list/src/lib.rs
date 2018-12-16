#![feature(box_syntax, type_ascription)]
#![allow(dead_code)]

use rand::prelude::*;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;
use std::ptr;

fn raw_to_mut<'a, T>(p: *mut T) -> Option<&'a mut T> {
    if p.is_null() {
        None
    } else {
        unsafe {
            Some(&mut *p)
        }
    }
}

#[derive(Debug)]
struct SkipListNode<K, V> {
    nexts: Vec<*mut SkipListNode<K, V>>,
    key: K,
    value: V,
}

impl<K: PartialEq, V> PartialEq for SkipListNode<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: PartialOrd, V> PartialOrd for SkipListNode<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K: PartialOrd + Debug, V: Debug> SkipListNode<K, V> {
    fn find(&self, key: &K) -> Option<&Self> {
        if self.key == *key {
            return Some(self);
        }
        for next in &self.nexts {
            let next: Option<&mut SkipListNode<K, V>> = raw_to_mut(*next);
            match next {
                Some(next_) => {
                    if next_.key <= *key {
                        return next_.find(key);
                    }
                }
                _ => ()
            }
        }
        None
    }

    fn find_le_closest_mut(&mut self, key: &K) -> Option<&mut Self> {
        if self.key > *key {
            return None;
        }
        for next in &self.nexts {
            let next: Option<&mut SkipListNode<K, V>> = raw_to_mut(*next);
            match next {
                Some(next_) => {
                    if next_.key <= *key {
                        return next_.find_le_closest_mut(key);
                    }
                }
                _ => ()
            }
        }
        Some(self)
    }

    fn find_lt_closest_mut(&mut self, key: &K) -> Option<&mut Self> {
        if self.key >= *key {
            return None;
        }
        for next in &self.nexts {
            let next: Option<&mut SkipListNode<K, V>> = raw_to_mut(*next);
            match next {
                Some(next_) => {
                    if next_.key < *key {
                        return next_.find_lt_closest_mut(key);
                    }
                }
                _ => ()
            }
        }
        Some(self)
    }

    fn replace_value(&mut self, value: V) -> V {
        mem::replace(&mut self.value, value)
    }
}

#[derive(Debug, PartialEq)]
pub struct SkipList<K, V> {
    head: *mut SkipListNode<K, V>,
    max_level: usize,
    cur_level: usize,
}

impl<K: PartialOrd + Debug, V: Debug> SkipList<K, V> {
    fn new(max_level: usize) -> Self {
        SkipList{ head: ptr::null_mut(), max_level, cur_level: 0 }
    }

    fn head(&self) -> Option<&mut SkipListNode<K, V>> {
        match raw_to_mut(self.head) {
            None => None,
            Some(node) => Some(node)
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        match self.head() {
            None => None,
            Some(node) => {
                match node.find(key) {
                    Some(node) => Some(&node.value),
                    _ => None
                }
            }
        }
    }

    fn set(&mut self, key: K, value: V) {
        let mut new_node = SkipListNode {
            nexts: vec![],
            key,
            value,
        };
        match self.head() {
            None => {
                self.head = Box::into_raw(box new_node);
                return;
            },
            _ => {}
        };
        let head = self.head().unwrap();
        match head.find_le_closest_mut(&new_node.key) {
            Some(node) => {
                if node.key == new_node.key {
                    node.replace_value(new_node.value);
                    return;
                }
                if node.nexts.len() == 0 {
                    node.nexts.push(Box::into_raw(box new_node));
                    return;
                }
                let new_level = self.new_level();
                if new_level && self.cur_level < self.max_level {
                    let last = node.nexts[node.nexts.len() - 1];
                    new_node.nexts.push(last);
                    node.nexts.push(Box::into_raw(box new_node));
                    self.cur_level += 1;
                    return;
                }
                match node.nexts.pop() {
                    Some(last) => {
                        new_node.nexts.push(last);
                        node.nexts.push(Box::into_raw(box new_node));
                    },
                    _ => ()
                }
            },
            _ => {
                new_node.nexts.push(self.head);
                self.head = Box::into_raw(box new_node);
            }
        };
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        match self.head() {
            None => None,
            Some(head) => {
                if head.key == *key {
                    let value = head.replace_value(unsafe { mem::uninitialized() });
                    unsafe {
                        Box::from_raw(self.head);
                    }
                    self.head = ptr::null_mut();
                    return Some(value);
                }
                match head.find_lt_closest_mut(key) {
                    Some(node) => {
                        let mut the_idx = None;
                        for (i, next) in node.nexts.iter().enumerate() {
                            match raw_to_mut(*next): Option<&mut SkipListNode<K, V>> {
                                Some(next) => {
                                    if next.key == *key {
                                        the_idx = Some(i);
                                        continue;
                                    }
                                    match the_idx {
                                        Some(idx) => {
                                            if i == idx + 1 {
                                                next.nexts.pop();
                                                match raw_to_mut(node.nexts[idx]): Option<&mut SkipListNode<K, V>> {
                                                    Some(the_node) => {
                                                        next.nexts.append(&mut the_node.nexts);
                                                    }
                                                    _ => ()
                                                }
                                            }
                                        }
                                        _ => ()
                                    }
                                }
                                _ => ()
                            }
                        }
                        match the_idx {
                            Some(idx) => {
                                let res = match raw_to_mut(node.nexts[idx]): Option<&mut SkipListNode<K, V>> {
                                    Some(the_node) => {
                                        let value = the_node.replace_value(unsafe { mem::uninitialized() });
                                        unsafe {
                                            Box::from_raw(node.nexts[idx]);
                                        }

                                        Some(value)
                                    }
                                    _ => None
                                };
                                node.nexts.remove(idx);
                                return res;
                            }
                            _ => None
                        }
                    }
                    _ => None
                }
            }
        }
    }

    fn new_level(&self) -> bool {
        let mut rng = rand::thread_rng();
        rng.gen_range::<usize, usize, usize>(0, 2) > 0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_set_less() {
        let mut l: SkipList<i32, i32> = SkipList::new(10);
        let key = 10;
        let value = 233;
        l.set(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        println!("l: {:?}", l);
        assert_eq!(l.head().unwrap().key, key);
        let key1 = key - 1;
        let value1 = value - 1;
        l.set(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key1);
    }

    #[test]
    fn test_set_more() {
        let mut l: SkipList<i32, i32> = SkipList::new(10);
        let key = 10;
        let value = 233;
        l.set(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        println!("l: {:?}", l);
        assert_eq!(l.head().unwrap().key, key);
        let key1 = key + 1;
        let value1 = value + 1;
        l.set(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key);
    }

    #[test]
    fn test_set_inner() {
        let mut l: SkipList<i32, i32> = SkipList::new(10);
        let key = 10;
        let value = 233;
        l.set(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        let key1 = key - 3;
        let value1 = value - 3;
        l.set(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key1);
        let key2 = key - 2;
        let value2 = value - 2;
        l.set(key2, value2);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key2).unwrap(), value2);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key1);
        let key3 = key - 1;
        let value3 = value - 1;
        l.set(key3, value3);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key3).unwrap(), value3);
        assert_eq!(*l.get(&key2).unwrap(), value2);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key1);
    }

    #[test]
    fn test_remove() {
        let mut l: SkipList<i32, i32> = SkipList::new(10);
        let key = 10;
        let value = 233;
        l.set(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        assert_eq!(l.head().unwrap().key, key);
        assert_eq!(Some(value), l.remove(&key));
        assert_eq!(None, l.get(&key));
        assert_eq!(None, l.remove(&(key + 1)));

        let key1 = key - 2;
        let value1 = value - 2;
        l.set(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(l.head().unwrap().key, key1);
    }
}
