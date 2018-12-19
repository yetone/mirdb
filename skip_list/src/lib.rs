#![feature(box_syntax, type_ascription, nll)]
#![allow(dead_code)]

use rand::prelude::*;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem;
use std::ptr;

fn random_level(max_level: usize) -> usize {
    let mut rng = rand::thread_rng();
    let mut l = 0;
    while rng.gen_range::<usize, usize, usize>(0, 101) > 0 && l < max_level {
        l += 1;
    }
    l
}

fn from_raw_mut<'a, T>(p: *mut T) -> Option<&'a mut T> {
    if p.is_null() {
        None
    } else {
        unsafe {
            Some(&mut *p)
        }
    }
}

fn from_raw<'a, T>(p: *mut T) -> Option<&'a T> {
    if p.is_null() {
        None
    } else {
        unsafe {
            Some(&*p)
        }
    }
}

#[derive(Debug)]
struct SkipListNode<K, V> {
    nexts: Vec<*mut SkipListNode<K, V>>,
    key: K,
    value: V,
    level: usize,
}

impl<K: PartialOrd + Debug, V: Debug> SkipListNode<K, V> {
    fn from_raw_mut<'a>(node_ptr: *mut SkipListNode<K, V>) -> Option<&'a mut SkipListNode<K, V>> {
        from_raw_mut(node_ptr)
    }

    fn from_raw<'a>(node_ptr: *mut SkipListNode<K, V>) -> Option<&'a SkipListNode<K, V>> {
        from_raw(node_ptr)
    }

    fn allocate(key: K, value: V, level: usize) -> *mut SkipListNode<K, V> {
        Box::into_raw(box SkipListNode {
            nexts: vec![],
            key,
            value,
            level,
        })
    }

    fn free(node_ptr: *mut SkipListNode<K, V>) {
        unsafe {
            Box::from_raw(node_ptr);
        }
    }

    fn replace_value(&mut self, value: V) -> V {
        mem::replace(&mut self.value, value)
    }
}

pub struct LevelGenerator(Box<dyn Fn(usize) -> usize>);

impl Debug for LevelGenerator {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "<level_generator>")
    }
}

#[derive(Debug)]
pub struct SkipList<K, V> {
    head: *mut SkipListNode<K, V>,
    max_level: usize,
    level_generator: LevelGenerator
}

impl<K: PartialOrd + Debug, V: Debug> SkipList<K, V> {
    fn new(max_level: usize) -> Self {
        Self::new_with_new_level(max_level, box random_level)
    }

    fn new_with_new_level(max_level: usize, level_generator: Box<dyn Fn(usize) -> usize>) -> Self {
        SkipList{
            head: ptr::null_mut(),
            max_level,
            level_generator: LevelGenerator(level_generator)
        }
    }

    fn head(&self) -> Option<&mut SkipListNode<K, V>> {
        match from_raw_mut(self.head) {
            None => None,
            Some(node) => Some(node)
        }
    }

    fn level(&self) -> usize {
        match self.head() {
            None => 0,
            Some(node) => node.level
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        println!("get {:?}", key);
        match self.head() {
            None => None,
            Some(head) => {
                if head.key > *key {
                    return None;
                }
                if head.key == *key {
                    return Some(&head.value);
                }

                let mut node = head;

                while node.nexts.len() > 0 {
                    println!("node: {:?}", node.key);
                    println!("node len: {}", node.nexts.len());
                    for next_ptr in &node.nexts {
                        match SkipListNode::from_raw_mut(*next_ptr) {
                            Some(next) => {
                                if next.key < *key {
                                    node = next;
                                    break;
                                }
                            }
                            _ => ()
                        };
                    }
                    break;
                }

                if node.key == *key {
                    return Some(&node.value);
                }

                for next_ptr in &node.nexts {
                    match SkipListNode::from_raw_mut(*next_ptr) {
                        Some(next) => {
                            if next.key == *key {
                                return Some(&next.value);
                            }
                        }
                        _ => ()
                    }
                }

                None
            }
        }
    }

    fn insert(&mut self, key: K, value: V) {
        match self.head() {
            None => {
                self.head = SkipListNode::allocate(
                    key, value, 0
                );
            }
            Some(head) => {
                if head.key == key {
                    head.value = value;
                    return;
                }

                if head.key > key {
                    let new_node_ptr = SkipListNode::allocate(
                        key, value, 0
                    );
                    let new_node = SkipListNode::from_raw_mut(new_node_ptr).unwrap();
                    new_node.nexts.push(self.head);
                    self.head = new_node_ptr;
                    return;
                }

                if head.nexts.len() == 0 {
                    let new_node_ptr = SkipListNode::allocate(
                        key, value, 0
                    );
                    let new_node = SkipListNode::from_raw_mut(new_node_ptr).unwrap();
                    if head.key > new_node.key {
                        new_node.nexts.push(self.head);
                        self.head = new_node_ptr;
                        return;
                    }
                    head.nexts.push(new_node_ptr);
                    return;
                }

                let mut level = (self.level_generator.0)(self.max_level);

                if level > head.level {
                    level = head.level + 1;
                }

                let mut updates = self.get_updates(&key, level);

                if level > head.level {
                    updates.push(head);
                    head.level += 1;
                }

                for update_ptr in &updates {
                    let update = SkipListNode::from_raw_mut(*update_ptr).unwrap();
                    for next_ptr in &update.nexts {
                        let next = SkipListNode::from_raw_mut(*next_ptr).unwrap();
                        if next.key == key {
                            next.replace_value(value);
                            return;
                        }
                    }
                }

                let new_node_ptr = SkipListNode::allocate(
                    key, value, level
                );

                let new_node = SkipListNode::from_raw(new_node_ptr).unwrap();

                for update_ptr in updates {
                    let update = SkipListNode::from_raw_mut(update_ptr).unwrap();
                    let mut idx = None;
                    for (i, next_ptr) in update.nexts.iter().enumerate() {
                        let next = SkipListNode::from_raw_mut(*next_ptr).unwrap();
                        if next.key < new_node.key {
                            idx = Some(i);
                            break;
                        }
                    }

                    if update.nexts.len() == 0 {
                        update.nexts.push(new_node_ptr);
                        continue;
                    } else {
                        let idx = match idx {
                            None => update.nexts.len() - 1,
                            Some(idx) => idx
                        };
                        let next_ptr = update.nexts[idx];
                        let next = SkipListNode::from_raw_mut(next_ptr).unwrap();
                        if next.level != level {
                            update.nexts.insert(idx, new_node_ptr);
                        }
                    }
                }
            }
        }
    }

    fn get_updates(&self, key: &K, level: usize) -> Vec<*mut SkipListNode<K, V>> {
        let mut updates = Vec::with_capacity(self.max_level);

        match self.head() {
            None => {
                return updates;
            }
            Some(head) => {
                if head.key >= *key {
                    return updates;
                }

                let mut node_ptr = self.head;

                loop {
                    let node = SkipListNode::from_raw_mut(node_ptr).unwrap();

                    if node.nexts.len() == 0 {
                        updates.push(node_ptr);
                        break;
                    }

                    if SkipListNode::from_raw_mut(node.nexts[node.nexts.len() - 1]).unwrap().key >= *key {
                        updates.push(node_ptr);
                        break;
                    }

                    let mut found = false;

                    for next_ptr in &node.nexts {
                        match SkipListNode::from_raw_mut(*next_ptr) {
                            None => (),
                            Some(next) => {
                                if next.key < *key {
                                    if found && next.level <= level {
                                        updates.push(node_ptr);
                                    }
                                    node_ptr = *next_ptr;
                                    break;
                                }
                                found = true;
                            }
                        }
                    }
                }

                updates
            }
        }
    }

    fn merge_nexts<'a, 'b>(&'a self, node: &'b mut SkipListNode<K, V>, node0: &'b mut SkipListNode<K, V>) {
        node.level = node0.level;
        if node.nexts.len() == 0 {
            node.nexts = node0.nexts.clone();
            return;
        }
        let last_ptr = node.nexts.pop().unwrap();
        let last = SkipListNode::from_raw_mut(last_ptr).unwrap();
        node.nexts = Vec::with_capacity(node0.nexts.len() + 1);
        for next_ptr0 in &node0.nexts {
            match SkipListNode::from_raw_mut(*next_ptr0) {
                None => (),
                Some(next0) => {
                    if next0.key != last.key {
                        node.nexts.push(*next_ptr0);
                    }
                }
            }
        }
        node.nexts.push(last_ptr);
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        match self.head() {
            None => return None,
            _ => ()
        };
        let head = self.head().unwrap();
        if head.key > *key {
            return None;
        }
        if head.key == *key {
            let old_value = head.replace_value(unsafe { mem::uninitialized() });
            if head.nexts.len() != 0 {
                match head.nexts.pop() {
                    None => return None,
                    Some(new_head_ptr) => {
                        match SkipListNode::from_raw_mut(new_head_ptr) {
                            None => return None,
                            Some(new_head) => {
                                self.merge_nexts(new_head, head);
                                self.head = new_head_ptr;
                            }
                        }
                    }
                };
            }
            SkipListNode::free(self.head);
            self.head = ptr::null_mut();
            return Some(old_value);
        }

        let updates = self.get_updates(key, head.level + 1);

        let mut deleted_ptr = None;

        for update_ptr in updates {
            let update = SkipListNode::from_raw_mut(update_ptr).unwrap();

            if update.nexts.len() == 0 {
                continue;
            }

            let mut idx = None;

            for (i, next_ptr) in update.nexts.iter().enumerate() {
                match SkipListNode::from_raw_mut(*next_ptr) {
                    None => (),
                    Some(next) => {
                        if next.key == *key {
                            idx = Some(i);
                            break;
                        }
                        if next.key < *key {
                            break;
                        }
                    }
                }
            }

            let node_ptr = match idx {
                None => continue,
                Some(idx) => update.nexts.remove(idx)
            };

            deleted_ptr = Some(node_ptr);

            match SkipListNode::from_raw_mut(node_ptr) {
                None => (),
                Some(node) => {
                    self.merge_nexts(update, node);
                    if update.level > 0 {
                        update.level -= 1;
                    }
                }
            }
        }

        if let Some(deleted_ptr) = deleted_ptr {
            let deleted = SkipListNode::from_raw_mut(deleted_ptr).unwrap();
            let old_value = deleted.replace_value(unsafe { mem::uninitialized() });
            SkipListNode::free(deleted_ptr);
            return Some(old_value);
        } else {
            None
        }

    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_set_less() {
        let mut l: SkipList<i32, i32> = SkipList::new(10);
        println!("1");
        let key = 10;
        let value = 233;
        l.insert(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        println!("2");
        assert!(l.get(&(key - 1)).is_none());
        println!("3");
        assert_eq!(l.head().unwrap().key, key);
        println!("4");
        let key1 = key - 1;
        let value1 = value - 1;
        l.insert(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        println!("5");
        assert_eq!(*l.get(&key).unwrap(), value);
        println!("6");
        assert_eq!(l.head().unwrap().key, key1);
        println!("7");
    }

    #[test]
    fn test_set_more() {
        let mut l: SkipList<i32, i32> = SkipList::new(10);
        let key = 10;
        let value = 233;
        l.insert(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        assert_eq!(l.head().unwrap().key, key);
        let key1 = key + 1;
        let value1 = value + 1;
        l.insert(key1, value1);
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
        l.insert(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        let key1 = key - 3;
        let value1 = value - 3;
        l.insert(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key1);
        let key2 = key - 2;
        let value2 = value - 2;
        l.insert(key2, value2);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key2).unwrap(), value2);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert_eq!(l.head().unwrap().key, key1);
        let key3 = key - 1;
        let value3 = value - 1;
        l.insert(key3, value3);
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
        l.insert(key, value);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key).unwrap(), value);
        assert!(l.get(&(key - 1)).is_none());
        assert_eq!(l.head().unwrap().key, key);
        assert_eq!(Some(value), l.remove(&key));
        assert_eq!(None, l.get(&key));
        assert_eq!(None, l.remove(&(key + 1)));

        let key1 = key - 2;
        let value1 = value - 2;
        l.insert(key1, value1);
        println!("l: {:?}", l);
        assert_eq!(*l.get(&key1).unwrap(), value1);
        assert_eq!(l.head().unwrap().key, key1);
    }

    #[test]
    fn test_with_new_level_false() {
        let mut l: SkipList<i32, i32> = SkipList::new_with_new_level(10, box |_| 0);
        let key = 10;
        let value = 233;
        l.insert(key, value);
        assert_eq!(0, l.level());
        let key1 = key + 10;
        let value1 = value + 10;
        l.insert(key1, value1);
        assert_eq!(0, l.level());
        let key2 = key + 1;
        let value2 = value + 1;
        l.insert(key2, value2);
        assert_eq!(0, l.level());
    }

    #[test]
    fn test_with_new_level_true() {
        let mut l: SkipList<i32, i32> = SkipList::new_with_new_level(10, box |_| 10);
        let key = 10;
        let value = 233;
        l.insert(key, value);
        assert_eq!(0, l.level());
        let key1 = key + 10;
        let value1 = value + 10;
        l.insert(key1, value1);
        assert_eq!(0, l.level());
        let key2 = key + 1;
        let value2 = value + 1;
        l.insert(key2, value2);
        assert_eq!(1, l.level());
    }
}
