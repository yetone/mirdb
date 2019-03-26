#![feature(box_syntax, type_ascription)]
#![allow(dead_code)]

use rand::prelude::*;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::ptr;
use std::ops::Drop;
use std::borrow::Borrow;
use std::ptr::NonNull;

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
}

struct NodeWrapper<K, V>(NonNull<SkipListNode<K, V>>);

unsafe impl<K, V> Send for NodeWrapper<K, V> {}

impl<K, V> SkipListNode<K, V> {
    fn from_raw_mut<'a>(node_ptr: *mut SkipListNode<K, V>) -> Option<&'a mut SkipListNode<K, V>> {
        from_raw_mut(node_ptr)
    }

    fn from_raw<'a>(node_ptr: *mut SkipListNode<K, V>) -> Option<&'a SkipListNode<K, V>> {
        from_raw(node_ptr)
    }

    fn allocate_dummy(max_height: usize) -> *mut SkipListNode<K, V> {
        SkipListNode::allocate(
            unsafe { mem::uninitialized() },
            unsafe { mem::uninitialized() },
            max_height
        )
    }

    fn allocate(key: K, value: V, height: usize) -> *mut SkipListNode<K, V> {
        Box::into_raw(box SkipListNode {
            nexts: vec![ptr::null_mut(); height + 1],
            key,
            value,
        })
    }

    fn height(&self) -> usize {
        self.nexts.len() - 1
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

pub trait HeightGenerator {
    fn gen_height(&mut self, max: usize) -> usize;
}

struct GenHeight {
    rng: ThreadRng,
}

unsafe impl Send for GenHeight {}

impl GenHeight {
    fn new() -> Self {
        GenHeight {
            rng: rand::thread_rng()
        }
    }
}

impl HeightGenerator for GenHeight {
    fn gen_height(&mut self, max_height: usize) -> usize {
        let mut l = 0;
        while self.rng.gen_range::<usize, usize, usize>(0, 2) > 0 && l < max_height {
            l += 1;
        }
        l
    }
}

pub struct SkipList<K, V> {
    head: *mut SkipListNode<K, V>,
    length: usize,
    height: usize,
    max_height: usize,
    height_generator: Box<dyn HeightGenerator + Send>
}

unsafe impl<K, V> Sync for SkipList<K, V> {}
unsafe impl<K, V> Send for SkipList<K, V> {}

impl<K: PartialOrd, V> SkipList<K, V> {
    pub fn new(max_height: usize) -> Self {
        Self::new_with_new_height(max_height, box GenHeight::new())
    }

    fn new_with_new_height(max_height: usize, height_generator: Box<dyn HeightGenerator + Send>) -> Self {
        SkipList{
            head: SkipListNode::allocate_dummy(max_height),
            length: 0,
            height: 0,
            max_height,
            height_generator
        }
    }

    fn head(&self) -> Option<&mut SkipListNode<K, V>> {
        match from_raw_mut(self.head) {
            None => None,
            Some(node) => Some(node)
        }
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where K: Borrow<Q>,
          Q: PartialOrd {
        let updates = self.get_updates(key);

        for update_ptr in updates {
            if let Some(update) = SkipListNode::from_raw(update_ptr) {
                for next_ptr in &update.nexts {
                    if let Some(next) = SkipListNode::from_raw(*next_ptr) {
                        if *next.key.borrow() == *key {
                            return Some(&next.value);
                        }
                    }
                }
            }
        }

        None
    }

    pub fn get_mut<Q: ?Sized>(&self, key: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: PartialOrd {
        let updates = self.get_updates(key);

        for update_ptr in updates {
            if let Some(update) = SkipListNode::from_raw(update_ptr) {
                for next_ptr in &update.nexts {
                    if let Some(next) = SkipListNode::from_raw_mut(*next_ptr) {
                        if *next.key.borrow() == *key {
                            return Some(&mut next.value);
                        }
                    }
                }
            }
        }

        None
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let updates = self.get_updates(&key);

        for update_ptr in &updates {
            if let Some(update) = SkipListNode::from_raw(*update_ptr) {
                for next_ptr in &update.nexts {
                    if let Some(next) = SkipListNode::from_raw_mut(*next_ptr) {
                        if next.key == key {
                            let old_value = next.replace_value(value);
                            return Some(old_value);
                        }
                    }
                }
            }
        }

        let height = self.height_generator.gen_height(self.max_height);

        let node_ptr = SkipListNode::allocate(key, value, height);

        for i in 0..=height {
            let update_ptr = updates[self.max_height - i];
            if let Some(update) = SkipListNode::from_raw_mut(update_ptr) {
                let next_ptr = update.nexts[update.height() - i];
                let node = SkipListNode::from_raw_mut(node_ptr).unwrap();
                let i0 = update.height() - i;
                unsafe {
                    *(node.nexts.get_unchecked_mut(height - i)) = next_ptr;
                    *(update.nexts.get_unchecked_mut(i0)) = node_ptr;
                }
            }
        }

        self.height += 1;
        None
    }

    fn get_updates<Q: ?Sized>(&self, key: &Q) -> Vec<*mut SkipListNode<K, V>>
    where K: Borrow<Q>,
          Q: PartialOrd {
        let mut updates = vec![self.head; self.max_height + 1];

        let mut current_ptr = self.head;

        'outer: loop {
            let current = SkipListNode::from_raw(current_ptr).unwrap();

            for next_ptr in &current.nexts {
                if let Some(next) = SkipListNode::from_raw(*next_ptr) {
                    if *next.key.borrow() < *key {
                        for i in 0..=current.height() {
                            updates[self.max_height - i] = current_ptr;
                        }
                        current_ptr = *next_ptr;
                        continue 'outer;
                    }
                }
            }

            for i in 0..=current.height() {
                updates[self.max_height - i] = current_ptr;
            }

            break;
        }

        updates
    }

    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
        where K: Borrow<Q>,
              Q: PartialOrd {

        let updates = self.get_updates(key);

        let mut node_ptr = None;

        let mut patch = vec![];
        for update_ptr in updates {
            if let Some(update) = SkipListNode::from_raw_mut(update_ptr) {
                for (i, next_ptr) in update.nexts.iter().enumerate() {
                    if let Some(next) = SkipListNode::from_raw_mut(*next_ptr) {
                        let next_key = next.key.borrow();
                        if *next_key == *key {
                            let i0 = next.height() + i - update.height();
                            patch.push((i, i0, update_ptr, *next_ptr));
                            node_ptr = Some(*next_ptr);
                        } else if *next_key < *key {
                            break
                        }
                    }
                }

            }
        }

        for (i, i0, update_ptr, next_ptr) in patch {
            if let Some(update) = SkipListNode::from_raw_mut(update_ptr) {
                if let Some(next) = SkipListNode::from_raw(next_ptr) {
                    unsafe {
                        *(update.nexts.get_unchecked_mut(i)) = next.nexts[i0];
                    }
                }
            }
        }

        if let Some(node_ptr) = node_ptr {
            if let Some(node) = SkipListNode::from_raw_mut(node_ptr) {
                let value = node.replace_value(unsafe { mem::uninitialized() });
                SkipListNode::free(node_ptr);
                self.height -= 1;
                return Some(value);
            }
        }

        None
    }
}

impl<K, V: Display> Display for SkipList<K, V> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "[").unwrap();
        let mut flag = false;
        let mut current_ptr = self.head;
        while let Some(current) = SkipListNode::from_raw(current_ptr) {
            current_ptr = current.nexts[current.height()];
            if let Some(current) = SkipListNode::from_raw(current_ptr) {
                if flag {
                    write!(f, ", ").unwrap();
                } else {
                    flag = true
                }
                write!(f, "{}", current.value).unwrap();
            }
        }
        write!(f, "]")
    }
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        println!("drop");
        // FIXME
    }
}

#[cfg(test)]
mod test {
    use rand::prelude::*;
    use std::collections::HashSet;
    use super::*;

    #[test]
    fn test_drop() {
        let mut map = SkipList::new(10);
        map.insert(1, 2);
        map.insert(1, 2);
        drop(map);
    }

    #[test]
    fn test_random() {
        use std::time;
        let st = time::SystemTime::now();
        let n = 100000;
        let mut rng = rand::thread_rng();
        let mut seen = HashSet::with_capacity(n);
        let mut kvs = Vec::with_capacity(n);
        for _ in 0..=n {
            let k = rng.gen_range::<usize, usize, usize>(0, n + 1);
            if seen.contains(&k) {
                continue;
            }
            let v = rng.gen_range::<usize, usize, usize>(0, n + 1);
            kvs.push((k, v));
            seen.insert(k);
        }
        println!("kvs: {}", kvs.len());
        let mut l = SkipList::new(10);
        for (k, v) in &kvs {
            l.insert(*k, *v);
        }
        println!("inserted: {}", kvs.len());
        for (k, v) in &kvs {
            assert_eq!(Some(v), l.get(k));
        }
        println!("getted: {}", kvs.len());
        for (k, v) in &kvs {
            assert_eq!(Some(*v), l.remove(k));
        }
        println!("removed: {}", kvs.len());
        for (k, _) in &kvs {
            assert_eq!(None, l.get(k));
        }
        println!("cost: {}ms", st.elapsed().unwrap().as_millis());
    }
}
