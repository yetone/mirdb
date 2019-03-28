use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::ops::Drop;
use std::borrow::Borrow;

use crate::node::SkipListNode;
use crate::height_generator::HeightGenerator;
use crate::height_generator::GenHeight;
use crate::iter::SkipListIter;
use crate::iter::SkipListIterMut;

pub struct SkipList<K, V> {
    head_: *mut SkipListNode<K, V>,
    length_: usize,
    height_: usize,
    max_height_: usize,
    height_generator: Box<dyn HeightGenerator + Send>
}

unsafe impl<K, V> Sync for SkipList<K, V> {}
unsafe impl<K, V> Send for SkipList<K, V> {}

impl<K, V> SkipList<K, V> {
    pub fn length(&self) -> usize {
        self.length_
    }

    pub fn height(&self) -> usize {
        self.height_
    }

    pub fn max_height(&self) -> usize {
        self.max_height_
    }

    pub fn head(&self) -> Option<&SkipListNode<K, V>> {
        SkipListNode::from_raw(self.head_)
    }

    pub fn head_mut(&self) -> Option<&mut SkipListNode<K, V>> {
        SkipListNode::from_raw_mut(self.head_)
    }

    pub fn new(max_height: usize) -> Self {
        Self::new_with_height_generator(max_height, box GenHeight::new())
    }

    pub fn new_with_height_generator(max_height: usize, height_generator: Box<dyn HeightGenerator + Send>) -> Self {
        SkipList{
            head_: SkipListNode::allocate_dummy(max_height),
            length_: 0,
            height_: 0,
            max_height_: max_height,
            height_generator
        }
    }

    fn dispose(&mut self) {
        unsafe {
            let mut current = self.head_;
            let mut is_head = true;

            while let Some(next) = (*current).next_mut((*current).height()) {
                if !is_head {
                    SkipListNode::free(current);
                }
                current = next;
                is_head = false;
            }

            if !is_head {
                SkipListNode::free(current);
            }
        }
    }

    pub fn clear(&mut self) {
        self.dispose();
        self.head_ = SkipListNode::allocate_dummy(self.max_height_);
        self.length_ = 0;
        self.height_ = 0;
    }
}

impl<K: Ord, V> SkipList<K, V> {
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
        where K: Borrow<Q>,
              Q: Ord {
        let updates = self.get_updates(key);

        for update_ptr in updates {
            if let Some(update) = SkipListNode::from_raw(update_ptr) {
                for next_ptr in &update.nexts_ {
                    if let Some(next) = SkipListNode::from_raw(*next_ptr) {
                        if *next.key_.borrow() == *key {
                            return Some(&next.value_);
                        }
                    }
                }
            }
        }

        None
    }

    pub fn get_mut<Q: ?Sized>(&self, key: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Ord {
        let updates = self.get_updates(key);

        for update_ptr in updates {
            if let Some(update) = SkipListNode::from_raw(update_ptr) {
                for next_ptr in &update.nexts_ {
                    if let Some(next) = SkipListNode::from_raw_mut(*next_ptr) {
                        if *next.key_.borrow() == *key {
                            return Some(&mut next.value_);
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
                for next_ptr in &update.nexts_ {
                    if let Some(next) = SkipListNode::from_raw_mut(*next_ptr) {
                        if next.key_ == key {
                            let old_value = next.replace_value(value);
                            return Some(old_value);
                        }
                    }
                }
            }
        }

        let height = self.height_generator.gen_height(self.max_height_);

        let node_ptr = SkipListNode::allocate(key, value, height);

        for i in 0..=height {
            let update_ptr = updates[self.max_height_ - i];
            if let Some(update) = SkipListNode::from_raw_mut(update_ptr) {
                let next_ptr = update.nexts_[update.height() - i];
                let node = SkipListNode::from_raw_mut(node_ptr).unwrap();
                let i0 = update.height() - i;
                unsafe {
                    *(node.nexts_.get_unchecked_mut(height - i)) = next_ptr;
                    *(update.nexts_.get_unchecked_mut(i0)) = node_ptr;
                }
            }
        }

        self.height_ = std::cmp::max(self.height_, height);
        self.length_ += 1;
        None
    }

    fn get_updates<Q: ?Sized>(&self, key: &Q) -> Vec<*mut SkipListNode<K, V>>
        where K: Borrow<Q>,
              Q: Ord {
        let mut updates = vec![self.head_; self.max_height_ + 1];

        let mut current_ptr = self.head_;

        'outer: loop {
            let current = SkipListNode::from_raw(current_ptr).unwrap();

            for next_ptr in &current.nexts_ {
                if let Some(next) = SkipListNode::from_raw(*next_ptr) {
                    if *next.key_.borrow() < *key {
                        for i in 0..=current.height() {
                            updates[self.max_height_ - i] = current_ptr;
                        }
                        current_ptr = *next_ptr;
                        continue 'outer;
                    }
                }
            }

            for i in 0..=current.height() {
                updates[self.max_height_ - i] = current_ptr;
            }

            break;
        }

        updates
    }

    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
        where K: Borrow<Q>,
              Q: Ord {

        let updates = self.get_updates(key);

        let mut node_ptr = None;

        let mut patch = vec![];
        for update_ptr in updates {
            if let Some(update) = SkipListNode::from_raw_mut(update_ptr) {
                for (i, next_ptr) in update.nexts_.iter().enumerate() {
                    if let Some(next) = SkipListNode::from_raw_mut(*next_ptr) {
                        let next_key = next.key_.borrow();
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
                        *(update.nexts_.get_unchecked_mut(i)) = next.nexts_[i0];
                    }
                }
            }
        }

        if let Some(node_ptr) = node_ptr {
            if let Some(node) = SkipListNode::from_raw_mut(node_ptr) {
                let value = node.replace_value(unsafe { mem::uninitialized() });
                SkipListNode::free(node_ptr);
                self.length_ -= 1;
                return Some(value);
            }
        }

        None
    }
}

impl<K, V: Display> Display for SkipList<K, V> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "[")?;
        for (i, (_, v)) in self.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", v)?;
        }
        write!(f, "]")
    }
}

impl<K: Ord + Clone, V: Clone> Clone for SkipList<K, V> {
    fn clone(&self) -> Self {
        let mut copied: SkipList<K, V> = SkipList::new(self.max_height_);
        for (k, v) in self.iter() {
            copied.insert(k.clone(), v.clone());
        }
        copied
    }
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        self.dispose();
    }
}

impl<K, V> SkipList<K, V> {
    pub fn iter(&self) -> SkipListIter<K, V> {
        SkipListIter::new(self)
    }

    pub fn iter_mut(&mut self) -> SkipListIterMut<K, V> {
        SkipListIterMut::new(self)
    }
}

#[cfg(test)]
mod test {
    use rand::prelude::*;
    use std::collections::HashSet;
    use super::*;
    use std::cmp::Ordering;
    use std::fmt::Debug;

    #[test]
    fn test_to_string() {
        let mut list = SkipList::new(10);
        for i in 0..3 {
            list.insert(i, i + 1);
        }
        assert_eq!("[1, 2, 3]", list.to_string());
    }

    #[test]
    fn test_drop() {
        struct A<T: Debug>(T);

        impl<T: PartialOrd + Debug> PartialOrd for A<T> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                self.0.partial_cmp(&other.0)
            }
        }

        impl<T: PartialEq + Debug> PartialEq for A<T> {
            fn eq(&self, other: &Self) -> bool {
                self.0.eq(&other.0)
            }
        }

        impl<T: Ord + Debug> Eq for A<T> {}

        impl<T: Ord + Debug> Ord for A<T> {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl<T: Debug> Drop for A<T> {
            fn drop(&mut self) {
                println!("drop: {:?}", self.0);
            }
        }

        type Key = A<Vec<u8>>;

        let mut map: SkipList<Key, i32> = SkipList::new(10);

        for i in 1..=4 {
            println!("insert: {:?}", i);
            map.insert(A(vec![i]), i as i32);
        }
    }

    #[test]
    fn test_clear() {
        let mut map: SkipList<i32, i32> = SkipList::new(10);
        map.insert(1, 1);
        map.insert(2, 2);
        assert_eq!(Some(&1), map.get(&1));
        assert_eq!(Some(&2), map.get(&2));
        assert_eq!(2, map.length());
        map.clear();
        assert_eq!(None, map.get(&1));
        assert_eq!(None, map.get(&2));
        assert_eq!(0, map.length());
        map.insert(1, 3);
        map.insert(3, 4);
        assert_eq!(Some(&3), map.get(&1));
        assert_eq!(Some(&4), map.get(&3));
        assert_eq!(2, map.length());
    }

    #[test]
    fn test_clone() {
        let n = 100;
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
        let mut l = SkipList::new(10);
        for (k, v) in &kvs {
            l.insert(*k, *v);
        }

        let c = l.clone();
        assert_eq!(c.length(), l.length());

        for (k, v) in &kvs {
            assert_eq!(Some(v), l.get(k));
            assert_eq!(Some(v), c.get(k));
        }
    }

    #[test]
    fn test_random() {
        use std::time;
        let st = time::SystemTime::now();
        let n = 10000;
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
