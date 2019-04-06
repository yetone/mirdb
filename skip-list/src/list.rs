use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::ops::Drop;
use std::borrow::Borrow;

use crate::node::SkipListNode;
use crate::height_generator::HeightGenerator;
use crate::height_generator::GenHeight;
use crate::iter::SkipListIter;
use crate::iter::SkipListIterMut;
use std::fmt::Debug;

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

            while let Some(next) = (*current).next_mut(0) {
                if !is_head {
                    SkipListNode::free(current);
                }
                current = next;
                is_head = false;
            }

            if !is_head {
                SkipListNode::free(current);
            }

            // drop head nexts ptr
            let nexts_ptr = &mut (*self.head_).nexts_ as *mut Vec<*mut SkipListNode<K, V>>;
            nexts_ptr.drop_in_place();
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

        let lower_bound = self.get_lower_bound(key);

        if let Some(next) = lower_bound.next(0) {
            if next.key().borrow() == key {
                return Some(next.value());
            }
        }

        None
    }

    pub fn get_mut<Q: ?Sized>(&self, key: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Ord {

        let lower_bound = self.get_lower_bound(key);

        if let Some(next) = lower_bound.next_mut(0) {
            if next.key().borrow() == key {
                return Some(next.value_mut());
            }
        }

        None
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {

        let height = self.height_generator.gen_height(self.max_height_);

        let (lower_bound, mut updates) = self.get_lower_bound_and_updates(&key);

        if let Some(next) = lower_bound.next_mut(0) {
            if next.key() == &key {
                return Some(next.replace_value(value));
            }
        }

        let node_ptr = SkipListNode::allocate(key, value, height);

        for i in 0..=height {
            let update = &mut updates[i];
            unsafe {
                *((*node_ptr).nexts_.get_unchecked_mut(i)) = *(update.nexts_.get_unchecked_mut(i));
                *(update.nexts_.get_unchecked_mut(i)) = node_ptr;
            }
        }

        self.height_ = ::std::cmp::max(self.height_, height);
        self.length_ += 1;
        None
    }

    pub fn get_updates_for_bench<Q: ?Sized>(&self, key: &Q) -> (&mut SkipListNode<K, V>, Vec<&mut SkipListNode<K, V>>)
        where K: Borrow<Q>,
              Q: Ord {
        self.get_lower_bound_and_updates(key)
    }

    fn get_lower_bound_and_updates<Q: ?Sized>(&self, key: &Q) -> (&mut SkipListNode<K, V>, Vec<&mut SkipListNode<K, V>>)
        where K: Borrow<Q>,
              Q: Ord {

        let max_height = self.max_height_;
        let mut updates= Vec::with_capacity(max_height + 1);

        unsafe {
            updates.set_len(max_height + 1);

            for update in updates.iter_mut().take(max_height + 1).skip(self.height_ + 1) {
                *update = &mut (*self.head_);
            }

            let mut current_ptr = self.head_;

            for i in (0..=self.height_).rev() {
                while let Some(next) = (*current_ptr).next_mut(i) {
                    if next.key().borrow() < key {
                        current_ptr = next;
                    } else {
                        break;
                    }
                }
                updates[i] = &mut (*current_ptr);
            }

            (&mut (*current_ptr), updates)
        }
    }

    fn get_lower_bound<Q: ?Sized>(&self, key: &Q) -> &mut SkipListNode<K, V>
        where K: Borrow<Q>,
              Q: Ord {

        unsafe {
            let mut current_ptr = self.head_;

            for i in (0..=self.height_).rev() {
                while let Some(next) = (*current_ptr).next_mut(i) {
                    if next.key().borrow() < key {
                        current_ptr = next;
                    } else {
                        break;
                    }
                }
            }

            &mut (*current_ptr)
        }
    }

    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
        where K: Borrow<Q>,
              Q: Ord {

        let (lower_bound, mut updates) = self.get_lower_bound_and_updates(key);

        if let Some(next) = lower_bound.next_mut(0) {
            if next.key().borrow() != key {
                return None;
            }

            for i in 0..=next.height() {
                let update = &mut updates[i];
                unsafe {
                    *(update.nexts_.get_unchecked_mut(i)) = *(next.nexts_.get_unchecked_mut(i));
                }
            }

            let old_value = next.replace_value(unsafe { mem::uninitialized() });
            SkipListNode::free(next);

            self.length_ -= 1;

            return Some(old_value);
        }
        None
    }
}

impl<K: Display, V: Display> Display for SkipList<K, V> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "[")?;
        for (i, (k, v)) in self.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "({}, {})", k, v)?;
        }
        write!(f, "]")
    }
}

impl<K, V> Debug for SkipList<K, V> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "SkipList {{ len = {}, height = {} }}", self.length(), self.height())
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
        assert_eq!("[(0, 1), (1, 2), (2, 3)]", list.to_string());
    }

    #[test]
    fn test_insert() {
        let mut list = SkipList::new(10);
        list.insert(2, 4);
        list.insert(0, 1);
        list.insert(1, 3);
        list.insert(0, 2);
        assert_eq!(Some(&2), list.get(&0));
        assert_eq!(Some(&3), list.get(&1));
        assert_eq!(Some(&4), list.get(&2));
        assert_eq!(None, list.get(&3));
        assert_eq!(3, list.length());
    }

    #[test]
    fn test_remove() {
        let mut list = SkipList::new(10);
        list.insert(2, 4);
        list.insert(0, 2);
        list.insert(1, 3);
        assert_eq!(Some(&2), list.get(&0));
        assert_eq!(Some(&3), list.get(&1));
        assert_eq!(Some(&4), list.get(&2));
        assert_eq!(None, list.get(&3));
        assert_eq!(3, list.length());
        list.remove(&1);
        assert_eq!(None, list.get(&1));
        assert_eq!(Some(&2), list.get(&0));
        assert_eq!(Some(&4), list.get(&2));
        assert_eq!(2, list.length());
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
