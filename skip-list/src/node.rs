use crate::util::from_raw;
use crate::util::from_raw_mut;
use std::mem;
use std::ptr;

#[derive(Debug)]
pub struct SkipListNode<K, V> {
    pub(crate) nexts_: Vec<*mut SkipListNode<K, V>>,
    pub(crate) key_: K,
    pub(crate) value_: V,
}

impl<K, V> SkipListNode<K, V> {
    pub fn key(&self) -> &K {
        &self.key_
    }

    pub fn value(&self) -> &V {
        &self.value_
    }

    pub fn value_mut(&mut self) -> &mut V {
        &mut self.value_
    }

    pub(crate) fn from_raw_mut<'a>(
        node_ptr: *mut SkipListNode<K, V>,
    ) -> Option<&'a mut SkipListNode<K, V>> {
        from_raw_mut(node_ptr)
    }

    pub(crate) fn from_raw<'a>(
        node_ptr: *mut SkipListNode<K, V>,
    ) -> Option<&'a SkipListNode<K, V>> {
        from_raw(node_ptr)
    }

    pub(crate) fn allocate_dummy(max_height: usize) -> *mut SkipListNode<K, V> {
        SkipListNode::allocate(
            unsafe { mem::uninitialized() },
            unsafe { mem::uninitialized() },
            max_height,
        )
    }

    pub(crate) fn allocate(key: K, value: V, height: usize) -> *mut SkipListNode<K, V> {
        Box::into_raw(box SkipListNode {
            nexts_: vec![ptr::null_mut(); height + 1],
            key_: key,
            value_: value,
        })
    }

    pub(crate) fn height(&self) -> usize {
        self.nexts_.len() - 1
    }

    pub(crate) fn free(node_ptr: *mut SkipListNode<K, V>) {
        unsafe {
            Box::from_raw(node_ptr);
        }
    }

    pub(crate) fn replace_value(&mut self, value: V) -> V {
        mem::replace(&mut self.value_, value)
    }

    pub fn next(&self, height: usize) -> Option<&SkipListNode<K, V>> {
        self.nexts_.get(height).and_then(|ptr| {
            if ptr.is_null() {
                None
            } else {
                Some(unsafe { &**ptr })
            }
        })
    }

    pub fn next_mut(&mut self, height: usize) -> Option<&mut SkipListNode<K, V>> {
        self.nexts_.get(height).and_then(|ptr| {
            if ptr.is_null() {
                None
            } else {
                Some(unsafe { &mut **ptr })
            }
        })
    }
}
