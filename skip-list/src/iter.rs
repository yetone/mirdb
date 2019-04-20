use crate::list::SkipList;
use crate::node::SkipListNode;

pub struct SkipListIter<'a, K, V>(Option<&'a SkipListNode<K, V>>);

impl<'a, K, V> SkipListIter<'a, K, V> {
    pub fn new(list: &'a SkipList<K, V>) -> Self {
        SkipListIter(list.head())
    }
}

impl<'a, K, V> Iterator for SkipListIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .and_then(|node| {
                self.0 = node.next(0);
                self.0
            })
            .map(|node| (&node.key_, &node.value_))
    }
}

pub struct SkipListIterMut<'a, K, V>(Option<&'a mut SkipListNode<K, V>>);

impl<'a, K, V> SkipListIterMut<'a, K, V> {
    pub fn new(list: &'a mut SkipList<K, V>) -> Self {
        SkipListIterMut(list.head_mut())
    }
}

impl<'a, K, V> Iterator for SkipListIterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        let current = ::std::mem::replace(&mut self.0, None);
        current.and_then(|node| match node.next_mut(0) {
            None => None,
            Some(next) => {
                let next: *mut SkipListNode<K, V> = next;
                ::std::mem::replace(&mut self.0, Some(unsafe { &mut *next }));
                Some(unsafe { (&(*next).key_, &mut (*next).value_) })
            }
        })
    }
}

#[cfg(test)]
mod test {
    use crate::list::SkipList;

    #[test]
    fn test_iter() {
        let mut list = SkipList::new(10);
        for i in 0..=10 {
            list.insert(i, i + 1);
        }
        for (k, v) in list.iter() {
            println!("k: {}, v: {}", k, v);
        }
    }

    #[test]
    fn test_iter_mut() {
        let mut list = SkipList::new(10);
        for i in 0..=10 {
            list.insert(i, i + 1);
        }
        for (k, v) in list.iter_mut() {
            *v = k - 1;
        }
        for (k, v) in list.iter() {
            println!("k: {}, v: {}", k, v);
        }
    }
}
