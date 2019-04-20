use std::borrow::Borrow;

pub trait Table<K, V> {
    fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord;

    fn get_mut<Q: ?Sized>(&self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord;

    fn insert(&mut self, k: K, v: V) -> Option<V>;

    fn clear(&mut self);

    fn is_full(&self) -> bool;

    fn size(&self) -> usize;
}
