use std::borrow::Borrow;
use std::cmp::Ordering;
use std::convert::From;
use std::fmt;
use std::hash;
use std::io::Cursor;
use std::ops::Index;
use std::ops::Range;
use std::ops::RangeFull;
use std::ops::RangeTo;
use std::slice::SliceIndex;

use bytes::buf;
use bytes::Bytes;
use bytes::BytesMut;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slice {
    inner: Bytes,
}

impl Default for Slice {
    #[inline]
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl Slice {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Bytes::with_capacity(cap),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn slice(&self, begin: usize, end: usize) -> Self {
        Self {
            inner: self.inner.slice(begin, end),
        }
    }

    pub fn slice_from(&self, begin: usize) -> Self {
        Self {
            inner: self.inner.slice_from(begin),
        }
    }

    pub fn slice_to(&self, end: usize) -> Self {
        Self {
            inner: self.inner.slice_to(end),
        }
    }
}

impl<'a> PartialEq<&'a [u8]> for Slice {
    fn eq(&self, other: &&[u8]) -> bool {
        let s: &[u8] = self.as_ref();
        (&s).eq(other)
    }
}

impl<'a> PartialOrd<&'a [u8]> for Slice {
    fn partial_cmp(&self, other: &&[u8]) -> Option<Ordering> {
        let s: &[u8] = self.as_ref();
        (&s).partial_cmp(other)
    }
}

impl PartialEq<Vec<u8>> for Slice {
    fn eq(&self, other: &Vec<u8>) -> bool {
        let s: &[u8] = self.as_ref();
        (&s).eq(&&other[..])
    }
}

impl PartialOrd<Vec<u8>> for Slice {
    fn partial_cmp(&self, other: &Vec<u8>) -> Option<Ordering> {
        let s: &[u8] = self.as_ref();
        (&s).partial_cmp(&&other[..])
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl hash::Hash for Slice {
    fn hash<H>(&self, state: &mut H)
    where
        H: hash::Hasher,
    {
        self.inner.hash(state)
    }
}

impl Serialize for Slice {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.inner.as_ref())
    }
}

struct SliceVisitor;

impl<'de> Visitor<'de> for SliceVisitor {
    type Value = Slice;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("need bytes")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Slice::from(value))
    }
}

impl<'de> Deserialize<'de> for Slice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SliceVisitor)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<'a> Borrow<[u8]> for &'a Slice {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.inner.borrow()
    }
}

impl Borrow<[u8]> for Slice {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.inner.borrow()
    }
}

impl AsRef<[u8]> for Slice {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl Index<RangeFull> for Slice {
    type Output = [u8];

    fn index(&self, index: RangeFull) -> &Self::Output {
        self.inner.as_ref().index(index)
    }
}

impl Index<RangeTo<usize>> for Slice {
    type Output = [u8];

    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        self.inner.as_ref().index(index)
    }
}

impl IntoIterator for Slice {
    type Item = u8;
    type IntoIter = buf::Iter<Cursor<Bytes>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a Slice {
    type Item = u8;
    type IntoIter = buf::Iter<Cursor<&'a Bytes>>;

    fn into_iter(self) -> Self::IntoIter {
        (&self.inner).into_iter()
    }
}

impl Extend<u8> for Slice {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = u8>,
    {
        self.inner.extend(iter)
    }
}

impl<'a> Extend<&'a u8> for Slice {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = &'a u8>,
    {
        self.inner.extend(iter)
    }
}

impl From<BytesMut> for Slice {
    fn from(src: BytesMut) -> Self {
        Self {
            inner: src.freeze(),
        }
    }
}

macro_rules! impl_from {
    ($type:ty) => {
        impl From<$type> for Slice {
            fn from(src: $type) -> Self {
                Self {
                    inner: From::from(src),
                }
            }
        }
    };
}

impl_from!(Vec<u8>);
impl_from!(String);
impl_from!(&[u8]);
impl_from!(&str);

#[cfg(test)]
mod test {
    use bincode::deserialize;
    use bincode::serialize;

    use crate::utils::to_str;

    use super::*;

    #[test]
    fn test_ord() {
        assert_eq!(Slice::from("abc"), Slice::from("abc"));
        assert!(Slice::from("abc") < Slice::from("abd"));
    }

    #[test]
    fn test_serde() {
        let a = Slice::from("abc");
        let encoded = serialize(&a).unwrap();
        let decoded: Slice = deserialize(&encoded).unwrap();
        assert_eq!(Slice::from("abc"), decoded);
        println!("a: {}", to_str(&a));
    }
}
