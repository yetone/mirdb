use std::borrow::Borrow;
use std::cmp::Ordering;
use std::convert::From;
use std::fmt;
use std::hash;
use std::ops::Index;
use std::ops::RangeFull;
use std::ops::RangeTo;

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
        Self {
            inner: Bytes::new(),
        }
    }
}

impl Slice {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        let buf = BytesMut::with_capacity(cap);
        Self {
            inner: buf.freeze(),
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
            inner: self.inner.slice(begin..end),
        }
    }

    pub fn slice_from(&self, begin: usize) -> Self {
        Self {
            inner: self.inner.slice(begin..),
        }
    }

    pub fn slice_to(&self, end: usize) -> Self {
        Self {
            inner: self.inner.slice(..end),
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
        self.as_ref()
    }
}

impl Borrow<[u8]> for Slice {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_ref()
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
    type IntoIter = std::vec::IntoIter<u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.to_vec().into_iter()
    }
}

impl<'a> IntoIterator for &'a Slice {
    type Item = &'a u8;
    type IntoIter = std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl Extend<u8> for Slice {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = u8>,
    {
        let mut buf = BytesMut::from(self.inner.as_ref());
        buf.extend(iter);
        self.inner = buf.freeze();
    }
}

impl<'a> Extend<&'a u8> for Slice {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = &'a u8>,
    {
        let mut buf = BytesMut::from(self.inner.as_ref());
        buf.extend(iter.into_iter().copied());
        self.inner = buf.freeze();
    }
}

impl From<BytesMut> for Slice {
    fn from(src: BytesMut) -> Self {
        Self { inner: src.into() }
    }
}

impl From<Vec<u8>> for Slice {
    fn from(src: Vec<u8>) -> Self {
        Self {
            inner: Bytes::from(src),
        }
    }
}

impl From<String> for Slice {
    fn from(src: String) -> Self {
        Self {
            inner: Bytes::from(src),
        }
    }
}

impl From<&[u8]> for Slice {
    fn from(src: &[u8]) -> Self {
        Self {
            inner: Bytes::copy_from_slice(src),
        }
    }
}

impl From<&str> for Slice {
    fn from(src: &str) -> Self {
        Self {
            inner: Bytes::copy_from_slice(src.as_bytes()),
        }
    }
}

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
