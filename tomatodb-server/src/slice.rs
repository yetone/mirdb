use std::borrow::Borrow;
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
use bytes::BytesMut;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{self, Visitor};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slice {
    inner: BytesMut,
}

impl Slice {
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(cap)
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
}

impl hash::Hash for Slice {
    fn hash<H>(&self, state: &mut H) where H: hash::Hasher {
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
        E: de::Error
    {
        Ok(Slice::from(value))
    }
}

impl<'de> Deserialize<'de> for Slice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de> {
        deserializer.deserialize_bytes(SliceVisitor)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Borrow<[u8]> for Slice {
    fn borrow(&self) -> &[u8] {
        self.inner.borrow()
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
    type IntoIter = buf::Iter<Cursor<BytesMut>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a Slice {
    type Item = u8;
    type IntoIter = buf::Iter<Cursor<&'a BytesMut>>;

    fn into_iter(self) -> Self::IntoIter {
        (&self.inner).into_iter()
    }
}

impl Extend<u8> for Slice {
    fn extend<T>(&mut self, iter: T) where T: IntoIterator<Item = u8> {
        self.inner.extend(iter)
    }
}

impl<'a> Extend<&'a u8> for Slice {
    fn extend<T>(&mut self, iter: T) where T: IntoIterator<Item = &'a u8> {
        self.inner.extend(iter)
    }
}

impl From<BytesMut> for Slice {
    fn from(src: BytesMut) -> Self {
        Self {
            inner: src
        }
    }
}

macro_rules! impl_from {
    ($type:ty) => {
        impl From<$type> for Slice {
            fn from(src: $type) -> Self {
                Self {
                    inner: From::from(src)
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
        let decoded = deserialize(&encoded).unwrap();
        assert_eq!(Slice::from("abc"), decoded);
    }
}