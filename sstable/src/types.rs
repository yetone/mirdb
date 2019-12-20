use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::MyResult;

pub trait RandomAccess {
    fn read_at(&self, offset: usize, dst: &mut [u8]) -> MyResult<usize>;
}

/// BufferBackedFile is a simple type implementing RandomAccess on a Vec<u8>. Used for some tests.
#[allow(unused)]
pub type BufferBackedFile = Vec<u8>;

impl RandomAccess for BufferBackedFile {
    fn read_at(&self, offset: usize, dst: &mut [u8]) -> MyResult<usize> {
        if offset > self.len() {
            return Ok(0);
        }
        let remaining = self.len() - offset;
        let to_read = if dst.len() > remaining {
            remaining
        } else {
            dst.len()
        };
        (&mut dst[0..to_read]).copy_from_slice(&self[offset..offset + to_read]);
        Ok(to_read)
    }
}

impl RandomAccess for File {
    fn read_at(&self, offset: usize, dst: &mut [u8]) -> MyResult<usize> {
        Ok((self as &FileExt).read_at(dst, offset as u64)?)
    }
}

pub trait SsIterator {
    fn valid(&self) -> bool;
    fn advance(&mut self) -> bool;
    fn prev(&mut self) -> bool;
    fn current_k(&self) -> Option<Vec<u8>>;
    fn current_v(&self) -> Option<Vec<u8>>;
    fn reset(&mut self);
    fn seek(&mut self, key: &[u8]);
    fn seek_to_last(&mut self);

    fn seek_to_first(&mut self) {
        self.reset();
        self.advance();
    }

    fn current_kv(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.valid() {
            return None
        }
        self.current_k().and_then(|k| {
            self.current_v().and_then(|v| {
                Some((k, v))
            })
        })
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.advance() {
            None
        } else {
            self.current_kv()
        }
    }

    fn count(&mut self) -> usize {
        self.reset();
        let mut count = 0;

        while let Some(_) = self.next() {
            count += 1;
        }
        count
    }
}

pub struct SsIteratorIterWrap<'a, T> {
    inner: &'a mut T,
}

impl<'a, T: SsIterator> SsIteratorIterWrap<'a, T> {
    pub fn new(iter: &'a mut T) -> Self {
        Self { inner: iter }
    }
}

impl<'a, T: SsIterator> Iterator for SsIteratorIterWrap<'a, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
