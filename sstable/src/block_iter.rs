use integer_encoding::{FixedInt, VarInt};

use crate::types::SsIterator;

#[derive(Clone, Debug)]
pub struct BlockIterState {
    pub(crate) key: Vec<u8>,
    pub(crate) current_offset: usize,
    pub(crate) next_offset: usize,
    pub(crate) val_offset: usize,
    pub(crate) restarts_offset: usize,
    pub(crate) current_restart_idx: usize,
}

impl BlockIterState {
    pub fn new(restarts_offset: usize) -> Self {
        Self {
            key: vec![],
            next_offset: 0,
            current_offset: 0,
            val_offset: 0,
            restarts_offset,
            current_restart_idx: 0
        }
    }

    pub fn reset(&mut self) {
        self.next_offset = 0;
        self.val_offset = 0;
        self.current_restart_idx = 0;
        self.key.clear();
    }
}

pub struct BlockIter<'a> {
    pub(crate) block: &'a [u8],
    pub(crate) state: BlockIterState,
}

impl<'a> BlockIter<'a> {

    pub fn new(block: &'a [u8], restarts_offset: usize) -> Self {
        let state = BlockIterState::new(restarts_offset);

        Self::new_with_state(block, state)
    }

    pub fn new_with_state(block: &'a [u8], state: BlockIterState) -> Self {
        Self {
            block,
            state,
        }
    }

    pub fn restart_count(&self) -> usize {
        let count = u32::decode_fixed(&self.block[self.block.len() - 4..]);
        count as usize
    }

    fn seek_to_restart_point(&mut self, idx: usize) {
        let off = self.get_restart_point_offset(idx);

        self.state.next_offset = off;
        self.state.current_offset = off;
        self.state.current_restart_idx = idx;

        // advances self.offset to point to the next entry
        let (shared, non_shared, _, head_len) = self.parse_entry_and_advance();

        assert_eq!(shared, 0);

        self.assemble_key(off + head_len, shared, non_shared);

        assert!(self.valid());
    }

    fn get_restart_point_offset(&self, idx: usize) -> usize {
        let restart = self.state.restarts_offset + 4 * idx;
        u32::decode_fixed(&self.block[restart..restart + 4]) as usize
    }

    fn parse_entry_and_advance(&mut self) -> (usize, usize, usize, usize) {
        let mut i = 0;

        let (shared, shared_len) = usize::decode_var(&self.block[self.state.next_offset..]);
        i += shared_len;

        let (non_shared, non_shared_len) = usize::decode_var(&self.block[self.state.next_offset + i..]);
        i += non_shared_len;

        let (val_size, val_size_len) = usize::decode_var(&self.block[self.state.next_offset + i..]);
        i += val_size_len;

        self.state.val_offset = self.state.next_offset + i + non_shared;
        self.state.next_offset = self.state.val_offset + val_size;

        (shared, non_shared, val_size, i)
    }

    fn assemble_key(&mut self, offset: usize, shared: usize, non_shared: usize) {
        self.state.key.truncate(shared);
        self.state.key.extend_from_slice(&self.block[offset..offset + non_shared]);
    }

    pub fn key(&self) -> &[u8] {
        &self.state.key[..]
    }
}

impl<'a> SsIterator for BlockIter<'a> {
    fn valid(&self) -> bool {
        !self.state.key.is_empty() && self.state.val_offset > 0 && self.state.val_offset <= self.state.restarts_offset
    }

    fn advance(&mut self) -> bool {
        if self.state.next_offset >= self.state.restarts_offset {
            self.state.key.clear();
            return false;
        } else {
            self.state.current_offset = self.state.next_offset;
        }

        let current_offset = self.state.current_offset;

        let (shared, non_shared, _val_size, entry_head_len) = self.parse_entry_and_advance();

        self.assemble_key(current_offset + entry_head_len, shared, non_shared);

        let restart_count = self.restart_count();

        while self.state.current_restart_idx + 1 < restart_count
            && self.get_restart_point_offset(self.state.current_restart_idx + 1) < self.state.current_offset
            {
                self.state.current_restart_idx += 1;
            }

        true
    }

    fn prev(&mut self) -> bool {
        let orig_offset = self.state.current_offset;
        if orig_offset == 0 {
            self.reset();
            return false;
        }

        while self.get_restart_point_offset(self.state.current_restart_idx) >= orig_offset {
            assert_ne!(self.state.current_restart_idx, 0);
            self.state.current_restart_idx -= 1;
        }

        self.state.next_offset = self.get_restart_point_offset(self.state.current_restart_idx);

        let mut r;
        loop {
            r = self.advance();
            if self.state.next_offset >= orig_offset {
                break;
            }
        }

        r
    }

    fn current_k(&self) -> Option<Vec<u8>> {
        if self.valid() {
            Some(self.state.key.to_vec())
        } else {
            None
        }
    }

    fn current_v(&self) -> Option<Vec<u8>> {
        if self.valid() {
            Some((&self.block[self.state.val_offset..self.state.next_offset]).to_vec())
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.state.reset();
    }

    fn seek(&mut self, to: &[u8]) {
        self.reset();

        let mut left = 0;
        let restart_count = self.restart_count();
        let mut right = if restart_count > 0 {restart_count - 1} else {0};

        while left < right {
            let m = (left + right + 1) / 2;
            self.seek_to_restart_point(m);
            if self.key() < to {
                left = m;
            } else {
                right = m - 1;
            }
        }

        assert_eq!(left, right);
        self.state.current_restart_idx = left;
        self.state.next_offset = self.get_restart_point_offset(left);

        while self.advance() {
            if self.key() >= to {
                break;
            }
        }
    }

    fn seek_to_last(&mut self) {
        let restart_count = self.restart_count();

        if restart_count > 0 {
            self.seek_to_restart_point(restart_count - 1);
        } else {
            self.reset();
        }

        // Stop at last entry, before the iterator becomes invalid.
        //
        // We're checking the position before calling advance; if a restart point points to the
        // last entry, calling advance() will directly reset the iterator.
        while self.state.next_offset < self.state.restarts_offset {
            self.advance();
        }

        assert!(self.valid());
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use crate::block::Block;
    use crate::block_builder::BlockBuilder;
    use crate::MyResult;
    use crate::Options;
    use crate::types::SsIterator;

    fn get_simple_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            ("prefix_key1".as_bytes(), "value1".as_bytes()),
            ("prefix_key2".as_bytes(), "value2".as_bytes()),
            ("prefix_key3".as_bytes(), "value3".as_bytes()),
        ]
    }

    #[test]
    fn test_iter() -> MyResult<()> {
        let path = Path::new("/tmp/test_data_block_iter");
        let mut f = File::create(path)?;
        let mut opt = Options::default();
        opt.block_size = 20;
        let mut b = BlockBuilder::new(opt);
        let data = get_simple_data();
        for (k, v) in &data {
            b.add(*k, *v);
        }
        let bh = b.flush(&mut f, 0)?;
        f.flush()?;

        let f = File::open(path)?;
        let (b1, _) = Block::new_from_location(&f, &bh, Options::default())?;

        let mut iter = b1.iter();
        assert_eq!(None, iter.current_k());
        assert!(iter.advance());
        assert_eq!(iter.current_k().unwrap(), "prefix_key1".as_bytes());
        assert!(!iter.prev());
        assert_eq!(iter.current_k(), None);
        assert!(iter.advance());
        assert_eq!(iter.current_k().unwrap(), "prefix_key1".as_bytes());
        assert!(iter.advance());
        assert_eq!(iter.current_k().unwrap(), "prefix_key2".as_bytes());
        assert!(iter.advance());
        assert_eq!(iter.current_k().unwrap(), "prefix_key3".as_bytes());
        assert!(!iter.advance());
        assert_eq!(iter.current_k(), None);
        assert!(!iter.advance());
        assert_eq!(iter.current_k(), None);
        assert!(iter.prev());
        assert_eq!(iter.current_k().unwrap(), "prefix_key2".as_bytes());
        assert!(iter.prev());
        assert_eq!(iter.current_k().unwrap(), "prefix_key1".as_bytes());
        assert!(!iter.prev());
        assert_eq!(iter.current_k(), None);
        assert!(!iter.prev());
        assert_eq!(iter.current_k(), None);

        let mut iter = b1.iter();
        assert!(!iter.prev());
        assert_eq!(None, iter.current_k());

        let mut iter = b1.iter();
        iter.seek_to_last();
        assert_eq!(iter.current_k().unwrap(), "prefix_key3".as_bytes());
        assert!(iter.prev());
        assert_eq!(iter.current_k().unwrap(), "prefix_key2".as_bytes());
        assert!(iter.prev());
        assert_eq!(iter.current_k().unwrap(), "prefix_key1".as_bytes());
        assert!(!iter.prev());
        assert_eq!(None, iter.current_k());
        Ok(())
    }
}
