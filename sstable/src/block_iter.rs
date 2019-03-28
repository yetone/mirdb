use integer_encoding::{FixedInt, VarInt};

pub struct BlockIter<'a> {
    pub(crate) block: &'a Vec<u8>,
    pub(crate) key: Vec<u8>,
    pub(crate) current_entry_offset: usize,
    pub(crate) offset: usize,
    pub(crate) val_offset: usize,
    pub(crate) restarts_offset: usize,
    pub(crate) current_restart_idx: usize,
}

impl<'a> BlockIter<'a> {
    pub fn valid(&self) -> bool {
        !self.key.is_empty() && self.val_offset > 0 && self.val_offset <= self.restarts_offset
    }

    pub fn reset(&mut self) {
        self.offset = 0;
        self.val_offset = 0;
        self.current_restart_idx = 0;
        self.key.clear();
    }

    pub fn restart_count(&self) -> usize {
        let count = u32::decode_fixed(&self.block[self.block.len() - 4..]);
        count as usize
    }

    fn advance(&mut self) -> bool {
        if self.offset >= self.restarts_offset {
            self.reset();
            return false;
        } else {
            self.current_entry_offset = self.offset;
        }

        let current_offset = self.current_entry_offset;

        let (shared, non_shared, _val_size, entry_head_len) = self.parse_entry_and_advance();
        self.assemble_key(current_offset + entry_head_len, shared, non_shared);

        let restart_count = self.restart_count();
        while self.current_restart_idx + 1 < restart_count
            && self.get_restart_point(self.current_restart_idx + 1) < self.current_entry_offset
            {
                self.current_restart_idx += 1;
            }
        true
    }

    fn seek_to_restart_point(&mut self, idx: usize) {
        let off = self.get_restart_point(idx);

        self.offset = off;
        self.current_entry_offset = off;
        self.current_restart_idx = idx;
        // advances self.offset to point to the next entry
        let (shared, non_shared, _, head_len) = self.parse_entry_and_advance();

        assert_eq!(shared, 0);
        self.assemble_key(off + head_len, shared, non_shared);
        assert!(self.valid());
    }


    fn get_restart_point(&self, idx: usize) -> usize {
        let restart = self.restarts_offset + 4 * idx;
        u32::decode_fixed(&self.block[restart..restart + 4]) as usize
    }

    fn parse_entry_and_advance(&mut self) -> (usize, usize, usize, usize) {
        let mut i = 0;
        let (shared, sharedlen) = usize::decode_var(&self.block[self.offset..]);
        i += sharedlen;

        let (non_shared, non_sharedlen) = usize::decode_var(&self.block[self.offset + i..]);
        i += non_sharedlen;

        let (valsize, valsizelen) = usize::decode_var(&self.block[self.offset + i..]);
        i += valsizelen;

        self.val_offset = self.offset + i + non_shared;
        self.offset = self.val_offset + valsize;

        (shared, non_shared, valsize, i)
    }

    fn assemble_key(&mut self, off: usize, shared: usize, non_shared: usize) {
        self.key.truncate(shared);
        self.key
            .extend_from_slice(&self.block[off..off + non_shared]);
    }

    pub fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        if self.valid() {
            key.clear();
            val.clear();
            key.extend_from_slice(&self.key);
            val.extend_from_slice(&self.block[self.val_offset..self.offset]);
            true
        } else {
            false
        }
    }

    pub fn current_kv(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut key = vec![];
        let mut value = vec![];
        if self.current(&mut key, &mut value) {
            Some((key, value))
        } else {
            None
        }
    }

    pub fn seek(&mut self, to: &[u8]) {
        self.reset();
        let mut left = 0;
        let restart_count = self.restart_count();
        let mut right = if restart_count > 0 {restart_count - 1} else {0};
        while left < right {
            let m = left + (right - left) / 2;
            self.seek_to_restart_point(m);
            if &self.key[..] < to {
                left = m;
            } else {
                right = m - 1;
            }
        }
        assert_eq!(left, right);
        self.current_restart_idx = left;
        self.offset = self.get_restart_point(left);

        while self.advance() {
            if &self.key[..] >= to {
                break;
            }
        }
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.advance() {
            return None;
        }
        self.current_kv()
    }
}
