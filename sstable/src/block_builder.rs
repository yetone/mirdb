use std::io::Seek;
use std::io::Write;

use integer_encoding::{FixedIntWriter, VarIntWriter};

use crate::block_handle::BlockHandle;
use crate::options::Options;
use crate::result::MyResult;
use crate::writer;

pub struct BlockBuilder {
    opt: Options,
    pub buffer: Vec<u8>,
    count: usize,
    restart_count: usize,
    pub(crate) last_key: Vec<u8>,
    restarts: Vec<u32>,
}

impl BlockBuilder {
    pub fn new(opt: Options) -> Self {
        BlockBuilder::new_with_buffer(vec![], opt)
    }

    pub fn new_with_buffer<T: Into<Vec<u8>>>(buffer: T, opt: Options) -> Self {
        BlockBuilder {
            opt,
            buffer: buffer.into(),
            count: 0,
            restart_count: 0,
            last_key: vec![],
            restarts: vec![],
        }
    }

    pub fn size_estimate(&self) -> usize {
        self.buffer.len() + 4 * self.restarts.len() + 4
    }

    pub fn add(&mut self, k: &[u8], v: &[u8]) {
        assert!(self.restart_count <= self.opt.block_restart_interval);
        assert!(
            self.buffer.is_empty() || self.last_key.as_slice() < k
        );

        let mut shared = 0;

        if self.restart_count < self.opt.block_restart_interval {
            let small = ::std::cmp::min(k.len(), self.last_key.len());

            while shared < small && self.last_key[shared] == k[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buffer.len() as u32);
            self.last_key.resize(0, 0);
            self.restart_count = 0;
        }

        let non_shared = k.len() - shared;

        self.buffer.write_varint(shared).expect("write key shared size error");
        self.buffer.write_varint(non_shared).expect("write key non-shared size error");
        self.buffer.write_varint(v.len()).expect("write value size error");
        self.buffer.extend_from_slice(&k[shared..]);
        self.buffer.extend_from_slice(v);

        self.last_key.resize(shared, 0);
        self.last_key.extend_from_slice(&k[shared..]);

        self.restart_count += 1;
        self.count += 1;
    }

    pub fn flush<T: Seek + Write>(&mut self, w: &mut T, offset: usize) -> MyResult<BlockHandle> {
        self.buffer.reserve(self.restarts.len() * 4 + 4);
        for i in &self.restarts {
            self.buffer.write_fixedint(*i as u32).expect("write restart point error");
        }
        self.buffer.write_fixedint(self.restarts.len() as u32).expect("write restarts count error");
        let (_value_size, next_offset) = writer::write_bytes(w, offset, &self.buffer)?;
        Ok(bh!(offset, next_offset - offset))
    }
}
