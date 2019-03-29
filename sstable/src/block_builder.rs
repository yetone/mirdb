use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use crc::crc32;
use crc::crc32::Hasher32;
use integer_encoding::{FixedIntWriter, VarIntWriter};
use snap::Encoder;

use crate::block_handle::BlockHandle;
use crate::options::CompressType;
use crate::options::Options;
use crate::error::MyResult;
use crate::util::mask_crc;

pub const BLOCK_CTYPE_LEN: usize = 1;
pub const BLOCK_CKSUM_LEN: usize = 4;

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
        let mut restarts = vec![0];
        restarts.reserve(1023);

        BlockBuilder {
            opt,
            buffer: buffer.into(),
            count: 0,
            restart_count: 0,
            last_key: vec![],
            restarts,
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.count = 0;
        self.restart_count = 0;
        self.last_key.clear();
        self.restarts.clear();
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

        // write restart points
        for i in &self.restarts {
            self.buffer.write_fixedint(*i as u32).expect("write restart point error");
        }
        // write restarts count
        self.buffer.write_fixedint(self.restarts.len() as u32).expect("write restarts count error");

        // compress buffer
        if self.opt.compress_type == CompressType::Snappy {
            let mut encoder = Encoder::new();
            self.buffer = encoder.compress_vec(&self.buffer)?;
        }

        // write ctype
        let ctype_buf = [self.opt.compress_type as u8; BLOCK_CTYPE_LEN];
        self.buffer.write(&ctype_buf)?;

        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(&self.buffer);

        // write crc
        self.buffer.write_fixedint(mask_crc(digest.sum32()))?;

        w.seek(SeekFrom::Start(offset as u64))?;
        w.write(&self.buffer)?;

        let bh = bh!(offset, self.buffer.len());

        self.reset();

        Ok(bh)
    }
}
