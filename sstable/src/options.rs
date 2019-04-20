use std::sync::Arc;
use std::sync::RwLock;

use crate::block::Block;
use crate::cache::Cache;

const KB: usize = 1 << 10;
const MB: usize = KB * KB;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CompressType {
    None = 0,
    Snappy = 1,
}

pub fn int_to_compress_type(i: u32) -> Option<CompressType> {
    match i {
        0 => Some(CompressType::None),
        1 => Some(CompressType::Snappy),
        _ => None,
    }
}

#[derive(Clone)]
pub struct Options {
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub block_cache: Arc<RwLock<Cache<Block>>>,
    pub compress_type: CompressType,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            block_size: BLOCK_MAX_SIZE,
            block_restart_interval: 16,
            block_cache: Arc::new(RwLock::new(Cache::new(
                BLOCK_CACHE_CAPACITY / BLOCK_MAX_SIZE,
            ))),
            compress_type: CompressType::Snappy,
        }
    }
}
