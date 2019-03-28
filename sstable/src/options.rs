use crate::cache::Cache;
use crate::block::Block;
use std::cell::RefCell;
use std::rc::Rc;

const KB: usize = 1 << 10;
const MB: usize = KB * KB;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;

#[derive(Clone)]
pub struct Options {
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub block_cache: Rc<RefCell<Cache<Block>>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            block_size: BLOCK_MAX_SIZE,
            block_restart_interval: 16,
            block_cache: Rc::new(RefCell::new(Cache::new(BLOCK_CACHE_CAPACITY / BLOCK_MAX_SIZE))),
        }
    }
}