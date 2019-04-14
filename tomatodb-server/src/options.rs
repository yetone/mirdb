use std::path::Path;
use sstable::Options as TableOptions;

const KB: usize = 1 << 10;
const MB: usize = KB * KB;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;

#[derive(Clone)]
pub struct Options {
    pub max_level: usize,
    pub work_dir: String,
    pub sst_max_size: usize,
    pub mem_table_max_size: usize,
    pub mem_table_max_height: usize,
    pub imm_mem_table_max_count: usize,
    pub block_size: usize,
    pub block_restart_interval: usize,

    pub l0_compaction_trigger: usize,
}

impl Options {
    pub fn to_table_opt(&self) -> TableOptions {
        let mut table_opt = TableOptions::default();
        table_opt.block_size = self.block_size;
        table_opt.block_restart_interval = self.block_restart_interval;
        table_opt
    }
}

impl Default for Options {
    fn default() -> Self {
        let opt = Options {
            max_level: 7,
            work_dir: "/tmp/tomatodb".into(),
            sst_max_size: MB * 100,
            mem_table_max_size: MB * 10,
            mem_table_max_height: 1 << 5,
            imm_mem_table_max_count: 1 << 4,
            block_size: BLOCK_MAX_SIZE,
            block_restart_interval: 16,

            l0_compaction_trigger: 4,
        };
        opt
    }
}
