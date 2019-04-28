use sstable::Options as TableOptions;
use std::path::Path;

pub const KB: usize = 1 << 10;
pub const MB: usize = KB * KB;
pub const GB: usize = KB * MB;
pub const TB: usize = KB * GB;

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

    pub l0_compaction_trigger: usize,

    pub thread_sleep_ms: usize,

    pub table_opt: TableOptions,
}

impl Options {
    pub fn get_table_opt(&self) -> &TableOptions {
        &self.table_opt
    }
}

impl Default for Options {
    fn default() -> Self {
        let mut table_opt = TableOptions::default();
        table_opt.block_size = BLOCK_MAX_SIZE;
        table_opt.block_restart_interval = 16;

        Options {
            max_level: 7,
            work_dir: "/tmp/mirdb".into(),
            sst_max_size: MB * 100,
            mem_table_max_size: MB * 4,
            mem_table_max_height: 1 << 5,
            imm_mem_table_max_count: 1 << 4,

            l0_compaction_trigger: 4,

            thread_sleep_ms: 500,

            table_opt,
        }
    }
}
