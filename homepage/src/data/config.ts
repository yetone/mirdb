/**
 * Configuration data for the MirDB Homepage.
 * Owner: Scenario 6 - Configuration Reference
 *
 * Contains all configurable options from mirdb.toml with their
 * default values and descriptions.
 */

import type { ConfigOption } from '../types';

export const configOptions: ConfigOption[] = [
  {
    name: 'addr',
    default: '0.0.0.0:12333',
    description: 'The address and port the server listens on for incoming connections.',
  },
  {
    name: 'work_dir',
    default: '/tmp/mirdb',
    description: 'The working directory where MirDB stores all data files including SSTables and WAL.',
  },
  {
    name: 'max_level',
    default: '7',
    description: 'Maximum number of levels in the LSM tree. Higher values allow more data but increase read latency.',
  },
  {
    name: 'sst_max_size',
    default: '100M',
    description: 'Maximum size of a single SSTable file. Larger files reduce file count but increase compaction time.',
  },
  {
    name: 'mem_table_max_size',
    default: '4M',
    description: 'Maximum size of the active memtable before it becomes immutable and is flushed to disk.',
  },
  {
    name: 'mem_table_max_height',
    default: '32',
    description: 'Maximum height of the skip list in the memtable. Affects memory usage and lookup performance.',
  },
  {
    name: 'imm_mem_table_max_count',
    default: '16',
    description: 'Maximum number of immutable memtables allowed before blocking writes to allow flushing.',
  },
  {
    name: 'block_size',
    default: '4K',
    description: 'Size of data blocks in SSTables. Smaller blocks improve point lookups, larger blocks improve scans.',
  },
  {
    name: 'block_restart_interval',
    default: '16',
    description: 'Number of keys between restart points in SSTable blocks. Affects compression and seek performance.',
  },
  {
    name: 'l0_compaction_trigger',
    default: '4',
    description: 'Number of Level 0 files that trigger compaction. Lower values reduce read amplification.',
  },
  {
    name: 'thread_sleep_ms',
    default: '500',
    description: 'Background thread sleep interval in milliseconds between checking for work to do.',
  },
];
