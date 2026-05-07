/**
 * Shared data constants for MirDB homepage content.
 */

export const FEATURES = [
  {
    title: 'Memcached Protocol',
    description:
      'Compatible with the standard memcached text protocol. Use existing clients without modification.',
    icon: 'protocol',
  },
  {
    title: 'Persistence',
    description:
      'Unlike memcached, MirDB persists data to disk using SSTables (Sorted String Tables).',
    icon: 'disk',
  },
  {
    title: 'LSM-Tree Architecture',
    description:
      'Log-Structured Merge-tree with memtables, immutable memtables, and multi-level SSTable compaction.',
    icon: 'tree',
  },
];

export const CONFIG_OPTIONS = [
  { name: 'addr', default: '0.0.0.0:12333', description: 'Server binding address' },
  { name: 'max_level', default: '7', description: 'Number of compaction levels' },
  { name: 'work_dir', default: '/tmp/mirdb', description: 'Data storage location' },
  { name: 'sst_max_size', default: '100M', description: 'Maximum SSTable file size' },
  { name: 'mem_table_max_size', default: '4M', description: 'Memtable flush threshold' },
  { name: 'block_size', default: '4K', description: 'SSTable block size' },
];

export const PROTOCOL_COMMANDS = [
  {
    name: 'SET',
    syntax: 'set <key> <flags> <ttl> <bytes>',
    description: 'Store a key-value pair',
    example: 'set mykey 0 0 5\nhello',
  },
  {
    name: 'GET',
    syntax: 'get <key>',
    description: 'Retrieve a value by key',
    example: 'get mykey',
  },
  {
    name: 'DELETE',
    syntax: 'delete <key>',
    description: 'Remove a key',
    example: 'delete mykey',
  },
  {
    name: 'INFO',
    syntax: 'info',
    description: 'Display database status',
    example: 'info',
  },
];

export const ROADMAP_ITEMS = [
  { title: 'Tokio-based async networking', status: 'implemented' as const },
  { title: 'Memtable with skip list', status: 'implemented' as const },
  { title: 'Minor compaction', status: 'implemented' as const },
  { title: 'Major compaction', status: 'implemented' as const },
  { title: 'Raft consensus', status: 'planned' as const },
];

export const NAV_LINKS = [
  { label: 'Features', href: '#features' },
  { label: 'Getting Started', href: '#quick-start' },
  { label: 'Protocol', href: '#protocol' },
  { label: 'Architecture', href: '#architecture' },
  { label: 'GitHub', href: 'https://github.com/yetone/mirdb' },
];

export const QUICK_START_STEPS = [
  {
    title: 'Clone the repository',
    description: 'Get the latest source code from GitHub.',
    code: 'git clone https://github.com/yetone/mirdb.git\ncd mirdb',
    language: 'bash',
  },
  {
    title: 'Build the project',
    description: 'Compile MirDB using Cargo.',
    code: 'cargo build --release',
    language: 'bash',
  },
  {
    title: 'Run the server',
    description: 'Start MirDB with the default configuration.',
    code: 'cargo run --release --bin mirdb',
    language: 'bash',
  },
  {
    title: 'Connect and test',
    description: 'Use telnet or nc to connect and run memcached commands.',
    code: 'telnet 127.0.0.1 12333\n\nset mykey 0 0 5\nhello\nSTORED\n\nget mykey\nVALUE mykey 0 5\nhello\nEND',
    language: 'bash',
  },
];
