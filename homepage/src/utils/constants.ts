/**
 * Application constants for MirDB Homepage.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';
export const PRODUCT_NAME = 'MirDB';
export const TAGLINE = 'A Persistent Key-Value Store with Memcached Protocol';

export const FEATURES = [
  {
    title: 'Memcached Protocol',
    description: 'Compatible with the standard memcached text protocol, allowing existing clients to connect seamlessly.',
  },
  {
    title: 'Disk Persistence',
    description: 'Persists data to disk using SSTables (Sorted String Tables) for durability.',
  },
  {
    title: 'LSM-Tree Architecture',
    description: 'Uses a Log-Structured Merge-tree approach for efficient writes and compaction.',
  },
  {
    title: 'Skip List Memtables',
    description: 'In-memory data stored in skip list data structures for fast lookups.',
  },
  {
    title: 'Compaction',
    description: 'Supports both minor (memtable to SSTable) and major (level) compaction.',
  },
  {
    title: 'Written in Rust',
    description: 'Built with Rust for memory safety and high performance.',
  },
];

export const ROADMAP_ITEMS = [
  { title: 'Tokio-based async networking', status: 'completed' as const },
  { title: 'Memtable with skip list', status: 'completed' as const },
  { title: 'Minor compaction', status: 'completed' as const },
  { title: 'Major compaction', status: 'completed' as const },
  { title: 'Raft consensus', status: 'planned' as const, description: 'Distributed operation support' },
];

export const QUICK_START = {
  installation: `# Clone and build
git clone ${GITHUB_URL}
cd mirdb
cargo build --release

# Run MirDB server
./target/release/mirdb`,
  usage: `# Connect with any memcached client (e.g., telnet)
telnet localhost 12333

# Set a value
set mykey 0 0 5
hello
STORED

# Get the value
get mykey
VALUE mykey 0 5
hello
END`,
};
