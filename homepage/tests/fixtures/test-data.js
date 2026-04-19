/**
 * Test Fixtures and Data
 * Owner: Scenario 1 - Hero Section Display
 *
 * Shared test data for all scenarios.
 */

// Hero Section Test Data
export const HERO_DATA = {
  title: 'MiRDB',
  tagline: 'A persistent key-value store with full Memcached protocol support',
  keywords: ['persistent', 'key-value store', 'memcached protocol'],
  githubUrl: 'https://github.com/yetone/mirdb',
  quickstartAnchor: '#quickstart',
};

// Quick Start Section Test Data (Scenario 3)
export const QUICKSTART_STEPS = [
  {
    number: 1,
    title: 'Download Pre-built Binary',
    description: 'Download the latest pre-built binary from the GitHub Releases page',
  },
  {
    number: 2,
    title: 'Or Install via Cargo',
    command: 'cargo install mirdb',
  },
  {
    number: 3,
    title: 'Run the Server',
    command: 'mirdb-server',
  },
  {
    number: 4,
    title: 'Connect with a Client',
    command: 'telnet localhost 12333',
  },
];

export const EXPECTED_COMMANDS = {
  cargoInstall: 'cargo install mirdb',
  runServer: 'mirdb-server',
  connect: 'telnet localhost 12333',
};

// Features Data - Scenario 2
export const FEATURES = [
  {
    id: 'memcached',
    title: 'Memcached Compatible',
    description: 'Full support for the Memcached text protocol. Use any existing Memcached client.',
    status: 'completed',
    icon: 'memcached.svg',
  },
  {
    id: 'persistence',
    title: 'Persistent Storage',
    description: 'Data persists across restarts using SSTable-based storage on disk.',
    status: 'completed',
    icon: 'storage.svg',
  },
  {
    id: 'lsm-tree',
    title: 'LSM Tree Architecture',
    description: 'Write-optimized Log-Structured Merge-tree for high performance.',
    status: 'completed',
    icon: 'performance.svg',
  },
  {
    id: 'raft',
    title: 'Raft Consensus',
    description: 'Distributed consensus for high availability and fault tolerance.',
    status: 'planned',
    icon: 'clock.svg',
  },
];

// Configuration Values - Scenario 6
export const CONFIG_VALUES = [
  {
    key: 'addr',
    value: '0.0.0.0:12333',
    description: 'Server listen address and port',
    port: 12333,
  },
  {
    key: 'work_dir',
    value: '/tmp/mirdb',
    description: 'Data directory path for storage files',
  },
  {
    key: 'max_level',
    value: '7',
    description: 'Maximum LSM tree levels',
  },
  {
    key: 'sst_max_size',
    value: '100MB',
    description: 'Maximum SSTable file size',
  },
  {
    key: 'mem_table_max_size',
    value: '4MB',
    description: 'Maximum memtable size before flush',
  },
  {
    key: 'block_size',
    value: '4KB',
    description: 'Data block size for SSTables',
  },
  {
    key: 'l0_compaction_trigger',
    value: '4',
    description: 'Level 0 files count before compaction',
  },
];

// Project Status Items - Scenario 14
export const STATUS_ITEMS = [
  {
    id: 'tokio-proto',
    name: 'Tokio/Proto',
    description: 'Async networking with memcached protocol',
    status: 'completed',
  },
  {
    id: 'skip-list',
    name: 'Skip List',
    description: 'Memtable with skip list data structure',
    status: 'completed',
  },
  {
    id: 'minor-compaction',
    name: 'Minor Compaction',
    description: 'Memtable to SSTable flush',
    status: 'completed',
  },
  {
    id: 'major-compaction',
    name: 'Major Compaction',
    description: 'SSTable level compaction',
    status: 'completed',
  },
  {
    id: 'raft',
    name: 'Raft',
    description: 'Distributed consensus protocol',
    status: 'planned',
  },
];

// Code Examples - Scenario 4 will extend
export const CODE_EXAMPLES = [];

// Navigation Data - Scenario 7
export const NAVIGATION_DATA = {
  githubUrl: 'https://github.com/yetone/mirdb',
  licenseUrl: 'https://github.com/yetone/mirdb/blob/master/LICENSE',
  licenseType: 'MIT',
  headerLinks: [
    { label: 'Features', href: '#features' },
    { label: 'Docs', href: '#quickstart' },
  ],
  footerLinks: [
    { label: 'GitHub', href: 'https://github.com/yetone/mirdb', external: true },
    { label: 'MIT License', href: 'https://github.com/yetone/mirdb/blob/master/LICENSE', external: true },
  ],
};

// Export all for convenience
export default {
  HERO_DATA,
  QUICKSTART_STEPS,
  EXPECTED_COMMANDS,
  FEATURES,
  CONFIG_VALUES,
  STATUS_ITEMS,
  CODE_EXAMPLES,
  NAVIGATION_DATA,
};
