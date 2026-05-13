/**
 * Application-wide constants for the MirDB homepage.
 */
import type { MirDBFeature, ComparisonRow, RoadmapItem, CodeExample, DocLink, DemoCaption } from '../types';

export const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
export const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb.svg';
export const CIRCLECI_PROJECT_URL = 'https://circleci.com/gh/yetone/mirdb';
export const GITHUB_API_URL = 'https://api.github.com/repos/yetone/mirdb';
export const MEMCACHED_PROTOCOL_URL = 'https://github.com/memcached/memcached/blob/master/doc/protocol.txt';

export const COMPARISON_DATA: ComparisonRow[] = [
  {
    dimension: 'Persistence',
    mirdb: true,
    memcached: false,
    redis: true,
  },
  {
    dimension: 'Memcached Protocol',
    mirdb: true,
    memcached: true,
    redis: false,
  },
  {
    dimension: 'Storage Engine',
    mirdb: 'LSM-tree',
    memcached: 'In-memory hash table',
    redis: 'In-memory data structures',
  },
  {
    dimension: 'Compaction',
    mirdb: true,
    memcached: false,
    redis: false,
  },
  {
    dimension: 'Write Performance',
    mirdb: 'High (append-only)',
    memcached: 'Very high (in-memory)',
    redis: 'High (in-memory)',
  },
];

export const DEMO_CAPTIONS: DemoCaption[] = [
  { id: 'connect', text: 'Connect to MirDB using any Memcached-compatible client on port 12333' },
  { id: 'set', text: 'Store a value with SET — data is persisted to disk via the write-ahead log' },
  { id: 'get', text: 'Retrieve values with GET — served from the in-memory skiplist memtable' },
  { id: 'delete', text: 'Remove entries with DELETE — cleanup is handled by background compaction' },
];

export const FEATURES: MirDBFeature[] = [
  {
    id: 'memcached-protocol',
    title: 'Memcached Protocol Compatible',
    description: 'Drop-in replacement for existing Memcached clients with full text protocol support.',
    icon: 'Server',
  },
  {
    id: 'persistent-storage',
    title: 'Persistent Storage with WAL',
    description: 'Data survives restarts via Write-Ahead Log for durability guarantees.',
    icon: 'Database',
  },
  {
    id: 'async-tokio',
    title: 'Async Performance with Tokio',
    description: 'Built on tokio for high concurrency and non-blocking I/O operations.',
    icon: 'Zap',
  },
  {
    id: 'lsm-tree',
    title: 'LSM-Tree Architecture',
    description: 'Optimized for write-heavy workloads with log-structured merge-tree design.',
    icon: 'Layers',
  },
  {
    id: 'skiplist-memtable',
    title: 'Skiplist Memtable',
    description: 'Efficient in-memory data structure for fast read/write operations.',
    icon: 'ListTree',
  },
  {
    id: 'compaction',
    title: 'Automatic Compaction',
    description: 'Minor and major compaction for automatic background optimization.',
    icon: 'RefreshCw',
  },
];

export const ROADMAP_ITEMS: RoadmapItem[] = [
  { id: 'tokio', title: 'Tokio Async Networking', status: 'completed', description: 'Async memcached protocol server' },
  { id: 'protocol', title: 'Memcached Protocol Support', status: 'completed', description: 'Full text protocol compatibility' },
  { id: 'memtable', title: 'Skip List Memtable', status: 'completed', description: 'In-memory sorted key-value store' },
  { id: 'minor-compaction', title: 'Minor Compaction', status: 'completed', description: 'Memtable to SSTable flush' },
  { id: 'major-compaction', title: 'Major Compaction', status: 'completed', description: 'Multi-level SSTable merge' },
  { id: 'raft', title: 'Raft Consensus', status: 'planned', description: 'Distributed consensus for cluster mode' },
];

export const QUICK_START_EXAMPLES: CodeExample[] = [
  {
    id: 'install',
    title: 'Install MirDB',
    language: 'bash',
    code: 'cargo install mirdb',
  },
  {
    id: 'start-server',
    title: 'Start the Server',
    language: 'bash',
    code: 'mirdb-server --listen 0.0.0.0:12333',
  },
  {
    id: 'set',
    title: 'Store a Value',
    language: 'bash',
    code: 'echo -e "set mykey 0 3600 5\\r\\nhello" | nc localhost 12333',
  },
  {
    id: 'get',
    title: 'Retrieve a Value',
    language: 'bash',
    code: 'echo -e "get mykey\\r\\n" | nc localhost 12333',
  },
];

export const DOC_LINKS: DocLink[] = [
  {
    id: 'readme',
    title: 'Project Overview',
    description: 'README with project goals, features, and getting started guide.',
    url: GITHUB_REPO_URL,
    icon: 'BookOpen',
  },
  {
    id: 'api',
    title: 'API / Protocol Reference',
    description: 'Memcached protocol specification for all supported commands.',
    url: MEMCACHED_PROTOCOL_URL,
    icon: 'Code',
  },
  {
    id: 'architecture',
    title: 'Technical Architecture',
    description: 'LSM-tree design, compaction strategy, and storage engine internals.',
    url: `${GITHUB_REPO_URL}#readme`,
    icon: 'Cpu',
  },
];
