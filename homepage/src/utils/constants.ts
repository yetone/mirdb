/**
 * Application constants for the MirDB Homepage.
 */
import type { Feature, NavigationLink, InstallationStep } from '../types';

export const PRODUCT_NAME = 'MirDB';

export const TAGLINE = 'A Persistent Key-Value Store with Memcached Protocol';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';

export const DOCUMENTATION_URL = 'https://github.com/yetone/mirdb#readme';

export const NAVIGATION_LINKS: NavigationLink[] = [
  { label: 'Features', href: '#features' },
  { label: 'Installation', href: '#installation' },
  { label: 'Usage', href: '#usage' },
  { label: 'GitHub', href: GITHUB_URL, isExternal: true },
];

export const FEATURES: Feature[] = [
  {
    id: 'memcached',
    title: 'Memcached Protocol',
    description: 'Compatible with the standard Memcached text protocol, allowing existing memcached clients to connect seamlessly.',
    icon: '🔌',
  },
  {
    id: 'persistence',
    title: 'Data Persistence',
    description: 'Unlike traditional memcached, MirDB persists data to disk using SSTables (Sorted String Tables).',
    icon: '💾',
  },
  {
    id: 'lsm',
    title: 'LSM Tree Architecture',
    description: 'Uses a Log-Structured Merge-tree approach with memtables, immutable memtables, and multi-level SSTable compaction.',
    icon: '🌳',
  },
  {
    id: 'async',
    title: 'Async I/O',
    description: 'Built on Tokio for high-performance asynchronous networking and I/O operations.',
    icon: '⚡',
  },
];

export const INSTALLATION_STEPS: InstallationStep[] = [
  {
    id: 1,
    label: 'Clone the repository',
    command: 'git clone https://github.com/yetone/mirdb.git',
  },
  {
    id: 2,
    label: 'Build the project',
    command: 'cd mirdb && cargo build --release',
  },
  {
    id: 3,
    label: 'Run MirDB',
    command: './target/release/mirdb-server',
  },
];
