/**
 * Application constants for MirDB homepage.
 *
 * Contains URLs, text content, and configuration values
 * used across multiple components.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';
export const DOCS_URL = '#documentation';
export const API_DOCS_URL = '#api-reference';

export const SITE_TITLE = 'MirDB';
export const SITE_DESCRIPTION = 'A Persistent Key-Value Store with Memcached Protocol';

export const NAV_LINKS: { label: string; href: string; external?: boolean }[] = [
  { label: 'GitHub', href: GITHUB_URL, external: true },
  { label: 'Documentation', href: DOCS_URL },
  { label: 'API Reference', href: API_DOCS_URL },
];

export const FEATURES = [
  {
    id: 'persistent-storage',
    title: 'Persistent Key-Value Storage',
    description: 'Store your data reliably with persistent key-value storage that survives restarts.',
  },
  {
    id: 'memcached-protocol',
    title: 'Memcached Protocol Compatibility',
    description: 'Use existing Memcached clients with full protocol support.',
  },
  {
    id: 'lsm-tree',
    title: 'LSM-tree Implementation',
    description: 'Efficient writes with memtable (skip-list) and SSTable storage.',
  },
  {
    id: 'wal',
    title: 'Write-Ahead Logging',
    description: 'Durability guaranteed with write-ahead logging for crash recovery.',
  },
  {
    id: 'compaction',
    title: 'Atomic Compaction',
    description: 'Automatic minor and major compaction for optimal storage efficiency.',
  },
];
