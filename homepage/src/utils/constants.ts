/**
 * Application constants for MirDB Homepage.
 */
import type { Feature, RoadmapItem } from '../types';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';
export const PRODUCT_NAME = 'MirDB';
export const TAGLINE = 'A Persistent Key-Value Store with Memcached Protocol';

export const FEATURES: Feature[] = [
  {
    title: 'Memcached Protocol',
    description: 'Full compatibility with Memcached protocol, allowing seamless integration with existing clients and tools.',
    icon: '🔌',
  },
  {
    title: 'Disk Persistence',
    description: 'SSTable-based storage engine ensures your data is safely persisted to disk with durability guarantees.',
    icon: '💾',
  },
  {
    title: 'LSM-Tree Architecture',
    description: 'Log-Structured Merge-Tree provides excellent write performance and efficient space utilization.',
    icon: '🌲',
  },
  {
    title: 'Skip List Memtables',
    description: 'In-memory skip list data structure enables fast lookups and efficient range queries.',
    icon: '📋',
  },
  {
    title: 'Compaction',
    description: 'Automatic minor and major compaction optimizes storage and maintains read performance.',
    icon: '🗜️',
  },
  {
    title: 'Written in Rust',
    description: 'Built with Rust for memory safety, performance, and reliability without garbage collection pauses.',
    icon: '🦀',
  },
];

export const ROADMAP_ITEMS: RoadmapItem[] = [
  {
    title: 'Core Storage Engine',
    status: 'completed',
    description: 'LSM-tree based storage with SSTable persistence',
  },
  {
    title: 'Memcached Protocol Support',
    status: 'completed',
    description: 'Full Memcached protocol compatibility',
  },
  {
    title: 'Write-Ahead Logging',
    status: 'completed',
    description: 'WAL for durability and crash recovery',
  },
  {
    title: 'Raft Consensus',
    status: 'planned',
    description: 'Distributed consensus for high availability',
  },
];
