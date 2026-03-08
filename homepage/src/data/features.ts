/**
 * Feature data for the Features section.
 * Owner: Scenario 3 - Features Section Display
 *
 * This file contains the feature list data to enable
 * easy content updates without code changes (NFR-5).
 */

import type { Feature } from '../types'

export const features: Feature[] = [
  {
    title: 'Memcached Protocol Support',
    description: 'Drop-in compatible with the standard memcached text protocol. Connect using any existing memcached client library.',
    icon: 'protocol',
  },
  {
    title: 'Data Persistence via SSTables',
    description: 'Unlike memcached, MirDB persists your data to disk using Sorted String Tables for reliable storage.',
    icon: 'storage',
  },
  {
    title: 'LSM Tree Architecture',
    description: 'Built on Log-Structured Merge-tree design for optimal write performance and efficient disk utilization.',
    icon: 'tree',
  },
  {
    title: 'Async Networking with Tokio',
    description: 'High-performance asynchronous networking powered by Tokio for handling thousands of concurrent connections.',
    icon: 'network',
  },
  {
    title: 'Skip List Memtable',
    description: 'In-memory data stored in a skip list structure for fast lookups and efficient sorted iteration.',
    icon: 'list',
  },
  {
    title: 'Multi-level Compaction',
    description: 'Automatic minor and major compaction keeps storage efficient and maintains consistent read performance.',
    icon: 'compact',
  },
]
