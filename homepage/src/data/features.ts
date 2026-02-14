/**
 * Feature data for the Features section.
 * Owner: Scenario 4 - Features Section
 *
 * Contains 6 features:
 * 1. Memcached Protocol Compatible
 * 2. Persistent Storage
 * 3. LSM Tree Architecture
 * 4. Async/Tokio Networking
 * 5. Skip List Memtable
 * 6. Automatic Compaction
 */

import type { Feature } from '../types';

export const features: Feature[] = [
  {
    icon: '🔌',
    title: 'Memcached Protocol Compatible',
    description:
      'Drop-in replacement for Memcached with full protocol support. Use existing clients and tools without any code changes.',
  },
  {
    icon: '💾',
    title: 'Persistent Storage',
    description:
      'Your data survives restarts. Unlike traditional Memcached, MirDB persists data to disk while maintaining in-memory performance.',
  },
  {
    icon: '🌲',
    title: 'LSM Tree Architecture',
    description:
      'Built on Log-Structured Merge Trees for optimal write performance and efficient storage utilization.',
  },
  {
    icon: '⚡',
    title: 'Async/Tokio Networking',
    description:
      'High-performance async networking powered by Tokio. Handle thousands of concurrent connections efficiently.',
  },
  {
    icon: '📋',
    title: 'Skip List Memtable',
    description:
      'Fast in-memory indexing using skip lists for efficient key-value lookups and range scans.',
  },
  {
    icon: '🔄',
    title: 'Automatic Compaction',
    description:
      'Background compaction automatically merges SSTables to reclaim space and maintain read performance.',
  },
];
