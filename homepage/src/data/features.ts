/**
 * Feature data for the MirDB Homepage.
 * Owner: Scenario 2 - Feature Overview Section
 */

import type { Feature } from '../types';

export const features: Feature[] = [
  {
    icon: '💾',
    title: 'Persistent Storage',
    description: 'Data survives restarts with durable disk-based storage. Your key-value pairs are safely persisted using a write-ahead log (WAL) for crash recovery.',
  },
  {
    icon: '🔌',
    title: 'Memcached Protocol',
    description: 'Drop-in compatibility with the Memcached text protocol. Use existing Memcached clients and tools without any code changes.',
  },
  {
    icon: '🌳',
    title: 'LSM Tree Architecture',
    description: 'Efficient write-optimized storage engine using Log-Structured Merge trees. Handles high write throughput while maintaining read performance.',
  },
  {
    icon: '📋',
    title: 'Skip Lists',
    description: 'In-memory data structures powered by skip lists for fast ordered key lookups. O(log n) search, insert, and delete operations.',
  },
  {
    icon: '🔄',
    title: 'Compaction',
    description: 'Automatic minor and major compaction to reclaim space and optimize read performance. Configurable compaction strategies for different workloads.',
  },
  {
    icon: '⚡',
    title: 'Async I/O',
    description: 'Built on Tokio for high-performance asynchronous I/O. Handle thousands of concurrent connections efficiently with minimal resource usage.',
  },
];
