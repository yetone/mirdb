/**
 * Features Data.
 * Owner: Scenario 2 - Features Showcase Section
 *
 * Contains array of feature objects for MirDB's key capabilities.
 */

import type { Feature } from '../types';

export const features: Feature[] = [
  {
    title: 'Tokio Async Runtime',
    description:
      'Built on Tokio for high-performance async I/O, enabling thousands of concurrent connections with minimal overhead.',
    icon: 'lightning',
  },
  {
    title: 'Memtable with Skiplist',
    description:
      'In-memory data structure using skip lists for O(log n) insertions and lookups, ensuring fast write performance.',
    icon: 'layers',
  },
  {
    title: 'Minor Compaction',
    description:
      'Efficiently flushes memtable to disk as sorted string tables (SSTables), maintaining write throughput.',
    icon: 'compress',
  },
  {
    title: 'Major Compaction',
    description:
      'Merges multiple SSTables to reduce storage overhead and optimize read performance in the LSM-tree.',
    icon: 'merge',
  },
];
