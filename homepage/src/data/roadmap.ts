/**
 * Roadmap data for the Roadmap section.
 * Owner: Scenario 7 - Roadmap Section
 *
 * Contains completed and planned items:
 * - Completed: tokio, memtable, minor compaction, major compaction
 * - Planned: Raft consensus
 */

import type { RoadmapItem } from '../types';

export const roadmapItems: RoadmapItem[] = [
  {
    name: 'Tokio Runtime',
    completed: true,
    description: 'High-performance async networking powered by Tokio for efficient concurrent connections.',
  },
  {
    name: 'Memtable',
    completed: true,
    description: 'In-memory write buffer using skip lists for fast key-value operations.',
  },
  {
    name: 'Minor Compaction',
    completed: true,
    description: 'Flush memtable contents to immutable SSTable files on disk.',
  },
  {
    name: 'Major Compaction',
    completed: true,
    description: 'Merge multiple SSTables to reclaim space and improve read performance.',
  },
  {
    name: 'Raft Consensus',
    completed: false,
    description: 'Distributed consensus protocol for replication and high availability.',
  },
];
