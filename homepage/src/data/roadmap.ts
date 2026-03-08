/**
 * Roadmap data for the Roadmap section.
 * Owner: Scenario 8 - Roadmap and Status Section
 *
 * This file contains the roadmap items to enable
 * easy content updates without code changes (NFR-5).
 */

import type { RoadmapItem } from '../types'

export const roadmapItems: RoadmapItem[] = [
  // Implemented features
  {
    title: 'Async Networking with Tokio',
    status: 'complete',
    description: 'High-performance async networking using Tokio runtime with memcached protocol support',
  },
  {
    title: 'Skip List Memtable',
    status: 'complete',
    description: 'In-memory data structure using skip lists for efficient key-value operations',
  },
  {
    title: 'Minor Compaction',
    status: 'complete',
    description: 'Automatic flushing of memtables to Level 0 SSTables when memory threshold is reached',
  },
  {
    title: 'Major Compaction',
    status: 'complete',
    description: 'SSTable level compaction with sorted, non-overlapping files for optimal read performance',
  },
  {
    title: 'Write-Ahead Log',
    status: 'complete',
    description: 'Durability through WAL ensuring crash recovery and data persistence',
  },
  // Planned features
  {
    title: 'Raft Consensus',
    status: 'planned',
    description: 'Distributed consensus protocol for cluster replication and high availability',
  },
  {
    title: 'Bloom Filters',
    status: 'planned',
    description: 'Probabilistic data structure for faster key existence checks across SSTables',
  },
]
