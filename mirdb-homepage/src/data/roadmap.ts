/**
 * Roadmap Data.
 * Owner: Scenario 4 - Roadmap Section
 */

import { RoadmapItem } from '../types/roadmap'

export const roadmapItems: RoadmapItem[] = [
  {
    id: 'tokio-runtime',
    title: 'Tokio Async Runtime',
    description: 'High-performance asynchronous I/O operations using Tokio.',
    status: 'complete',
  },
  {
    id: 'memcached-protocol',
    title: 'Memcached Protocol Support',
    description: 'Full compatibility with memcached text protocol for easy integration.',
    status: 'complete',
  },
  {
    id: 'skiplist-memtable',
    title: 'Skiplist Memtable',
    description: 'Efficient in-memory skiplist for fast writes and ordered storage.',
    status: 'complete',
  },
  {
    id: 'minor-compaction',
    title: 'Minor Compaction',
    description: 'Automatic flushing of memtable data to SSTables on disk.',
    status: 'complete',
  },
  {
    id: 'major-compaction',
    title: 'Major Compaction',
    description: 'Background merging of SSTables for optimized storage and reads.',
    status: 'complete',
  },
  {
    id: 'bloom-filters',
    title: 'Bloom Filters',
    description: 'Probabilistic data structure for efficient key existence checks.',
    status: 'in-progress',
  },
  {
    id: 'raft-consensus',
    title: 'Raft Consensus',
    description: 'Distributed consensus algorithm for high availability and data replication.',
    status: 'planned',
  },
  {
    id: 'cluster-mode',
    title: 'Cluster Mode',
    description: 'Multi-node deployment with automatic sharding and failover.',
    status: 'planned',
  },
]
