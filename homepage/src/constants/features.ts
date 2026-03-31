/**
 * Features constants for the MirDB Homepage.
 * Owner: Scenario 2 - Features Section Display
 *
 * Contains the list of MirDB features with their descriptions and status.
 */

import type { Feature } from '../types';

export const features: Feature[] = [
  {
    id: 'memcached-protocol',
    name: 'Memcached Protocol',
    description:
      'Full compatibility with the standard memcached text protocol, allowing existing memcached clients to connect seamlessly without any code changes.',
    status: 'implemented',
    icon: 'Network',
  },
  {
    id: 'persistence',
    name: 'Persistence',
    description:
      'Unlike traditional memcached, MirDB persists data to disk using SSTables (Sorted String Tables), ensuring durability across restarts.',
    status: 'implemented',
    icon: 'HardDrive',
  },
  {
    id: 'lsm-tree',
    name: 'LSM Tree Architecture',
    description:
      'Uses a Log-Structured Merge-tree approach with memtables, immutable memtables, and multi-level SSTable compaction for optimal write performance.',
    status: 'implemented',
    icon: 'GitBranch',
  },
  {
    id: 'async-networking',
    name: 'Async Networking',
    description:
      'Tokio-based async networking provides high-performance, non-blocking I/O for handling thousands of concurrent connections efficiently.',
    status: 'implemented',
    icon: 'Zap',
  },
  {
    id: 'compaction',
    name: 'Compaction',
    description:
      'Automatic minor and major compaction processes keep storage optimized by merging SSTables and removing obsolete data.',
    status: 'implemented',
    icon: 'Layers',
  },
  {
    id: 'raft-consensus',
    name: 'Raft Consensus',
    description:
      'Distributed consensus protocol for high availability and fault tolerance across multiple nodes. Coming soon to enable cluster deployments.',
    status: 'planned',
    icon: 'Users',
  },
];
