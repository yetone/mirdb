/**
 * Features Data.
 * Owner: Scenario 3 - Features Section
 */

import { Feature } from '../types/features'

export const features: Feature[] = [
  {
    id: 'tokio-async',
    title: 'Tokio Async Runtime',
    description: 'Built on Tokio for high-performance asynchronous I/O operations and efficient concurrency handling.',
    icon: '⚡',
    status: 'complete',
  },
  {
    id: 'memcached-protocol',
    title: 'Memcached Protocol',
    description: 'Full compatibility with the memcached text protocol, enabling drop-in replacement for existing systems.',
    icon: '🔌',
    status: 'complete',
  },
  {
    id: 'skiplist-memtable',
    title: 'Skiplist Memtable',
    description: 'Efficient in-memory skiplist data structure for fast writes and ordered key-value storage.',
    icon: '📋',
    status: 'complete',
  },
  {
    id: 'minor-compaction',
    title: 'Minor Compaction',
    description: 'Automatic minor compaction to flush memtable data to sorted string tables (SSTables) on disk.',
    icon: '📦',
    status: 'complete',
  },
  {
    id: 'major-compaction',
    title: 'Major Compaction',
    description: 'Background major compaction to merge SSTables, optimize storage, and improve read performance.',
    icon: '🗜️',
    status: 'complete',
  },
]
