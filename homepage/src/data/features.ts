/**
 * Feature card content for the MirDB homepage
 */

import type { Feature } from '@/types'

export const features: Feature[] = [
  {
    id: 'lsm-tree',
    title: 'LSM-Tree Storage',
    description: 'High-performance storage engine built on Log-Structured Merge-Tree architecture for optimized write throughput and efficient disk utilization.',
    icon: 'database',
  },
  {
    id: 'memcached',
    title: 'Memcached Compatible',
    description: 'Drop-in replacement for Memcached with full protocol support. Use your existing Memcached clients without any code changes.',
    icon: 'plug',
  },
  {
    id: 'ttl',
    title: 'TTL Support',
    description: 'Automatic key expiration with configurable time-to-live. Set expiration times for cache entries and let MirDB handle cleanup.',
    icon: 'clock',
  },
  {
    id: 'wal',
    title: 'Write-Ahead Log',
    description: 'Durability guaranteed through Write-Ahead Logging. Crash recovery ensures your data persists even after unexpected failures.',
    icon: 'shield',
  },
]
