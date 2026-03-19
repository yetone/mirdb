/**
 * Features data for MirDB Homepage.
 * Owner: Scenario 2 - Value Proposition Features Section
 */

import type { Feature } from '@/types';

export const features: Feature[] = [
  {
    id: 'memcached-compatible',
    title: 'Memcached Compatible',
    description: 'Drop-in replacement for memcached clients. Use existing memcached client libraries to connect seamlessly without any code changes.',
    icon: 'Plug',
  },
  {
    id: 'persistent-storage',
    title: 'Persistent Storage',
    description: 'Data persists across restarts using LSM tree architecture with SSTables. Never lose your cached data again.',
    icon: 'Database',
  },
  {
    id: 'rust-performance',
    title: 'Rust Performance',
    description: 'Built in Rust for memory safety and blazing-fast performance. Async I/O with Tokio for high concurrency.',
    icon: 'Zap',
  },
];
