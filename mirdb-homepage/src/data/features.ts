/**
 * Features data for MirDB Homepage.
 * Owner: Scenario 2 - Features Section Display
 *
 * Contains the four key features:
 * - LSM-tree architecture
 * - Rust implementation
 * - Crash recovery
 * - Memcached compatibility
 */

import { Feature } from '../types';

export const features: Feature[] = [
  {
    id: 'lsm-tree',
    title: 'LSM-tree Architecture',
    description:
      'Built on a Log-Structured Merge-tree architecture for exceptional write performance. Optimized for high-throughput workloads with efficient compaction strategies.',
    icon: 'tree',
  },
  {
    id: 'rust-implementation',
    title: 'Built with Rust',
    description:
      'Memory-safe implementation with zero-cost abstractions. Enjoy the performance of C/C++ with the safety guarantees of Rust.',
    icon: 'rust',
  },
  {
    id: 'crash-recovery',
    title: 'Crash Recovery',
    description:
      'Write-ahead logging ensures data durability. Automatic recovery from unexpected shutdowns with no data loss.',
    icon: 'shield',
  },
  {
    id: 'memcached-compatibility',
    title: 'Memcached Protocol',
    description:
      'Drop-in compatibility with the Memcached protocol. Use existing Memcached clients to interact with MirDB seamlessly.',
    icon: 'plug',
  },
];
