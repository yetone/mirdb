/**
 * Feature data for MirDB Homepage.
 * Owner: Scenario 3 - Features Section Display
 */
import { FeatureData } from '@/types';

export const features: FeatureData[] = [
  {
    icon: '🔌',
    title: 'Memcached Protocol',
    description:
      'Drop-in compatible with the standard Memcached text protocol. Use existing Memcached clients seamlessly without any code changes.',
  },
  {
    icon: '💾',
    title: 'Persistence',
    description:
      'Unlike traditional Memcached, MirDB persists data to disk using SSTables (Sorted String Tables). Your data survives restarts.',
  },
  {
    icon: '🌲',
    title: 'LSM Tree',
    description:
      'Built on a Log-Structured Merge-tree architecture with efficient memtables, multi-level compaction, and optimized read/write paths.',
  },
];
