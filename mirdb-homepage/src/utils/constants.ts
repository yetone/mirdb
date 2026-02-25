/**
 * Application constants for the MirDB Homepage.
 */

import type { Feature, ProjectStatusItem, NavigationLink } from '../types';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';

export const COLORS = {
  rustOrange: '#b7410e',
  rustOrangeDark: '#8b2f08',
  bgLight: '#ffffff',
  bgDark: '#1a1a2e',
  textLight: '#333333',
  textDark: '#e0e0e0',
};

export const BREAKPOINTS = {
  mobile: 768,
  tablet: 1024,
  desktop: 1200,
};

export const FEATURES: Feature[] = [
  {
    title: 'Persistent Storage',
    description: 'Data persists reliably across restarts using write-ahead logging and SSTable storage.',
    icon: '💾',
    status: 'completed',
  },
  {
    title: 'Memcached Protocol Compatibility',
    description: 'Drop-in replacement for Memcached with full protocol support using Tokio async runtime.',
    icon: '🔌',
    status: 'completed',
  },
  {
    title: 'LSM Tree Architecture',
    description: 'High-performance Log-Structured Merge-tree implementation with efficient compaction.',
    icon: '🌲',
    status: 'completed',
  },
  {
    title: 'Raft Support',
    description: 'Distributed consensus for high availability and fault tolerance.',
    icon: '🔄',
    status: 'planned',
  },
];

export const PROJECT_STATUS: ProjectStatusItem[] = [
  { title: 'tokio with memcached protocol', completed: true },
  { title: 'memtable with skiplist', completed: true },
  { title: 'minor compaction', completed: true },
  { title: 'major compaction', completed: true },
  { title: 'raft', completed: false },
];

export const NAVIGATION_LINKS: NavigationLink[] = [
  { label: 'Features', href: '#features' },
  { label: 'Usage', href: '#usage' },
  { label: 'Architecture', href: '#architecture' },
  { label: 'Resources', href: '#resources' },
];
