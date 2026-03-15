/**
 * Application constants for the MirDB Homepage.
 */

import type { NavLink, Feature } from '../types'

export const GITHUB_URL = 'https://github.com/yetone/mirdb'
export const GITHUB_ISSUES_URL = 'https://github.com/yetone/mirdb/issues'
export const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb'

export const PRODUCT_NAME = 'MirDB'
export const TAGLINE = 'A persistent key-value store with Memcached protocol support'

export const SECTIONS: NavLink[] = [
  { label: 'Features', href: '#features' },
  { label: 'Quick Start', href: '#quick-start' },
  { label: 'Architecture', href: '#architecture' },
  { label: 'Community', href: '#community' },
]

export const FEATURES: Feature[] = [
  {
    title: 'Memcached Protocol',
    description: 'Full compatibility with the standard memcached text protocol. Use your existing memcached clients and tools without any changes.',
    icon: 'Network',
  },
  {
    title: 'Persistent Storage',
    description: 'Data is durably stored using an LSM tree architecture. Your data survives restarts and is safely persisted to disk.',
    icon: 'HardDrive',
  },
  {
    title: 'Skip-List Memtable',
    description: 'High-performance in-memory data structure using skip lists for O(log n) operations on the active memtable.',
    icon: 'Layers',
  },
  {
    title: 'Intelligent Compaction',
    description: 'Both minor and major compaction strategies optimize storage efficiency and maintain consistent read performance.',
    icon: 'Combine',
  },
  {
    title: 'Built with Rust',
    description: 'Memory-safe implementation in Rust provides reliability, performance, and freedom from common security vulnerabilities.',
    icon: 'Shield',
  },
  {
    title: 'Async I/O',
    description: 'Tokio-powered asynchronous networking ensures high throughput and efficient resource utilization.',
    icon: 'Zap',
  },
]
