/**
 * Content configuration for the MirDB homepage.
 *
 * This file contains all text content, feature descriptions,
 * and navigation items for easy maintenance and updates.
 */

import type { Feature, NavItem, CodeExample } from '../types'

export const GITHUB_URL = 'https://github.com/yetone/mirdb'
export const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb.svg?style=svg'
export const CIRCLECI_BUILD_URL = 'https://circleci.com/gh/yetone/mirdb'

export const heroContent = {
  title: 'MirDB',
  subtitle: 'A persistent key-value store with Memcached protocol',
  description: 'Fast, durable, and Memcached-compatible key-value storage written in Rust.',
}

export const features: Feature[] = [
  {
    name: 'Memcached Protocol',
    description: 'Drop-in compatibility with existing memcached clients and tools',
  },
  {
    name: 'Disk Persistence',
    description: 'Data survives restarts with automatic write-ahead logging',
  },
  {
    name: 'LSM Tree Architecture',
    description: 'Efficient write and read performance with multi-level compaction',
  },
  {
    name: 'Skip-list Memtable',
    description: 'Fast in-memory operations with probabilistic structure',
  },
  {
    name: 'Multi-level Compaction',
    description: 'Optimized storage management with multi-level compaction strategy',
  },
]

export const navItems: NavItem[] = [
  { label: 'Documentation', href: `${GITHUB_URL}#readme` },
  { label: 'Examples', href: `${GITHUB_URL}#usage` },
  { label: 'GitHub', href: GITHUB_URL, external: true },
  { label: 'About', href: '#about' },
]

export const quickStartCommands: CodeExample[] = [
  {
    language: 'bash',
    title: 'Installation',
    code: 'cargo install mirdb-server',
  },
  {
    language: 'bash',
    title: 'Usage',
    code: `# Start the server
mirdb-server

# Connect with telnet
telnet localhost 11211

# Set a value
set mykey 0 0 5
hello

# Get the value
get mykey`,
  },
]

export const footerLinks: NavItem[] = [
  { label: 'GitHub', href: GITHUB_URL, external: true },
  { label: 'Issues', href: `${GITHUB_URL}/issues`, external: true },
  { label: 'License', href: `${GITHUB_URL}/blob/master/LICENSE`, external: true },
]
