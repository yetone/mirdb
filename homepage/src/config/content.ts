/**
 * Content configuration for the MirDB homepage.
 */

import type { Feature, NavItem, CodeExample, HeroContent } from '../types'

export const GITHUB_URL = 'https://github.com/yetone/mirdb'
export const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb'

export const heroContent: HeroContent = {
  title: 'MirDB',
  subtitle: 'Persistent Key-Value Store',
  description: 'Fast, durable, and Memcached-compatible key-value storage written in Rust. Drop-in replacement with disk persistence.',
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
    description: 'Fast in-memory operations with probabilistic data structure',
  },
  {
    name: 'Multi-level Compaction',
    description: 'Optimized storage with automatic background compaction',
  },
]

export const navItems: NavItem[] = [
  { label: 'Documentation', href: '#docs' },
  { label: 'Examples', href: '#examples' },
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
    title: 'Start Server',
    code: 'mirdb-server',
  },
  {
    language: 'bash',
    title: 'Connect with memcached client',
    code: `echo "set mykey 0 0 5\\r\\nhello\\r\\n" | nc localhost 11211`,
  },
]

export const footerLinks: NavItem[] = [
  { label: 'GitHub', href: GITHUB_URL, external: true },
  { label: 'Issues', href: `${GITHUB_URL}/issues`, external: true },
  { label: 'License', href: `${GITHUB_URL}/blob/master/LICENSE`, external: true },
]
