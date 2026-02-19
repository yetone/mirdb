/**
 * Site configuration
 */

import type { NavItem } from '@/types'

export const siteConfig = {
  name: 'MirDB',
  tagline: 'High-performance persistent key-value store with Memcached protocol compatibility',
  github: 'https://github.com/yetone/mirdb',
}

export const navItems: NavItem[] = [
  { label: 'Features', href: '#features' },
  { label: 'Quick Start', href: '#quick-start' },
  { label: 'Usage', href: '#usage' },
  { label: 'GitHub', href: 'https://github.com/yetone/mirdb', external: true },
]
