/**
 * Site configuration
 */
import { NavItem } from '@/types'
import { GITHUB_URL, SECTION_IDS } from '@/utils/constants'

export const siteConfig = {
  name: 'MirDB',
  tagline: 'A persistent key-value store with Memcached protocol compatibility',
  description: 'MirDB is a high-performance persistent key-value store built with LSM-Tree storage engine. Drop-in replacement for Memcached with data persistence.',
  github: GITHUB_URL,
}

export const navItems: NavItem[] = [
  { label: 'Features', href: `#${SECTION_IDS.features}` },
  { label: 'Quick Start', href: `#${SECTION_IDS.quickStart}` },
  { label: 'Usage', href: `#${SECTION_IDS.usage}` },
  { label: 'GitHub', href: GITHUB_URL, external: true },
]
