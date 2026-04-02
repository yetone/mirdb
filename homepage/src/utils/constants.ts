/**
 * Application constants for MirDB Homepage.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const SITE_TITLE = 'MirDB - Persistent Key-Value Store';
export const SITE_DESCRIPTION = 'MirDB: A persistent key-value store with Memcached protocol support and LSM Tree architecture';

export const BREAKPOINTS = {
  mobile: 768,
  tablet: 1024,
  desktop: 1280,
} as const;

export const SECTION_IDS = {
  hero: 'hero',
  features: 'features',
  quickstart: 'quickstart',
  performance: 'performance',
  comparison: 'comparison',
} as const;

export const NAV_ITEMS = [
  { label: 'Home', href: '#hero' },
  { label: 'Features', href: '#features' },
  { label: 'Quick Start', href: '#quickstart' },
  { label: 'GitHub', href: GITHUB_URL, external: true },
];
