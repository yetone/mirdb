/**
 * Application constants for MirDB Homepage.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const DOCS_URL = 'https://github.com/yetone/mirdb#readme';

export const SITE_TITLE = 'MirDB - Persistent Key-Value Store';
export const SITE_DESCRIPTION = 'MirDB is a high-performance persistent key-value store with Memcached protocol support. Built on LSM Tree architecture for reliable disk persistence.';
export const SITE_URL = 'https://mirdb.io';
export const OG_IMAGE = '/og-image.png';

export const BREAKPOINTS = {
  mobile: 640,
  tablet: 768,
  desktop: 1024,
} as const;

export const SECTION_IDS = {
  hero: 'hero',
  features: 'features',
  quickstart: 'quickstart',
  performance: 'performance',
  comparison: 'comparison',
} as const;

export const HERO_CONTENT = {
  title: 'MirDB: Persistent Key-Value Store',
  tagline: 'Memcached protocol with disk persistence',
  ctaText: 'Get Started',
  ctaHref: GITHUB_URL,
} as const;

export const NAV_ITEMS = [
  { label: 'Home', href: '#', external: false },
  { label: 'Features', href: '#features', external: false },
  { label: 'Docs', href: '#quickstart', external: false },
  { label: 'GitHub', href: GITHUB_URL, external: true },
];
