/**
 * Application constants for MirDB Homepage.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const DOCS_URL = 'https://github.com/yetone/mirdb#readme';

export const SITE_TITLE = 'MirDB - Persistent Key-Value Store';
export const SITE_DESCRIPTION = 'A persistent key-value store with Memcached protocol support';

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
