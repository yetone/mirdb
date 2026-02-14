/**
 * Application constants for the MirDB Homepage.
 */
import type { NavLink } from '../types';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const DOCS_URL = 'https://github.com/yetone/mirdb#readme';

export const NAV_LINKS: NavLink[] = [
  { label: 'Home', href: '#' },
  { label: 'Features', href: '#features' },
  { label: 'Quick Start', href: '#quickstart' },
  { label: 'Roadmap', href: '#roadmap' },
  { label: 'Documentation', href: DOCS_URL },
];

export const THEME_STORAGE_KEY = 'mirdb-theme';

export const TAGLINE = 'Persistent Key-Value Storage with Memcached Protocol';
