/**
 * Application constants for the MirDB Homepage.
 */
import type { NavLink } from '../types';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const DOCS_URL = 'https://github.com/yetone/mirdb#readme';

export const NAV_LINKS: NavLink[] = [
  { label: 'Home', href: '#home' },
  { label: 'Features', href: '#features' },
  { label: 'Documentation', href: DOCS_URL, external: true },
  { label: 'GitHub', href: GITHUB_URL, external: true },
];

export const THEME_STORAGE_KEY = 'mirdb-theme';

export const TAGLINE = 'Persistent Key-Value Storage with Memcached Protocol';
export const COPYRIGHT_YEAR = new Date().getFullYear();
export const PROJECT_NAME = 'MirDB';
