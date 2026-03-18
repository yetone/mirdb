/**
 * Application constants for MirDB Homepage.
 */

import type { NavItem } from '../types';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';

export const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb';

export const SITE_TITLE = 'MirDB - Persistent Key-Value Store';

export const SITE_DESCRIPTION = 'A persistent key-value store with Memcached protocol support, built with Rust and Tokio';

export const NAV_ITEMS: NavItem[] = [
  { label: 'Features', href: '#features' },
  { label: 'Usage', href: '#usage' },
  { label: 'Quick Start', href: '#quickstart' },
  { label: 'GitHub', href: GITHUB_URL },
];

export const THEME_KEY = 'mirdb-theme';
