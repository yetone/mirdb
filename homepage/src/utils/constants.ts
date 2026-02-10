/**
 * Application constants for the MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain shared constants.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';

export const SITE_TITLE = 'MirDB';

export const SITE_TAGLINE = 'A Persistent Key-Value Store with Memcached Protocol';

export const SITE_DESCRIPTION = 'MirDB is a high-performance, persistent key-value store that speaks the Memcached protocol. Built with Rust and Tokio for maximum performance and reliability.';

export const DOCS_URL = '#getting-started';

export const NAVIGATION_LINKS = [
  { label: 'Features', href: '#features', isExternal: false },
  { label: 'Usage', href: '#usage', isExternal: false },
  { label: 'Getting Started', href: '#getting-started', isExternal: false },
  { label: 'GitHub', href: GITHUB_URL, isExternal: true },
];
