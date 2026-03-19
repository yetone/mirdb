/**
 * Application constants for MirDB Homepage.
 *
 * Contains URLs, configuration values, and static content.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb.svg?style=svg';

export const NAV_LINKS = [
  { label: 'Features', href: '#features' },
  { label: 'How It Works', href: '#how-it-works' },
  { label: 'Status', href: '#status' },
  { label: 'Quick Start', href: '#quick-start' },
  { label: 'Resources', href: '#resources' },
];

export const QUICK_START_COMMANDS = [
  { lang: 'bash', code: 'git clone https://github.com/yetone/mirdb.git' },
  { lang: 'bash', code: 'cd mirdb' },
  { lang: 'bash', code: 'cargo run --release' },
];
