/**
 * Application-wide constants for the MirDB homepage.
 */

export const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
export const CIRCLECI_BADGE_URL = 'https://circleci.com/gh/yetone/mirdb.svg';
export const CIRCLECI_PROJECT_URL = 'https://circleci.com/gh/yetone/mirdb';
export const GITHUB_API_URL = 'https://api.github.com/repos/yetone/mirdb';

import type { DemoCaption } from '../types';

export const DEMO_CAPTIONS: DemoCaption[] = [
  { id: 'connect', text: 'Connect to MirDB using any Memcached-compatible client on port 12333' },
  { id: 'set', text: 'Store a value with SET — data is persisted to disk via the write-ahead log' },
  { id: 'get', text: 'Retrieve values with GET — served from the in-memory skiplist memtable' },
  { id: 'delete', text: 'Remove entries with DELETE — cleanup is handled by background compaction' },
];
