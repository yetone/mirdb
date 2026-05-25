/**
 * Application constants for MirDB Homepage.
 */

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

export const METRICS_POLL_INTERVAL_MS = 2000;

export const HEALTH_POLL_INTERVAL_MS = 5000;

export const LSM_STATE_CACHE_TTL_MS = 5000;

export const DEFAULT_THEME = 'light' as const;

export const GITHUB_REPO_URL = 'https://github.com/mirdb/mirdb';

export const DOCS_URL = 'https://mirdb.io/docs';

export const COMMUNITY_URL = 'https://mirdb.io/community';
