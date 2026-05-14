/**
 * Application constants for the MirDB homepage.
 * Created by the first scenario builder.
 */

export const APP_NAME = 'MirDB';
export const APP_TAGLINE = 'A persistent key-value store with Memcached protocol compatibility';
export const APP_DESCRIPTION = 'MirDB is a high-performance persistent key-value store written in Rust. It speaks the Memcached protocol while storing data durably on disk using LSM trees.';
export const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
export const DOCS_URL = 'https://github.com/yetone/mirdb#readme';
export const COMMUNITY_URL = 'https://github.com/yetone/mirdb/discussions';
export const SITE_URL = 'https://mirdb.dev';

export const DEFAULT_PORT = 12333;
export const COPY_FEEDBACK_DURATION_MS = 2000;

export const SECTION_IDS = {
  HERO: 'hero',
  FEATURES: 'features',
  QUICK_START: 'quick-start',
  ARCHITECTURE: 'architecture',
  FAQ: 'faq',
} as const;
