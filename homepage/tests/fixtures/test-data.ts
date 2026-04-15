/**
 * Shared Test Data and Constants
 *
 * Contains test fixtures used across all test files.
 */

// Product information
export const PRODUCT_NAME = 'MirDB';
export const PRODUCT_TAGLINE = 'A persistent Memcached-compatible key-value store';
export const HERO_KEYWORDS = ['persistent', 'Memcached', 'key-value'];
export const CTA_TEXT = 'Get Started';

// Test configuration
export const TEST_CONFIG = {
  PORT: 12333,
  HOST: 'localhost',
  DEFAULT_TIMEOUT: 5000,
};

// Test URLs
export const TEST_URLS = {
  HOME: '/',
  QUICKSTART: '/#quickstart',
  FEATURES: '/#features',
  TECHSPECS: '/#techspecs',
};

// UI Selectors
export const SELECTORS = {
  // Quick Start section
  QUICK_START_SECTION: '#quickstart',
  CODE_BLOCK: '.code-block',
  CODE_CONTENT: '.code-block__content',
  COPY_BUTTON: '.code-block__copy-btn',

  // Hero section
  HERO_SECTION: '#hero',
  HERO_TITLE: '.hero__title',
  HERO_CTA: '.hero__cta',

  // Features section
  FEATURES_SECTION: '#features',
  FEATURE_CARD: '.feature-card',

  // Navigation
  NAV: '.nav',
  NAV_LINK: '.nav__link',

  // Organized by section
  HERO: {
    SECTION: '#hero',
    TITLE: '.hero__title',
    HEADLINE: '.hero__headline',
    DESCRIPTION: '.hero__description',
    CTA: '.hero__cta',
  },
  FEATURES: {
    SECTION: '#features',
  },
  QUICKSTART: {
    SECTION: '#quickstart',
  },
  TECHSPECS: {
    SECTION: '#techspecs',
    TITLE: '.techspecs__title',
    GRID: '.techspecs__grid',
    ITEM: '.techspecs__item',
    LABEL: '.techspecs__label',
    VALUE: '.techspecs__value',
  },
  FOOTER: {
    SECTION: '#footer',
    LINKS: '.footer__links',
    COPYRIGHT: '.footer__copyright',
  },
  HEADER: {
    SECTION: '#header',
    NAV: '.nav',
    NAV_LINKS: '.nav__links',
  },
};

// External URLs
export const EXTERNAL_URLS = {
  GITHUB_REPO: 'https://github.com/mirdb/mirdb',
  DOCUMENTATION: 'https://mirdb.dev/docs',
};

// Technical Specifications configuration values
export const TECHSPECS_CONFIG = {
  PORT: '12333',
  WORK_DIR: '/tmp/mirdb',
  SSTABLE_MAX_SIZE: '100 MB',
  MEMTABLE_MAX_SIZE: '4 MB',
  BLOCK_SIZE: '4 KB',
  MAX_LSM_LEVELS: '7',
};

// Sample code for testing
export const SAMPLE_CODE = {
  PYTHON: `from pymemcache.client import base

# Connect to MirDB
client = base.Client(('localhost', 12333))

# Store a value
client.set('key', 'value')

# Retrieve it back
result = client.get('key')
print(result)  # b'value'`,
};

// Expected text content
export const EXPECTED_TEXT = {
  PRODUCT_NAME: 'MirDB',
  TAGLINE: 'persistent Memcached-compatible key-value store',
  CONNECTION_HOST: 'localhost',
  CONNECTION_PORT: '12333',
};
