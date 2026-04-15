/**
 * Shared Test Data and Constants
 * Used across E2E and unit tests.
 */

export const TEST_CONFIG = {
  PORT: 12333,
  HOST: 'localhost',
  DEFAULT_TIMEOUT: 5000,
};

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
};

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

export const EXPECTED_TEXT = {
  PRODUCT_NAME: 'MirDB',
  TAGLINE: 'persistent Memcached-compatible key-value store',
  CONNECTION_HOST: 'localhost',
  CONNECTION_PORT: '12333',
};
