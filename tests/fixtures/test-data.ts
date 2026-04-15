/**
 * Shared Test Data and Constants
 *
 * Common test fixtures used across E2E and unit tests.
 */

export const SELECTORS = {
  // Theme Toggle
  themeToggle: '#theme-toggle',
  themeToggleIconLight: '.theme-toggle__icon--light',
  themeToggleIconDark: '.theme-toggle__icon--dark',

  // Header
  header: '.header',
  headerLogo: '.header__logo',
  headerNav: '.header__nav',

  // Hero
  hero: '.hero',
  heroTitle: '.hero__title',
  heroTagline: '.hero__tagline',
  heroCta: '.hero__cta',

  // Features
  features: '.features',
  featuresGrid: '.features__grid',
  featureCard: '.feature-card',

  // Quick Start
  quickstart: '.quickstart',
  codeBlock: '.code-block',
  copyButton: '.code-block__copy',

  // Tech Specs
  techspecs: '.techspecs',
  techspecsTable: '.techspecs__table',

  // Footer
  footer: '.footer',
  footerLinks: '.footer__links',
};

export const THEMES = {
  light: 'light',
  dark: 'dark',
};

export const THEME_STORAGE_KEY = 'mirdb-theme';

export const EXPECTED_CONTENT = {
  title: 'MirDB',
  tagline: 'A persistent Memcached-compatible key-value store',
  features: [
    'Memcached Protocol',
    'Disk Persistence',
    'LSM Tree Architecture',
    'Built with Rust',
  ],
};

export const COLORS = {
  light: {
    bg: 'rgb(255, 255, 255)',
    text: 'rgb(15, 23, 42)',
  },
  dark: {
    bg: 'rgb(15, 23, 42)',
    text: 'rgb(241, 245, 249)',
  },
};
