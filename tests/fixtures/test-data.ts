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
  headerNavLink: '.header__nav-link',

  // Hero
  hero: '.hero',
  heroTitle: '.hero__title',
  heroTagline: '.hero__tagline',
  heroCta: '.hero__cta',
  heroDescription: '.hero__description',

  // Features
  features: '.features',
  featuresGrid: '.features__grid',
  featureCard: '.feature-card',
  featureCardTitle: '.feature-card__title',

  // Quick Start
  quickstart: '.quickstart',
  codeBlock: '.code-block',
  copyButton: '.code-block__copy',
  copyButtonText: '.code-block__copy span',

  // Tech Specs
  techspecs: '.techspecs',
  techspecsTable: '.techspecs__table',

  // Footer
  footer: '.footer',
  footerLinks: '.footer__links',
  footerLink: '.footer__link',
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

// Browser names for cross-browser testing
export const BROWSERS = {
  chromium: 'chromium',
  firefox: 'firefox',
  webkit: 'webkit',
  msedge: 'msedge',
} as const;
