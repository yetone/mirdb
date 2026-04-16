/**
 * E2E Test Fixtures for Homepage.
 *
 * Provides selectors, viewport sizes, and test data for E2E testing.
 */

export const homepageSelectors = {
  hero: {
    section: '[data-testid="hero-section"]',
    headline: '[data-testid="hero-headline"]',
    subheadline: '[data-testid="hero-subheadline"]',
    primaryCta: '[data-testid="hero-primary-cta"]',
    secondaryCta: '[data-testid="hero-secondary-cta"]',
  },
  features: {
    section: '[data-testid="features-section"]',
    card: '[data-testid="feature-card"]',
  },
  demo: {
    section: '[data-testid="demo-section"]',
    input: '[data-testid="demo-url-input"]',
    button: '[data-testid="demo-try-button"]',
  },
  analytics: {
    section: '[data-testid="analytics-section"]',
  },
  footer: {
    section: '[data-testid="footer-section"]',
  },
};

export const viewportSizes = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
  largeDesktop: { width: 1920, height: 1080 },
};

export const testUrls = {
  valid: [
    'https://example.com',
    'https://example.com/path/to/page',
    'http://example.org',
  ],
  invalid: [
    '',
    'not-a-url',
    'ftp://invalid.com',
  ],
};
