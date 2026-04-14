/**
 * Shared Test Fixtures
 * Owner: First Builder
 *
 * Contains:
 * - Test URLs and selectors
 * - Expected content strings
 * - Viewport sizes for responsive tests
 * - Accessibility test configurations
 */

export const SELECTORS = {
  // Hero section
  hero: '#hero',
  heroLogo: '.hero-logo',
  heroTagline: '.hero-tagline',
  heroTitle: '.hero-title',
  ctaGetStarted: '.cta-primary',
  ctaGitHub: '.cta-secondary',

  // Other sections (for other scenarios)
  features: '#features',
  quickStart: '#quick-start',
  architecture: '#architecture',
  protocol: '#protocol',
  status: '#status',
  header: 'header',
  footer: 'footer',
};

export const CONTENT = {
  productName: 'MirDB',
  tagline: 'A Persistent Key-Value Store with Memcached Protocol',
  ctaGetStartedText: 'Get Started',
  ctaGitHubText: 'View on GitHub',
  githubUrl: 'https://github.com/akiozihao/mirdb',
};

export const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
};
