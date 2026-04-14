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

  // Quick Start section
  quickStart: '#quick-start',
  quickStartTitle: '#quick-start .section-title',
  quickStartStep: '.quick-start-step',
  stepTitle: '.step-title',
  stepNumber: '.step-number',
  codeBlock: '.code-block',
  codeHeader: '.code-header',
  codeLanguage: '.code-language',
  copyButton: '.copy-button',
  codeContent: '.code-content',

  // Other sections (for other scenarios)
  features: '#features',
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

  // Quick Start content
  quickStartTitle: 'Quick Start',
  installationCommand: 'cargo install mirdb',
  serverStartCommand: 'mirdb',
  setCommand: 'SET',
  getCommand: 'GET',
  deleteCommand: 'DELETE',
};

export const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
};
