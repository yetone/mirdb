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

  // Features section
  features: '#features',
  featuresTitle: '.features-title',
  featuresGrid: '.features-grid',
  featureCard: '.feature-card',
  featureIcon: '.feature-icon',
  featureTitle: '.feature-title',
  featureDescription: '.feature-description',

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
  architecture: '#architecture',
  protocol: '#protocol',
  status: '#status',
  header: 'header.header',
  footer: 'footer',

  // Navigation section (Scenario 7)
  headerLogo: '.header-logo',
  logoText: '.logo-text',
  nav: 'nav.nav',
  navList: '.nav-list',
  navLink: '.nav-link',
  mobileMenuToggle: '.mobile-menu-toggle',
  hamburgerLine: '.hamburger-line',

  // Footer section (Scenario 8)
  footerContainer: '.footer-container',
  footerLinks: '.footer-links',
  footerLink: '.footer-link',
  footerGithubLink: '[data-testid="github-link"]',
  footerIssuesLink: '[data-testid="issues-link"]',
  footerContributingLink: '[data-testid="contributing-link"]',
  footerCopyright: '[data-testid="footer-copyright"]',

  // Architecture section (Scenario 4)
  architectureDiagram: '.architecture-diagram',
  architectureComponents: '.architecture-components',
  componentCard: '.component-card',
  componentTitle: '.component-title',
  componentDescription: '.component-description',
  mermaidDiagram: '.mermaid',

  // Protocol section
  protocolTitle: '.protocol-title',
  protocolTable: '.protocol-table',
  commandTable: '[data-testid="command-table"]',
  mirdbCommandTable: '[data-testid="mirdb-command-table"]',
  mirdbBadge: '.mirdb-badge',
  syntaxExamples: '.syntax-examples',
  syntaxExample: '.syntax-example',
};

export const CONTENT = {
  productName: 'MirDB',
  tagline: 'A Persistent Key-Value Store with Memcached Protocol',
  ctaGetStartedText: 'Get Started',
  ctaGitHubText: 'View on GitHub',
  githubUrl: 'https://github.com/akiozihao/mirdb',

  // Features content
  featuresSectionTitle: 'Features',
  featuresSubtitle: 'Powerful capabilities that set MirDB apart from traditional caching solutions',

  // Quick Start content
  quickStartTitle: 'Quick Start',
  installationCommand: 'cargo install mirdb',
  serverStartCommand: 'mirdb',
  setCommand: 'SET',
  getCommand: 'GET',
  deleteCommand: 'DELETE',
};

export const FEATURES_DATA = [
  { id: 'persistence', title: 'Disk Persistence' },
  { id: 'lsm-tree', title: 'LSM Tree Architecture' },
  { id: 'memcached-protocol', title: 'Memcached Protocol' },
  { id: 'compression', title: 'Snappy Compression' },
  { id: 'bloom-filters', title: 'Bloom Filters' },
  { id: 'async-io', title: 'Tokio Async Runtime' },
];

export const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
};
