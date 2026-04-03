/**
 * Test Configuration for MirDB Homepage
 *
 * Shared configuration for Playwright and Jest tests
 */

const testConfig = {
  // Base URL for local development
  baseUrl: 'http://localhost:3000',

  // Viewport sizes for testing
  viewports: {
    desktop: { width: 1920, height: 1080 },
    laptop: { width: 1366, height: 768 },
    tablet: { width: 768, height: 1024 },
    mobile: { width: 375, height: 667 }
  },

  // Test timeouts
  timeouts: {
    default: 30000,
    navigation: 10000,
    animation: 500
  },

  // Screenshot settings
  screenshots: {
    path: './tests/screenshots',
    fullPage: true
  }
};

module.exports = testConfig;
