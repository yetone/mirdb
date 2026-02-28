// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright configuration for MirDB homepage browser compatibility tests.
 * Tests Chrome, Firefox, Safari (WebKit), and Edge rendering.
 *
 * Owner: Scenario 12 - Browser Compatibility
 */
module.exports = defineConfig({
  testDir: './',
  timeout: 30000,
  expect: {
    timeout: 5000
  },
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',

  use: {
    // Base URL will be set when running tests
    baseURL: process.env.BASE_URL || 'http://localhost:9999',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },

  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        // Chrome-specific settings for best compatibility
        launchOptions: {
          args: ['--disable-web-security']
        }
      },
    },
    {
      name: 'firefox',
      use: {
        ...devices['Desktop Firefox'],
      },
    },
    {
      name: 'webkit',
      use: {
        ...devices['Desktop Safari'],
      },
    },
    // Edge is Chromium-based, test using same engine
    {
      name: 'edge',
      use: {
        ...devices['Desktop Edge'],
      },
    },
  ],
});
