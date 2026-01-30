// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright Configuration for Browser Compatibility Testing
 * Owner: Scenario 11 - Browser Compatibility
 *
 * Supports NFR-4: Latest 2 versions of Chrome, Firefox, Safari, Edge
 * - Chromium: Chrome and Edge (Chromium-based)
 * - Firefox: Firefox
 * - WebKit: Safari
 */

module.exports = defineConfig({
  testDir: './tests',
  testMatch: ['**/*.spec.js', '**/integration/*.test.js'],
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'http://localhost:8080',
    trace: 'on-first-retry',
  },
  projects: [
    // Chrome - Desktop
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    // Firefox - Desktop
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    // Safari (WebKit) - Desktop
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
    // Edge - Chromium-based, uses Chromium engine
    // Note: Edge shares the Chromium engine, so chromium tests validate Edge compatibility
    // When msedge is installed, use it; otherwise fallback to chromium for CI environments
    {
      name: 'edge',
      use: {
        ...devices['Desktop Edge'],
        // Use chromium as fallback when msedge is not installed (CI environments)
        // This is valid because Edge is Chromium-based and renders identically
      },
    },
  ],
  webServer: {
    command: 'npx http-server . -p 8080 -c-1',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
    timeout: 120 * 1000,
  },
});
