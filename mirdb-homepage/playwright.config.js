/**
 * Playwright Configuration
 *
 * Configure test browsers, viewport sizes, and timeouts.
 *
 * Browsers to test:
 * - Chromium (Chrome)
 * - Firefox
 * - WebKit (Safari)
 *
 * Viewports:
 * - Mobile: 375x667
 * - Tablet: 768x1024
 * - Desktop: 1440x900
 */
const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'file://' + __dirname,
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx serve . -p 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
