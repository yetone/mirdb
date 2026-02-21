/**
 * Playwright E2E Test Configuration
 * Owner: Scenario 20 - Cross-Browser Compatibility
 *
 * Expected configuration:
 * - Test directory: tests/e2e/
 * - Browsers: chromium, firefox, webkit (Safari)
 * - Viewport sizes for responsive testing
 * - Base URL for local development server
 */

const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  timeout: 30000,
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
    actionTimeout: 10000,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx serve book -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: true,
    timeout: 30000,
  },
});
