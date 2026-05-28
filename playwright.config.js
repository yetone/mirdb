/**
 * Playwright E2E Test Configuration
 * Owner: Scenario 11 - Performance and Static Site Build
 *
 * Expected contents:
 * - Test directory: tests/e2e
 * - Base URL: http://localhost:3000 (or configured port)
 * - Viewport presets: desktop (1920x1080), tablet (768x1024), mobile (375x667)
 * - Screenshot and trace options for debugging
 * - axe-core integration for accessibility tests
 */

const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx serve src -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
