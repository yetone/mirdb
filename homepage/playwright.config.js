// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright configuration for MirDB homepage E2E tests
 * @see https://playwright.dev/docs/test-configuration
 */
module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'file://' + __dirname + '/public/index.html',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1920, height: 1080 },
      },
    },
  ],
  webServer: {
    command: 'npx serve public -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
