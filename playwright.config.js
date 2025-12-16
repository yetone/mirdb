// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright configuration for MirDB homepage E2E tests.
 * Supports cross-browser testing per NFR-6 (Chrome, Firefox, Safari, Edge).
 */
module.exports = defineConfig({
  testDir: './tests/e2e',
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* Retry on CI only */
  retries: process.env.CI ? 2 : 0,
  /* Reporter to use */
  reporter: 'list',
  /* Test timeout - increased for slower browsers */
  timeout: 60000,
  /* Shared settings for all the projects below */
  use: {
    /* Collect trace when retrying the failed test */
    trace: 'on-first-retry',
    /* Headless mode */
    headless: true,
  },

  /* Configure projects for major browsers per NFR-6 */
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
    {
      name: 'edge',
      use: { ...devices['Desktop Edge'] },
    },
  ],
});
