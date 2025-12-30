// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright configuration for cross-browser testing
 * Tests MirDB homepage across Chrome, Firefox, Safari (WebKit), and Edge
 *
 * Note: Edge uses the same Chromium rendering engine as Chrome, so Chromium tests
 * effectively cover Edge compatibility. To run Edge tests specifically, ensure
 * Microsoft Edge is installed and uncomment the 'edge' project below.
 */
module.exports = defineConfig({
  testDir: './tests',
  testMatch: '**/cross-browser*.spec.js',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: `file://${process.cwd()}/index.html`,
    trace: 'on-first-retry',
  },

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
    // Edge uses the same Chromium engine - uncomment if Edge browser is installed
    // {
    //   name: 'edge',
    //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
    // },
  ],
});
