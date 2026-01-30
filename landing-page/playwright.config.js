// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Cross-Browser Testing Configuration
 *
 * This configuration supports testing on:
 * - Chromium (Chrome and Edge share the same rendering engine)
 * - Firefox
 * - WebKit (Safari)
 *
 * Note: Edge uses Chromium's Blink engine, so tests passing on Chromium
 * will behave identically on Edge. The 'edge' project requires MS Edge
 * to be installed locally and is commented out for CI environments.
 */
module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:3000',
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
    // Uncomment to test on Edge (requires MS Edge installed locally)
    // Edge uses the same Chromium/Blink engine as Chrome, so Chromium tests verify Edge compatibility
    // {
    //   name: 'edge',
    //   use: {
    //     ...devices['Desktop Edge'],
    //     channel: 'msedge',
    //   },
    // },
  ],
  webServer: {
    command: 'npx serve . -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
