// @ts-check
const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright Configuration for Cross-Browser Testing
 *
 * This configuration enables testing across multiple browsers:
 * - chromium: Google Chrome / Chromium (primary browser)
 * - firefox: Mozilla Firefox
 * - webkit: Safari (WebKit engine)
 * - edge: Microsoft Edge (optional - requires Edge installed)
 *
 * Edge testing is commented out by default as it requires Edge browser
 * to be installed on the system. Uncomment to enable when Edge is available.
 * Note: Edge uses Chromium engine, so chromium tests provide good coverage.
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
    // Edge testing (optional - requires Microsoft Edge installed)
    // Edge uses Chromium engine, so chromium tests provide equivalent coverage
    // Uncomment for local testing when Edge is available:
    // {
    //   name: 'edge',
    //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
    // },
  ],
  webServer: {
    command: 'npx serve -l 3000 .',
    url: 'http://localhost:3000',
    reuseExistingServer: true,
    timeout: 120000,
  },
});
