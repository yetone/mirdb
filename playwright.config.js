const { defineConfig, devices } = require('@playwright/test');

/**
 * Playwright configuration for cross-browser testing
 * NFR-4: Page must render correctly in modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * Browser Coverage:
 * - Chromium: Covers Chrome and Edge (both use Chromium engine)
 * - Firefox: Mozilla Firefox
 * - WebKit: Covers Safari
 */
module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
  },
  /* Configure projects for major browsers */
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
    /* Edge uses Chromium engine, covered by chromium project */
    /* For explicit Edge testing, uncomment below: */
    // {
    //   name: 'msedge',
    //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
    // },
  ],
  webServer: {
    command: 'npx http-server -p 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
