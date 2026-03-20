/**
 * Playwright Test Configuration
 *
 * Browsers (Scenario 13 - Cross-Browser):
 * - Chromium (Chrome)
 * - Firefox
 * - WebKit (Safari)
 * - Microsoft Edge
 *
 * Viewports:
 * - Desktop: 1280x720
 * - Tablet: 768x1024 (Scenario 10)
 * - Mobile: 375x667 (Scenario 9)
 *
 * Configuration:
 * - Base URL
 * - Screenshot on failure
 * - Video recording
 * - Trace collection
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
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },

  projects: [
    // Desktop browsers (Scenario 13 - Cross-Browser Compatibility)
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
      use: {
        ...devices['Desktop Edge'],
        channel: 'msedge',
      },
    },
    // Mobile browsers
    {
      name: 'Mobile Chrome',
      use: { ...devices['Pixel 5'] },
    },
    {
      name: 'Mobile Safari',
      use: { ...devices['iPhone 12'] },
    },
    {
      name: 'Tablet',
      use: {
        viewport: { width: 768, height: 1024 },
        deviceScaleFactor: 2,
      },
    },
  ],

  webServer: {
    command: 'npx http-server . -p 3000 -s',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 120 * 1000,
  },
});
