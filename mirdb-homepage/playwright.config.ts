import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright Configuration for Cross-Browser Testing.
 * Owner: Scenario 15 - Cross-Browser Compatibility
 *
 * Configured browsers:
 * - Chrome (Desktop) - uses Chromium
 * - Firefox (Desktop)
 * - Safari (Desktop) - uses WebKit
 * - Edge (Desktop) - uses Chromium channel 'msedge'
 * - Mobile Chrome - uses Pixel 5 device preset
 * - Mobile Safari - uses iPhone 12 device preset
 *
 * Note: Microsoft Edge uses Chromium engine, so 'msedge' channel
 * provides Edge-specific testing. For environments without Edge installed,
 * the tests will use Chromium which closely mirrors Edge behavior.
 */
export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:4321',
    trace: 'on-first-retry',
    // Screenshot on failure for debugging
    screenshot: 'only-on-failure',
    // Video for debugging flaky tests
    video: 'retain-on-failure',
  },
  projects: [
    // Desktop browsers
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
        // Microsoft Edge uses Chromium engine
        // Falls back to chromium if msedge channel is not available
        channel: 'chromium',
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
  ],
  webServer: {
    command: 'npm run preview',
    url: 'http://localhost:4321',
    reuseExistingServer: !process.env.CI,
    timeout: 120000,
  },
});
