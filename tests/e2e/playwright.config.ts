/**
 * Playwright Configuration for MirDB Browser Compatibility Tests
 * Owner: Scenario 10 - Browser Compatibility
 *
 * Tests the homepage rendering and functionality across modern browsers
 * as specified in NFR-4: Chrome, Firefox, Safari, Edge latest versions
 */

import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [
    ['list'],
    ['json', { outputFile: 'test-results.json' }]
  ],
  use: {
    baseURL: process.env.TEST_BASE_URL || 'http://localhost:8080',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  timeout: 30000,
  expect: {
    timeout: 5000
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
    // WebKit (Safari engine) - may not be available in all environments
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
    // Edge uses Chromium engine, tested via chromium project
  ],
});
