/**
 * Playwright Configuration for MirDB Homepage
 * Owner: First test builder
 *
 * Configuration:
 * - Base URL for local dev server
 * - Browser projects (Chrome, Firefox, Safari, Edge)
 * - Viewport sizes for responsive tests
 * - Test timeout settings
 * - Reporter configuration
 * - Web server config for serving static files
 */

import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './',
  testMatch: ['e2e/**/*.spec.ts', 'integration/**/*.spec.ts'],
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:8080',
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
  ],
  webServer: {
    command: 'npx http-server .. -p 8080 -c-1',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
    cwd: __dirname,
  },
});
