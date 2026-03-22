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
    {
      name: 'edge',
      use: {
        ...devices['Desktop Chrome'],
        // Microsoft Edge uses the Chromium engine, so we test with Chromium
        // using Edge-specific user agent to verify Edge-specific behavior
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
      },
    },
  ],
  webServer: {
    command: 'npx http-server .. -p 8080 -c-1',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
    cwd: __dirname,
  },
});
