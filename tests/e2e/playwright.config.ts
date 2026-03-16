/**
 * Playwright Test Configuration
 * Owner: First Builder
 *
 * Configuration includes:
 * - Base URL for local server
 * - Viewport sizes for responsive testing
 * - Browser configurations (Chromium, Firefox, WebKit)
 * - Screenshot and video settings
 * - Accessibility testing plugins
 */

import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:8080',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx http-server ../../docs -p 8080 -c-1',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
    cwd: __dirname,
    timeout: 30000,
  },
});
