/**
 * Playwright Configuration
 * Owner: First builder (shared resource)
 */

import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './website/tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  timeout: 30000,

  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'mobile',
      use: { ...devices['iPhone 12'] },
    },
    {
      name: 'tablet',
      use: {
        viewport: { width: 768, height: 1024 },
      },
    },
  ],

  webServer: {
    command: 'npx serve website -p 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 120000,
  },
});
