/**
 * Playwright Configuration
 * Owner: Scenario 7 - Responsive Design - Mobile
 */
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:1111',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'Desktop Chrome',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: `${process.env.HOME}/.local/bin/zola serve --port 1111`,
    url: 'http://127.0.0.1:1111',
    reuseExistingServer: true,
    cwd: '/workspace/homepage',
    timeout: 60000,
  },
});
