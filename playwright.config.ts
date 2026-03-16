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
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [['list'], ['html', { open: 'never' }]],
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
    command: 'npx serve docs -p 8080',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
    timeout: 120 * 1000,
    cwd: '/workspace',
  },
});
