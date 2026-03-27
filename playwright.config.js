/**
 * Playwright E2E Test Configuration
 * Owner: First scenario writing E2E tests
 *
 * Configuration:
 * - Test directory: tests/e2e/
 * - Browsers: Chrome, Firefox, Safari, Edge (Scenario 18)
 * - Viewport sizes for responsive tests (Scenario 10-12)
 * - Base URL for local dev server
 */
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
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
  ],
  webServer: {
    command: 'npm run serve',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
  },
});
