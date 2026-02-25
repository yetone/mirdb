import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright configuration for performance tests.
 * Uses the production preview server (vite preview) instead of dev server
 * for more accurate Lighthouse measurements.
 */
export default defineConfig({
  testDir: './tests/e2e',
  testMatch: 'performance.spec.ts',
  fullyParallel: false, // Lighthouse tests must run serially
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1, // Single worker for Lighthouse
  reporter: 'html',
  timeout: 120000, // Longer timeout for Lighthouse audits
  use: {
    baseURL: 'http://localhost:4173',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npm run build && npm run preview',
    url: 'http://localhost:4173',
    reuseExistingServer: !process.env.CI,
    timeout: 180000, // Build + preview startup time
  },
});
