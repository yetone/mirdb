import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright configuration for performance tests
 * Uses production build (npm run preview) for accurate performance metrics
 */
export default defineConfig({
  testDir: './tests',
  testMatch: '**/performance*.spec.ts',
  fullyParallel: false, // Run performance tests sequentially for consistency
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1, // Single worker for consistent performance measurements
  reporter: [['html', { outputFolder: 'playwright-report-performance' }], ['list']],
  use: {
    baseURL: 'http://localhost:4321',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    // Use production build for accurate performance testing
    command: 'npm run build && npm run preview',
    url: 'http://localhost:4321',
    reuseExistingServer: !process.env.CI,
    timeout: 180 * 1000, // Allow more time for build
  },
});
