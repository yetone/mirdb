/**
 * Playwright Configuration for Cross-Browser Testing
 * Owner: Scenario 13 - Cross-Browser Compatibility
 *
 * Configured browsers:
 * - Chromium (Chrome)
 * - Firefox
 * - WebKit (Safari)
 * - Edge (requires MS Edge installed locally with channel: 'msedge')
 *
 * Note: Edge tests use the msedge channel which requires Microsoft Edge
 * to be installed on the system. To skip Edge tests on systems without
 * Edge installed, run: npx playwright test --project=chromium --project=firefox --project=webkit
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
    baseURL: 'http://localhost:4173',
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
      use: { ...devices['Desktop Edge'], channel: 'msedge' },
    },
  ],
  webServer: {
    command: 'npm run preview',
    url: 'http://localhost:4173',
    reuseExistingServer: !process.env.CI,
  },
});
