import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright Configuration
 *
 * E2E testing configuration for the URL Shortener frontend.
 * Includes mobile viewport testing for responsive design validation.
 * Includes cross-browser compatibility testing (NFR-6, NFR-7).
 * Owner: Scenario 8 - Theme Support and Toggle (extended)
 * Extended by: Scenario 16 - Cross-Browser Compatibility
 */
export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',

  use: {
    baseURL: 'http://localhost:5173',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },

  projects: [
    // Desktop Chrome (Primary target browser - NFR-6: Chrome 90+)
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    // Firefox Desktop (NFR-6: Firefox 88+)
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    // WebKit/Safari (NFR-6: Safari 14+)
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
    // Edge (NFR-6: Edge 90+ - Chromium-based)
    {
      name: 'msedge',
      use: { ...devices['Desktop Edge'], channel: 'msedge' },
    },
    // Mobile Chrome
    {
      name: 'Mobile Chrome',
      use: {
        ...devices['Pixel 5'],
        viewport: { width: 320, height: 568 },
      },
    },
    // Mobile Safari
    {
      name: 'Mobile Safari',
      use: {
        ...devices['iPhone 12'],
        viewport: { width: 390, height: 844 },
      },
    },
    // Desktop Chrome with specific viewport
    {
      name: 'Desktop Chrome',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1280, height: 720 },
      },
    },
  ],

  webServer: {
    command: 'npm run dev',
    url: 'http://localhost:5173',
    reuseExistingServer: true,
    timeout: 120000,
  },
});
