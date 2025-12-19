import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright configuration for cross-browser testing
 * Supports NFR-5: Chrome, Firefox, Safari, Edge (last 2 versions)
 *
 * Note: Edge uses the same Chromium engine as Chrome, so testing on Chromium
 * effectively covers Edge compatibility. Edge-specific channel can be enabled
 * in environments where Edge is installed.
 */
export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'file://' + process.cwd() + '/public',
    trace: 'on-first-retry',
  },
  projects: [
    // Chromium (Chrome/Edge engine - Blink)
    // This covers both Chrome and Edge since Edge is Chromium-based
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    // Firefox (Gecko rendering engine)
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    // WebKit (Safari rendering engine)
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
  ],
  webServer: undefined,
});
