import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:3000',
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
    // Note: Edge uses Chromium engine, so chromium tests effectively cover Edge compatibility
    // Uncomment below to test on actual Edge browser when available
    // {
    //   name: 'edge',
    //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
    // },
  ],
  webServer: {
    command: 'npx serve -l 3000 ./public',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
