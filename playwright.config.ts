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
    // Edge is Chromium-based and shares the same rendering engine
    // On Linux/CI environments, we use chromium which validates Edge compatibility
    // Uncomment below to test Edge specifically on Windows:
    // {
    //   name: 'msedge',
    //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
    // },
  ],
  webServer: {
    command: 'npx serve src -p 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
