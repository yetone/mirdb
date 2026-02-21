// @ts-check
const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [['list'], ['html', { open: 'never' }]],
  timeout: 30000,
  use: {
    baseURL: 'http://localhost:1316/mirdb/',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: `${process.env.HOME}/.local/bin/hugo server --bind 0.0.0.0 -p 1316`,
    url: 'http://localhost:1316/mirdb/',
    reuseExistingServer: true,
    timeout: 60000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
