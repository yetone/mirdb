// @ts-check
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'file://' + __dirname,
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        browserName: 'chromium',
        headless: true,
      },
    },
    {
      name: 'firefox',
      use: {
        browserName: 'firefox',
        headless: true,
      },
    },
    {
      name: 'webkit',
      use: {
        browserName: 'webkit',
        headless: true,
      },
    },
  ],
  webServer: undefined,
});
