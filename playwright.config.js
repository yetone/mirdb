const { defineConfig } = require('@playwright/test');
const { existsSync } = require('fs');

// Check if Microsoft Edge is installed (for msedge channel)
const edgePaths = ['/opt/microsoft/msedge/msedge', '/usr/bin/microsoft-edge'];
const isEdgeInstalled = edgePaths.some(path => existsSync(path));

module.exports = defineConfig({
  testDir: './tests',
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
      use: { browserName: 'chromium' },
    },
    {
      name: 'firefox',
      use: { browserName: 'firefox' },
    },
    {
      name: 'webkit',
      use: { browserName: 'webkit' },
    },
    // Edge uses Chromium engine, so chromium tests cover Edge compatibility
    // Only include msedge project when Edge is installed on the system
    ...(isEdgeInstalled ? [{
      name: 'msedge',
      use: { browserName: 'chromium', channel: 'msedge' },
    }] : []),
  ],
  webServer: {
    command: 'npx http-server -p 8080',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
  },
});
