// @ts-check
const { defineConfig, devices } = require('@playwright/test');
const fs = require('fs');

// Check if Edge is available on the system
const isEdgeAvailable = (() => {
  const edgePaths = [
    '/opt/microsoft/msedge/msedge',           // Linux
    '/usr/bin/microsoft-edge',                 // Linux alternative
    '/Applications/Microsoft Edge.app',        // macOS
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe', // Windows
  ];
  return edgePaths.some(p => fs.existsSync(p));
})();

// Base browser projects (always available via Playwright)
const projects = [
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
];

// Add Edge project only if Edge is installed on the system
// Note: Edge uses Chromium engine, so chromium tests provide equivalent coverage
if (isEdgeAvailable) {
  projects.push({
    name: 'msedge',
    use: { ...devices['Desktop Edge'], channel: 'msedge' },
  });
}

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
  },
  projects,
  webServer: {
    command: 'npx serve . -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 120000,
  },
});
