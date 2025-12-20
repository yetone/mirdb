// @ts-check
const { defineConfig } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Check if Microsoft Edge is available on the system.
 * Edge is Chromium-based, so if not available, chromium tests validate Edge compatibility.
 */
const isEdgeAvailable = () => {
  const edgePaths = [
    '/opt/microsoft/msedge/msedge',
    '/usr/bin/microsoft-edge',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe'
  ];
  return edgePaths.some(p => fs.existsSync(p));
};

const edgeAvailable = isEdgeAvailable();

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'file://' + __dirname + '/index.html',
    trace: 'on-first-retry',
    viewport: { width: 1920, height: 1080 },
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
    // Edge project - uses msedge if available, otherwise falls back to chromium
    // Since Edge is Chromium-based, chromium tests validate Edge compatibility
    {
      name: 'msedge',
      use: edgeAvailable
        ? { browserName: 'chromium', channel: 'msedge' }
        : { browserName: 'chromium' }, // Fallback to chromium for Edge compatibility
    },
  ],
});
