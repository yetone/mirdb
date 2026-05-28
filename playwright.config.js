/**
 * Playwright E2E Test Configuration
 * Owner: Scenario 11 - Performance and Static Site Build
 *
 * Expected contents:
 * - Test directory: tests/e2e
 * - Base URL: http://localhost:3000 (or configured port)
 * - Viewport presets: desktop (1920x1080), tablet (768x1024), mobile (375x667)
 * - Screenshot and trace options for debugging
 * - axe-core integration for accessibility tests
 */

const { defineConfig, devices } = require('@playwright/test');
const path = require('path');

// Use full Chromium if available; falls back to system Chrome or headless shell
function findChromiumPath() {
  const candidates = [
    process.env.PW_CHROMIUM_PATH,
    process.env.CHROME_PATH,
    path.join(require('os').homedir(), '.cache/ms-playwright/chromium-1223/chrome-linux64/chrome'),
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ];
  const fs = require('fs');
  for (const p of candidates) {
    if (p && fs.existsSync(p)) return p;
  }
  return undefined;
}

const chromiumPath = findChromiumPath();

module.exports = defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
    launchOptions: chromiumPath ? { executablePath: chromiumPath } : {},
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx serve src -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
