/**
 * Playwright E2E test configuration.
 *
 * This file is created by the first scenario builder.
 * Defines browser projects, viewport sizes, and test timeouts.
 */
module.exports = {
  testDir: 'tests/e2e',
  projects: [
    { name: 'chromium', use: { browserName: 'chromium' } },
    { name: 'firefox', use: { browserName: 'firefox' } },
    { name: 'webkit', use: { browserName: 'webkit' } }
  ]
};
