// @ts-check
const { defineConfig, devices } = require('@playwright/test');
const { execSync } = require('child_process');

// Check if Edge is available on this system
function isEdgeAvailable() {
  try {
    execSync('which msedge || which microsoft-edge || which microsoft-edge-stable', { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

// Base projects (available on all systems)
const baseProjects = [
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

// Add Edge only if available (Edge uses Chromium engine, so chromium tests provide coverage)
const projects = isEdgeAvailable()
  ? [...baseProjects, { name: 'msedge', use: { ...devices['Desktop Edge'], channel: 'msedge' } }]
  : baseProjects;

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'file://' + process.cwd(),
    trace: 'on-first-retry',
    viewport: { width: 1280, height: 720 },
  },
  projects,
});
