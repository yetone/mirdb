/**
 * Playwright E2E Test Configuration
 * Owner: First scenario writing E2E tests
 *
 * Configuration:
 * - Test directory: tests/e2e/
 * - Browsers: Chrome, Firefox, Safari, Edge (Scenario 18)
 * - Viewport sizes for responsive tests (Scenario 10-12)
 * - Base URL for local dev server
 *
 * Cross-browser compatibility (Scenario 18):
 * - chromium: Tests Chrome compatibility
 * - firefox: Tests Firefox compatibility
 * - webkit: Tests Safari compatibility
 * - edge: Tests Edge (requires msedge installed, skipped otherwise)
 *   Note: Edge is Chromium-based, so chromium tests verify Edge compatibility
 */
import { defineConfig, devices } from '@playwright/test';
import { execSync } from 'child_process';

// Check if Edge is installed
const isEdgeAvailable = (() => {
  try {
    execSync('which microsoft-edge 2>/dev/null || which msedge 2>/dev/null');
    return true;
  } catch {
    return false;
  }
})();

// Base browser projects (always available)
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

// Add Edge project only if Edge is installed
const projects = isEdgeAvailable
  ? [...baseProjects, { name: 'edge', use: { ...devices['Desktop Edge'], channel: 'msedge' } }]
  : baseProjects;

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: 'http://localhost:8080',
    trace: 'on-first-retry',
  },
  projects,
  webServer: {
    command: 'npm run build && npx http-server docs -p 8080 -c-1 --silent',
    url: 'http://localhost:8080',
    reuseExistingServer: !process.env.CI,
    timeout: 120000,
  },
});
