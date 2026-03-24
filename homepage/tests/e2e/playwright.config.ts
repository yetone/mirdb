/**
 * Playwright Configuration
 * Owner: First scenario builder
 *
 * Configuration:
 * - Base URL for local dev server
 * - Browser configurations
 * - Test directory
 * - Screenshots on failure
 * - Viewport sizes for responsive tests
 */

import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
    testDir: '.',
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 1 : undefined,
    reporter: 'list',
    use: {
        baseURL: 'http://localhost:8080',
        trace: 'on-first-retry',
        screenshot: 'only-on-failure',
    },
    projects: [
        {
            name: 'chromium',
            use: { ...devices['Desktop Chrome'] },
        },
    ],
    webServer: {
        command: 'npx http-server src -p 8080 -c-1',
        url: 'http://localhost:8080',
        reuseExistingServer: true,
        timeout: 60000,
    },
});
