/**
 * Base test setup for MirDB homepage e2e tests.
 * Owner: First scenario builder
 *
 * Creates page objects and common test utilities.
 * Provides access to the homepage URL and standard configuration.
 *
 * Expected exports:
 * - setupHomePage(): Promise<Page>
 * - DEFAULT_CONFIG: { port: 12333, workDir: '/tmp/mirdb' }
 */

// Note: This file should be populated by the first scenario builder
// with actual implementation for setting up the test environment.

const { chromium } = require('playwright');

const DEFAULT_CONFIG = {
  port: 12333,
  workDir: '/tmp/mirdb'
};

async function setupHomePage() {
  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto('http://localhost:8080');
  return page;
}

module.exports = {
  setupHomePage,
  DEFAULT_CONFIG
};