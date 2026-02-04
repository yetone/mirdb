/**
 * Test Setup and Utilities
 * Owner: First builder (Shared)
 *
 * Expected exports:
 * - Test configuration
 * - Common test utilities
 * - Mock data
 * - Assertion helpers
 */

const { chromium } = require('playwright');

// Test configuration
const config = {
  baseUrl: 'http://localhost:1111',
  timeout: 30000,
  viewports: {
    desktop: { width: 1920, height: 1080 },
    tablet: { width: 768, height: 1024 },
    mobile: { width: 375, height: 667 }
  }
};

// Common test utilities
async function launchBrowser() {
  return await chromium.launch({ headless: true });
}

async function createPage(browser, viewport = config.viewports.desktop) {
  const context = await browser.newContext({ viewport });
  return await context.newPage();
}

async function navigateToHome(page) {
  await page.goto(config.baseUrl);
  await page.waitForLoadState('networkidle');
}

// Assertion helpers
function assertElementVisible(element, message) {
  if (!element) {
    throw new Error(message || 'Element not found');
  }
}

module.exports = {
  config,
  launchBrowser,
  createPage,
  navigateToHome,
  assertElementVisible
};
