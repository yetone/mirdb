/**
 * MirDB Landing Page - Test Utilities
 * Owner: First scenario builder
 *
 * Shared test helpers for:
 * - Page navigation and setup
 * - Common assertions
 * - Viewport configuration
 * - Accessibility testing helpers
 */

const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
};

/**
 * Navigate to the landing page and wait for it to load
 * @param {import('@playwright/test').Page} page
 */
async function setupPage(page) {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Set viewport for a specific device type
 * @param {import('@playwright/test').Page} page
 * @param {'mobile' | 'tablet' | 'desktop'} device
 */
async function setViewport(page, device) {
  const viewport = VIEWPORTS[device];
  if (!viewport) {
    throw new Error(`Unknown device type: ${device}`);
  }
  await page.setViewportSize(viewport);
}

/**
 * Wait for CSS animations to complete
 * @param {import('@playwright/test').Page} page
 * @param {number} timeout - Timeout in milliseconds
 */
async function waitForAnimations(page, timeout = 1000) {
  await page.waitForTimeout(timeout);
}

/**
 * Check if an element has focus
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 */
async function hasFocus(page, selector) {
  const element = page.locator(selector);
  return await element.evaluate((el) => document.activeElement === el);
}

module.exports = {
  VIEWPORTS,
  setupPage,
  setViewport,
  waitForAnimations,
  hasFocus,
};
