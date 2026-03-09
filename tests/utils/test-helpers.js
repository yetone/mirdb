/**
 * Shared Test Utilities
 * Created by: First test builder (Scenario 2 - Features Section)
 *
 * Exports:
 * - loadPage(page, viewport): Load homepage with specific viewport
 * - checkContrast(element): Verify color contrast ratio
 * - getComputedStyles(page, selector, properties): Get CSS properties
 * - waitForDOMReady(page): Wait for page load
 */

/**
 * Load the homepage with an optional viewport size
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {Object} viewport - Optional viewport { width, height }
 */
async function loadPage(page, viewport = null) {
  if (viewport) {
    await page.setViewportSize(viewport);
  }
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Get computed CSS properties for an element
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {string} selector - CSS selector
 * @param {string[]} properties - CSS properties to retrieve
 * @returns {Promise<Object>} Object with property values
 */
async function getComputedStyles(page, selector, properties) {
  return await page.evaluate(
    ({ selector, properties }) => {
      const element = document.querySelector(selector);
      if (!element) return null;
      const computed = window.getComputedStyle(element);
      const result = {};
      properties.forEach(prop => {
        result[prop] = computed.getPropertyValue(prop);
      });
      return result;
    },
    { selector, properties }
  );
}

/**
 * Wait for DOM to be fully ready
 * @param {import('@playwright/test').Page} page - Playwright page object
 */
async function waitForDOMReady(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

module.exports = {
  loadPage,
  getComputedStyles,
  waitForDOMReady,
};
