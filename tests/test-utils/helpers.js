/**
 * Shared Test Utilities
 * Common helper functions for MirDB homepage tests
 */

const path = require('path');

/**
 * Get the file URL for the index.html page
 * @returns {string} File URL to the homepage
 */
function getPageUrl() {
  return `file://${path.resolve(process.cwd(), 'index.html')}`;
}

/**
 * Load the homepage in a Playwright page
 * @param {import('@playwright/test').Page} page - Playwright page instance
 * @returns {Promise<void>}
 */
async function loadPage(page) {
  await page.goto(getPageUrl());
}

/**
 * Check if an element exists on the page
 * @param {import('@playwright/test').Page} page - Playwright page instance
 * @param {string} selector - CSS selector
 * @returns {Promise<boolean>}
 */
async function checkElementExists(page, selector) {
  const element = await page.locator(selector);
  return await element.count() > 0;
}

/**
 * Check text content of an element
 * @param {import('@playwright/test').Page} page - Playwright page instance
 * @param {string} selector - CSS selector
 * @param {string} expectedText - Expected text content
 * @returns {Promise<boolean>}
 */
async function checkTextContent(page, selector, expectedText) {
  const element = await page.locator(selector);
  const text = await element.textContent();
  return text && text.includes(expectedText);
}

/**
 * Check attribute value of an element
 * @param {import('@playwright/test').Page} page - Playwright page instance
 * @param {string} selector - CSS selector
 * @param {string} attr - Attribute name
 * @param {string} expectedValue - Expected attribute value
 * @returns {Promise<boolean>}
 */
async function checkAttribute(page, selector, attr, expectedValue) {
  const element = await page.locator(selector);
  const value = await element.getAttribute(attr);
  return value && value.includes(expectedValue);
}

/**
 * Measure page load time
 * @param {import('@playwright/test').Page} page - Playwright page instance
 * @returns {Promise<number>} Load time in milliseconds
 */
async function measureLoadTime(page) {
  const startTime = Date.now();
  await page.goto(getPageUrl());
  await page.waitForLoadState('domcontentloaded');
  return Date.now() - startTime;
}

/**
 * Common viewport sizes for responsive testing
 */
const viewportSizes = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1920, height: 1080 },
};

module.exports = {
  getPageUrl,
  loadPage,
  checkElementExists,
  checkTextContent,
  checkAttribute,
  measureLoadTime,
  viewportSizes,
};
