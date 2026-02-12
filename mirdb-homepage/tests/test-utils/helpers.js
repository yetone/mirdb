/**
 * Shared Test Utilities
 * Owner: First builder
 *
 * Provides common test functions and constants for both unit and e2e tests.
 */

const fs = require('fs');
const path = require('path');

/**
 * Load the HTML content for unit testing
 * @returns {string} The HTML content of index.html
 */
function loadHTML() {
  const htmlPath = path.join(__dirname, '../../index.html');
  return fs.readFileSync(htmlPath, 'utf-8');
}

/**
 * Load a page for Playwright testing
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {string} [urlPath='/'] - Optional path to append to base URL
 */
async function loadPage(page, urlPath = '/') {
  await page.goto(urlPath);
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Check if an element exists on the page
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {string} selector - CSS selector
 * @returns {Promise<boolean>} Whether the element exists
 */
async function checkElementExists(page, selector) {
  const element = page.locator(selector);
  return await element.count() > 0;
}

/**
 * Check if element contains specific text
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {string} selector - CSS selector
 * @param {string} text - Text to search for
 * @returns {Promise<boolean>} Whether the text exists in the element
 */
async function checkTextContent(page, selector, text) {
  const element = page.locator(selector);
  const content = await element.textContent();
  return content && content.includes(text);
}

/**
 * Check if element has specific attribute value
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {string} selector - CSS selector
 * @param {string} attr - Attribute name
 * @param {string} value - Expected attribute value
 * @returns {Promise<boolean>} Whether the attribute matches
 */
async function checkAttribute(page, selector, attr, value) {
  const element = page.locator(selector);
  const attrValue = await element.getAttribute(attr);
  return attrValue === value;
}

/**
 * Measure page load time
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @returns {Promise<number>} Load time in milliseconds
 */
async function measureLoadTime(page) {
  const timing = await page.evaluate(() => {
    const perf = performance.getEntriesByType('navigation')[0];
    return perf ? perf.loadEventEnd - perf.startTime : 0;
  });
  return timing;
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
  loadHTML,
  loadPage,
  checkElementExists,
  checkTextContent,
  checkAttribute,
  measureLoadTime,
  viewportSizes,
};
