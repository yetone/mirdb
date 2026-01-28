/**
 * Shared Test Utilities
 *
 * Common utilities used across unit and E2E tests.
 *
 * Expected exports:
 * - loadHomepage(): Load the homepage for testing
 * - getElement(selector): Query element helper
 * - VIEWPORT_SIZES: Common viewport dimensions
 */

const fs = require('fs');
const path = require('path');

/**
 * Common viewport sizes for responsive testing
 */
const VIEWPORT_SIZES = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
  largeDesktop: { width: 1920, height: 1080 }
};

/**
 * Load the homepage HTML content for unit testing
 * @returns {string} HTML content of index.html
 */
function loadHomepageHTML() {
  const htmlPath = path.join(__dirname, '../../index.html');
  return fs.readFileSync(htmlPath, 'utf8');
}

/**
 * Setup JSDOM document with homepage content
 * @returns {Document} JSDOM document object
 */
function setupDocument() {
  const html = loadHomepageHTML();
  document.documentElement.innerHTML = html;
  return document;
}

/**
 * Get element by selector with helpful error message
 * @param {string} selector - CSS selector
 * @param {Document|Element} context - Optional context element
 * @returns {Element|null}
 */
function getElement(selector, context = document) {
  return context.querySelector(selector);
}

/**
 * Get all elements matching selector
 * @param {string} selector - CSS selector
 * @param {Document|Element} context - Optional context element
 * @returns {NodeList}
 */
function getAllElements(selector, context = document) {
  return context.querySelectorAll(selector);
}

/**
 * Wait for an element to appear (for E2E tests)
 * @param {import('@playwright/test').Page} page - Playwright page object
 * @param {string} selector - CSS selector
 * @param {number} timeout - Timeout in milliseconds
 * @returns {Promise<import('@playwright/test').Locator>}
 */
async function waitForElement(page, selector, timeout = 5000) {
  const locator = page.locator(selector);
  await locator.waitFor({ state: 'visible', timeout });
  return locator;
}

module.exports = {
  VIEWPORT_SIZES,
  loadHomepageHTML,
  setupDocument,
  getElement,
  getAllElements,
  waitForElement
};
