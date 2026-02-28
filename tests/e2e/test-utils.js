/**
 * Shared Test Utilities
 * Owner: First Builder
 *
 * Purpose: Shared helpers for e2e tests
 */

const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

/**
 * Wait for page to fully load
 * @param {import('@playwright/test').Page} page
 */
async function waitForPageLoad(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Get computed CSS styles for an element
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 * @returns {Promise<CSSStyleDeclaration>}
 */
async function getComputedStyles(page, selector) {
  return await page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return null;
    return window.getComputedStyle(element);
  }, selector);
}

/**
 * Check if element has visible focus indicator
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 * @returns {Promise<boolean>}
 */
async function hasVisibleFocusIndicator(page, selector) {
  const element = page.locator(selector);
  await element.focus();

  const styles = await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const computed = window.getComputedStyle(el);
    return {
      outline: computed.outline,
      outlineWidth: computed.outlineWidth,
      outlineStyle: computed.outlineStyle,
      outlineColor: computed.outlineColor,
      boxShadow: computed.boxShadow,
    };
  }, selector);

  if (!styles) return false;

  // Check for visible outline or box-shadow
  const hasOutline = styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none';
  const hasBoxShadow = styles.boxShadow !== 'none';

  return hasOutline || hasBoxShadow;
}

module.exports = {
  VIEWPORTS,
  waitForPageLoad,
  getComputedStyles,
  hasVisibleFocusIndicator,
};
