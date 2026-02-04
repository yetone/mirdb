/**
 * Test Setup and Utilities
 * Owner: First builder (Shared)
 *
 * Common test utilities and configuration
 */

const { test, expect } = require('@playwright/test');

/**
 * Common test configuration
 */
const config = {
  baseUrl: 'http://localhost:8080',
  viewport: {
    desktop: { width: 1920, height: 1080 },
    tablet: { width: 768, height: 1024 },
    mobile: { width: 375, height: 667 }
  },
  wcag: {
    contrastRatioNormal: 4.5,
    contrastRatioLarge: 3,
  }
};

/**
 * Navigate to the homepage
 * @param {import('@playwright/test').Page} page
 */
async function goToHomepage(page) {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Check if element has visible focus
 * @param {import('@playwright/test').Locator} locator
 * @returns {Promise<boolean>}
 */
async function hasFocusIndicator(locator) {
  const outlineStyle = await locator.evaluate(el => {
    const styles = window.getComputedStyle(el);
    return {
      outline: styles.outline,
      outlineWidth: styles.outlineWidth,
      outlineStyle: styles.outlineStyle,
      boxShadow: styles.boxShadow
    };
  });

  // Check if there's a visible outline or box-shadow
  const hasOutline = outlineStyle.outlineStyle !== 'none' &&
                     outlineStyle.outlineWidth !== '0px';
  const hasBoxShadow = outlineStyle.boxShadow !== 'none';

  return hasOutline || hasBoxShadow;
}

/**
 * Press Tab key and return focused element
 * @param {import('@playwright/test').Page} page
 */
async function pressTab(page) {
  await page.keyboard.press('Tab');
  return page.locator(':focus');
}

/**
 * Check element visibility
 * @param {import('@playwright/test').Locator} locator
 */
async function isVisuallyVisible(locator) {
  const boundingBox = await locator.boundingBox();
  if (!boundingBox) return false;

  // Check if element is within viewport and has dimensions
  return boundingBox.width > 0 && boundingBox.height > 0;
}

module.exports = {
  config,
  goToHomepage,
  hasFocusIndicator,
  pressTab,
  isVisuallyVisible,
  test,
  expect
};
