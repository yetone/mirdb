/**
 * Shared Test Utilities
 * Created by: First test builder
 *
 * Exports:
 * - loadPage(viewport): Load homepage with specific viewport
 * - checkContrast(element): Verify color contrast ratio
 * - getComputedStyles(element, properties): Get CSS properties
 * - waitForDOMReady(): Wait for page load
 */

/**
 * Load page with specific viewport dimensions
 * @param {import('@playwright/test').Page} page
 * @param {Object} viewport - { width, height }
 */
async function loadPage(page, viewport = { width: 1280, height: 720 }) {
  await page.setViewportSize(viewport);
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Check if an element has sufficient color contrast
 * @param {import('@playwright/test').Locator} element
 * @returns {Promise<{ratio: number, passes: boolean}>}
 */
async function checkContrast(element) {
  const styles = await element.evaluate(el => {
    const computed = window.getComputedStyle(el);
    return {
      color: computed.color,
      backgroundColor: computed.backgroundColor
    };
  });

  // Parse RGB values and calculate contrast
  const parseRGB = (str) => {
    const match = str.match(/\d+/g);
    return match ? match.map(Number) : [0, 0, 0];
  };

  const luminance = (rgb) => {
    const [r, g, b] = rgb.map(v => {
      v /= 255;
      return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };

  const fg = parseRGB(styles.color);
  const bg = parseRGB(styles.backgroundColor);
  const l1 = luminance(fg);
  const l2 = luminance(bg);

  const ratio = (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);

  return {
    ratio,
    passes: ratio >= 4.5
  };
}

/**
 * Get computed styles for an element
 * @param {import('@playwright/test').Locator} element
 * @param {string[]} properties
 */
async function getComputedStyles(element, properties) {
  return await element.evaluate((el, props) => {
    const computed = window.getComputedStyle(el);
    const result = {};
    props.forEach(prop => {
      result[prop] = computed.getPropertyValue(prop);
    });
    return result;
  }, properties);
}

/**
 * Wait for DOM to be fully ready
 * @param {import('@playwright/test').Page} page
 */
async function waitForDOMReady(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForFunction(() => document.readyState === 'complete');
}

module.exports = {
  loadPage,
  checkContrast,
  getComputedStyles,
  waitForDOMReady
};
