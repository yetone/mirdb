/**
 * Shared Test Utilities
 *
 * Helpers for E2E tests:
 * - waitForPageLoad() - Wait for page to fully load
 * - getContrastRatio() - Calculate color contrast
 * - simulateNetwork() - Simulate network conditions
 * - getComputedStyle() - Get computed style property value
 * - isInViewport() - Check if element is visible in viewport
 * - checkAccessibility() - Run accessibility checks
 *
 * These utilities can be used by any test file.
 */

/**
 * Wait for the page to fully load
 * @param {import('@playwright/test').Page} page
 */
async function waitForPageLoad(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Calculate color contrast ratio between two colors
 * @param {string} color1 - RGB or hex color
 * @param {string} color2 - RGB or hex color
 * @returns {number} Contrast ratio
 */
function getContrastRatio(color1, color2) {
  const getLuminance = (r, g, b) => {
    const [rs, gs, bs] = [r, g, b].map((c) => {
      c = c / 255;
      return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
  };

  const parseColor = (color) => {
    if (color.startsWith('#')) {
      const hex = color.slice(1);
      return [
        parseInt(hex.slice(0, 2), 16),
        parseInt(hex.slice(2, 4), 16),
        parseInt(hex.slice(4, 6), 16),
      ];
    }
    const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (match) {
      return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
    }
    return [0, 0, 0];
  };

  const [r1, g1, b1] = parseColor(color1);
  const [r2, g2, b2] = parseColor(color2);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Simulate network conditions
 * @param {import('@playwright/test').Page} page
 * @param {'slow3G' | 'fast3G' | 'offline'} condition
 */
async function simulateNetwork(page, condition) {
  const cdpSession = await page.context().newCDPSession(page);

  const conditions = {
    slow3G: { downloadThroughput: 50000, uploadThroughput: 50000, latency: 2000 },
    fast3G: { downloadThroughput: 180000, uploadThroughput: 75000, latency: 150 },
    offline: { offline: true, downloadThroughput: 0, uploadThroughput: 0, latency: 0 },
  };

  await cdpSession.send('Network.emulateNetworkConditions', {
    offline: false,
    ...conditions[condition],
  });
}

/**
 * Check if an element is visible in the viewport
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 * @returns {Promise<boolean>}
 */
async function isInViewport(page, selector) {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return false;
    const rect = element.getBoundingClientRect();
    return (
      rect.top >= 0 &&
      rect.left >= 0 &&
      rect.bottom <= window.innerHeight &&
      rect.right <= window.innerWidth
    );
  }, selector);
}

/**
 * Get computed style property value
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 * @param {string} property
 * @returns {Promise<string>}
 */
async function getComputedStyle(page, selector, property) {
  return await page.evaluate(([sel, prop]) => {
    const element = document.querySelector(sel);
    if (!element) return '';
    return window.getComputedStyle(element).getPropertyValue(prop);
  }, [selector, property]);
}

module.exports = {
  waitForPageLoad,
  getContrastRatio,
  simulateNetwork,
  isInViewport,
  getComputedStyle,
};
