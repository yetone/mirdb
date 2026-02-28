/**
 * Shared Test Utilities
 * Owner: First Builder
 *
 * Purpose: Shared helpers for e2e tests
 */

const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1024, height: 768 },
};

async function waitForPageLoad(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

async function checkContrast(page, element) {
  const styles = await element.evaluate((el) => {
    const computed = window.getComputedStyle(el);
    return {
      color: computed.color,
      backgroundColor: computed.backgroundColor,
    };
  });
  return styles;
}

async function getComputedStyles(page, selector) {
  return await page.locator(selector).evaluate((el) => {
    const computed = window.getComputedStyle(el);
    return {
      color: computed.color,
      backgroundColor: computed.backgroundColor,
      fontSize: computed.fontSize,
      fontWeight: computed.fontWeight,
    };
  });
}

/**
 * Parse an RGB or RGBA color string to RGB values
 * @param {string} colorStr - Color string like "rgb(255, 255, 255)" or "rgba(255, 255, 255, 1)"
 * @returns {{r: number, g: number, b: number}} RGB values (0-255)
 */
function parseRgb(colorStr) {
  const match = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (!match) {
    throw new Error(`Cannot parse color: ${colorStr}`);
  }
  return {
    r: parseInt(match[1], 10),
    g: parseInt(match[2], 10),
    b: parseInt(match[3], 10),
  };
}

/**
 * Calculate relative luminance per WCAG 2.1
 * @param {{r: number, g: number, b: number}} rgb - RGB values (0-255)
 * @returns {number} Relative luminance (0-1)
 */
function getLuminance({ r, g, b }) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const srgb = c / 255;
    return srgb <= 0.03928 ? srgb / 12.92 : Math.pow((srgb + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors per WCAG 2.1
 * @param {string} foregroundColor - Foreground color as RGB string
 * @param {string} backgroundColor - Background color as RGB string
 * @returns {number} Contrast ratio (1 to 21)
 */
function calculateContrastRatio(foregroundColor, backgroundColor) {
  const fgRgb = parseRgb(foregroundColor);
  const bgRgb = parseRgb(backgroundColor);
  const fgLuminance = getLuminance(fgRgb);
  const bgLuminance = getLuminance(bgRgb);
  const lighter = Math.max(fgLuminance, bgLuminance);
  const darker = Math.min(fgLuminance, bgLuminance);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Get the effective background color by traversing up the DOM
 * Starts from the element itself (inclusive) to check its own background
 * @param {Page} page - Playwright page
 * @param {Locator} element - Element locator
 * @returns {Promise<string>} Effective background color
 */
async function getEffectiveBackgroundColor(page, element) {
  return await element.evaluate((el) => {
    let current = el;
    while (current) {
      const style = window.getComputedStyle(current);
      const bg = style.backgroundColor;
      // Check if background is not transparent
      if (bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
        return bg;
      }
      current = current.parentElement;
    }
    // Fallback to dark theme background
    return 'rgb(13, 17, 23)';
  });
}

/**
 * Measure contrast ratio for an element against its background
 * @param {Page} page - Playwright page
 * @param {Locator} element - Element locator
 * @returns {Promise<{ratio: number, foreground: string, background: string}>}
 */
async function measureContrastRatio(page, element) {
  const foreground = await element.evaluate((el) => {
    return window.getComputedStyle(el).color;
  });
  const background = await getEffectiveBackgroundColor(page, element);
  const ratio = calculateContrastRatio(foreground, background);
  return { ratio, foreground, background };
}

// WCAG AA contrast ratio thresholds
const WCAG_AA_NORMAL_TEXT = 4.5;
const WCAG_AA_LARGE_TEXT = 3.0;

module.exports = {
  VIEWPORTS,
  waitForPageLoad,
  checkContrast,
  getComputedStyles,
  parseRgb,
  getLuminance,
  calculateContrastRatio,
  getEffectiveBackgroundColor,
  measureContrastRatio,
  WCAG_AA_NORMAL_TEXT,
  WCAG_AA_LARGE_TEXT,
};
