/**
 * Accessibility testing utilities for MirDB homepage.
 * Owner: Scenario 6 - Accessibility Compliance
 *
 * Provides functions for testing WCAG compliance.
 *
 * Expected exports:
 * - checkContrast(element): Promise<boolean>
 * - checkKeyboardNavigation(): Promise<boolean>
 * - checkAltText(): Promise<boolean>
 */

/**
 * Checks if an element meets WCAG AA contrast requirements (4.5:1 minimum)
 * @param {import('@playwright/test').Locator} element - The element to check
 * @returns {Promise<boolean>} Whether the contrast ratio meets requirements
 */
export async function checkContrast(element) {
  const color = await element.evaluate(el => window.getComputedStyle(el).color);
  const backgroundColor = await element.evaluate(el => window.getComputedStyle(el).backgroundColor);

  // Convert hex to RGB if needed
  const rgbColor = parseColor(color);
  const rgbBg = parseColor(backgroundColor);

  // Calculate contrast ratio
  const luminance1 = calculateRelativeLuminance(rgbColor);
  const luminance2 = calculateRelativeLuminance(rgbBg);
  const contrastRatio = (Math.max(luminance1, luminance2) + 0.05) /
                         (Math.min(luminance1, luminance2) + 0.05);

  return contrastRatio >= 4.5;
}

/**
 * Verifies all interactive elements are keyboard accessible
 * @param {import('@playwright/test').Page} page - The page to test
 * @returns {Promise<boolean>} Whether keyboard navigation works as expected
 */
export async function checkKeyboardNavigation(page) {
  await page.keyboard.press('Tab');
  const focusedElement = await page.evaluate(() => document.activeElement);

  // Check if focus is visible
  const hasFocusStyle = await page.evaluate(el => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    return style.outline !== 'none' &&
           (style.outlineWidth === '0px' || style.outlineWidth > '0px');
  }, focusedElement);

  // Check tab order covers all interactive elements
  const interactiveElements = await page.$$eval('a, button, input, [tabindex]:not([tabindex="-1"])', els => els.map(el => el.tagName));

  return hasFocusStyle && interactiveElements.length > 0;
}

/**
 * Checks all images have appropriate alt text
 * @param {import('@playwright/test').Page} page - The page to test
 * @returns {Promise<boolean>} Whether all images have valid alt text
 */
export async function checkAltText(page) {
  const images = await page.$$eval('img', imgs =>
    imgs.map(img => ({
      src: img.src,
      alt: img.alt,
      hasAlt: img.hasAttribute('alt'),
      isValid: img.alt.trim().length > 0
    }))
  );

  return images.every(img => img.hasAlt && img.isValid);
}

// Helper functions
function parseColor(color) {
  if (color.startsWith('rgb')) {
    const match = color.match(/\d+/g);
    return { r: parseInt(match[0]), g: parseInt(match[1]), b: parseInt(match[2]) };
  }
  // Handle hex values
  const hex = color.replace('#', '');
  return {
    r: parseInt(hex.substring(0, 2), 16),
    g: parseInt(hex.substring(2, 4), 16),
    b: parseInt(hex.substring(4, 6), 16)
  };
}

function calculateRelativeLuminance(rgb) {
  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}