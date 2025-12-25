// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * WCAG 2.1 AA Contrast Requirements:
 * - Normal text (< 18pt or < 14pt bold): 4.5:1 minimum contrast ratio
 * - Large text (>= 18pt or >= 14pt bold): 3:1 minimum contrast ratio
 * - UI components and graphical objects: 3:1 minimum contrast ratio
 */

/**
 * Calculate relative luminance according to WCAG 2.1
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} - Relative luminance (0-1)
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - RGB string like "rgb(255, 255, 255)"
 * @param {string} color2 - RGB string like "rgb(0, 0, 0)"
 * @returns {number} - Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const parseRGB = (color) => {
    // Handle rgb(), rgba(), or hex colors
    if (color.startsWith('rgb')) {
      const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (match) {
        return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
      }
    } else if (color.startsWith('#')) {
      const hex = color.slice(1);
      if (hex.length === 3) {
        return [
          parseInt(hex[0] + hex[0], 16),
          parseInt(hex[1] + hex[1], 16),
          parseInt(hex[2] + hex[2], 16)
        ];
      }
      return [
        parseInt(hex.slice(0, 2), 16),
        parseInt(hex.slice(2, 4), 16),
        parseInt(hex.slice(4, 6), 16)
      ];
    }
    return [0, 0, 0];
  };

  const [r1, g1, b1] = parseRGB(color1);
  const [r2, g2, b2] = parseRGB(color2);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Check if text is "large" according to WCAG (18pt+ regular or 14pt+ bold)
 * @param {number} fontSize - Font size in pixels
 * @param {string|number} fontWeight - Font weight
 * @returns {boolean}
 */
function isLargeText(fontSize, fontWeight) {
  // Convert px to pt (1pt = 1.333px approximately)
  const fontSizePt = fontSize / 1.333;
  const isBold = typeof fontWeight === 'number' ? fontWeight >= 700 :
    fontWeight === 'bold' || fontWeight === '700' || fontWeight === '800' || fontWeight === '900';

  // Large text: 18pt+ regular OR 14pt+ bold
  return fontSizePt >= 18 || (isBold && fontSizePt >= 14);
}

/**
 * Get computed styles for an element
 * @param {import('@playwright/test').Page} page
 * @param {import('@playwright/test').Locator} element
 * @returns {Promise<{color: string, backgroundColor: string, fontSize: number, fontWeight: string}>}
 */
async function getElementStyles(page, element) {
  return await element.evaluate((el) => {
    const style = window.getComputedStyle(el);

    // Get actual background color by traversing up if transparent
    let bgColor = style.backgroundColor;
    let currentEl = el;
    while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
      currentEl = currentEl.parentElement;
      if (!currentEl) {
        bgColor = 'rgb(255, 255, 255)'; // Default to white
        break;
      }
      bgColor = window.getComputedStyle(currentEl).backgroundColor;
    }

    return {
      color: style.color,
      backgroundColor: bgColor,
      fontSize: parseFloat(style.fontSize),
      fontWeight: style.fontWeight
    };
  });
}

// Test Case 1: Body text has at least 4.5:1 contrast ratio
test('TC1: Body text has at least 4.5:1 contrast ratio against background', async ({ page }) => {
  await page.goto('/');

  // Test various body text elements
  const bodyTextSelectors = [
    '.description',           // Hero description
    '.feature-card p',        // Feature card descriptions
    '.step-description',      // Quick start step descriptions
    '.footer-info p'          // Footer text
  ];

  for (const selector of bodyTextSelectors) {
    const elements = page.locator(selector);
    const count = await elements.count();

    for (let i = 0; i < count; i++) {
      const element = elements.nth(i);
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const styles = await getElementStyles(page, element);
        const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
        const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
        const requiredRatio = isLarge ? 3.0 : 4.5;

        expect(
          contrastRatio,
          `Body text "${selector}" (item ${i + 1}) has contrast ratio ${contrastRatio.toFixed(2)}:1, ` +
          `expected at least ${requiredRatio}:1. Color: ${styles.color}, Background: ${styles.backgroundColor}`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    }
  }
});

// Test Case 2: Large headings have at least 3:1 contrast ratio
test('TC2: Large headings (18px+ bold or 24px+) have at least 3:1 contrast ratio', async ({ page }) => {
  await page.goto('/');

  // Test all heading elements
  const headingSelectors = [
    '.hero h1',               // Main hero title (MirDB)
    '.tagline',               // Hero tagline
    '.features h2',           // Features section title
    '.feature-card h3',       // Feature card titles
    '.quick-start h2',        // Quick start section title
    '.step h3'                // Step titles
  ];

  for (const selector of headingSelectors) {
    const elements = page.locator(selector);
    const count = await elements.count();

    for (let i = 0; i < count; i++) {
      const element = elements.nth(i);
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const styles = await getElementStyles(page, element);
        const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
        const isLarge = isLargeText(styles.fontSize, styles.fontWeight);

        // For headings, we verify they are indeed large text and meet at least 3:1
        // If not large text, they need to meet 4.5:1
        const requiredRatio = isLarge ? 3.0 : 4.5;

        expect(
          contrastRatio,
          `Heading "${selector}" (item ${i + 1}) has contrast ratio ${contrastRatio.toFixed(2)}:1, ` +
          `expected at least ${requiredRatio}:1. Font size: ${styles.fontSize}px, Weight: ${styles.fontWeight}, ` +
          `Color: ${styles.color}, Background: ${styles.backgroundColor}`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    }
  }
});

// Test Case 3: Button text meets 4.5:1 contrast requirement
test('TC3: Button text meets 4.5:1 contrast requirement', async ({ page }) => {
  await page.goto('/');

  // Test all button elements
  const buttonSelectors = [
    '.btn-primary',           // Primary CTA buttons
    '.btn-secondary'          // Secondary CTA buttons
  ];

  for (const selector of buttonSelectors) {
    const elements = page.locator(selector);
    const count = await elements.count();

    for (let i = 0; i < count; i++) {
      const element = elements.nth(i);
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const styles = await getElementStyles(page, element);
        const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
        const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
        const requiredRatio = isLarge ? 3.0 : 4.5;

        expect(
          contrastRatio,
          `Button "${selector}" (item ${i + 1}) has contrast ratio ${contrastRatio.toFixed(2)}:1, ` +
          `expected at least ${requiredRatio}:1. Color: ${styles.color}, Background: ${styles.backgroundColor}`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    }
  }
});

// Test Case 4: Code snippet text meets contrast requirements
test('TC4: Code text and syntax highlighting colors meet contrast requirements', async ({ page }) => {
  await page.goto('/');

  // Test code elements in pre blocks
  const codeSelectors = [
    '.step pre',              // Code blocks in steps
    '.step code'              // Inline code elements
  ];

  for (const selector of codeSelectors) {
    const elements = page.locator(selector);
    const count = await elements.count();

    for (let i = 0; i < count; i++) {
      const element = elements.nth(i);
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const styles = await getElementStyles(page, element);
        const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
        const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
        const requiredRatio = isLarge ? 3.0 : 4.5;

        expect(
          contrastRatio,
          `Code element "${selector}" (item ${i + 1}) has contrast ratio ${contrastRatio.toFixed(2)}:1, ` +
          `expected at least ${requiredRatio}:1. Color: ${styles.color}, Background: ${styles.backgroundColor}`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    }
  }
});

// Additional test: Navigation links contrast
test('TC5: Navigation links meet contrast requirements', async ({ page }) => {
  await page.goto('/');

  const navLinks = page.locator('.nav-links a');
  const count = await navLinks.count();

  for (let i = 0; i < count; i++) {
    const element = navLinks.nth(i);
    const isVisible = await element.isVisible().catch(() => false);

    if (isVisible) {
      const styles = await getElementStyles(page, element);
      const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
      const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
      const requiredRatio = isLarge ? 3.0 : 4.5;

      expect(
        contrastRatio,
        `Navigation link (item ${i + 1}) has contrast ratio ${contrastRatio.toFixed(2)}:1, ` +
        `expected at least ${requiredRatio}:1. Color: ${styles.color}, Background: ${styles.backgroundColor}`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  }
});

// Additional test: Footer links contrast
test('TC6: Footer links meet contrast requirements', async ({ page }) => {
  await page.goto('/');

  const footerLinks = page.locator('.footer-nav a');
  const count = await footerLinks.count();

  for (let i = 0; i < count; i++) {
    const element = footerLinks.nth(i);
    const isVisible = await element.isVisible().catch(() => false);

    if (isVisible) {
      const styles = await getElementStyles(page, element);
      const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
      const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
      const requiredRatio = isLarge ? 3.0 : 4.5;

      expect(
        contrastRatio,
        `Footer link (item ${i + 1}) has contrast ratio ${contrastRatio.toFixed(2)}:1, ` +
        `expected at least ${requiredRatio}:1. Color: ${styles.color}, Background: ${styles.backgroundColor}`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  }
});
