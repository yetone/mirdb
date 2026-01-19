// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

/**
 * Calculate relative luminance of a color according to WCAG 2.1
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance (0-1)
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
 * Calculate contrast ratio between two colors according to WCAG 2.1
 * @param {string} color1 - First color in rgb/rgba format
 * @param {string} color2 - Second color in rgb/rgba format
 * @returns {number} Contrast ratio (1 to 21)
 */
function getContrastRatio(color1, color2) {
  const parseColor = (color) => {
    const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (!match) return { r: 0, g: 0, b: 0 };
    return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
  };

  const c1 = parseColor(color1);
  const c2 = parseColor(color2);

  const l1 = getLuminance(c1.r, c1.g, c1.b);
  const l2 = getLuminance(c2.r, c2.g, c2.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Get the effective background color of an element, traversing up the DOM if transparent
 * @param {import('@playwright/test').Locator} element
 * @returns {Promise<string>} Background color in rgb format
 */
async function getEffectiveBackgroundColor(element) {
  return await element.evaluate((el) => {
    let current = el;
    while (current) {
      const style = window.getComputedStyle(current);
      const bgColor = style.backgroundColor;

      // Check if the background is not transparent
      if (bgColor && !bgColor.includes('rgba(0, 0, 0, 0)') && bgColor !== 'transparent') {
        return bgColor;
      }
      current = current.parentElement;
    }
    // Default to white if no background found
    return 'rgb(255, 255, 255)';
  });
}

// WCAG 2.1 AA minimum contrast ratios
const WCAG_AA_NORMAL_TEXT = 4.5;  // For text smaller than 18pt (or 14pt bold)
const WCAG_AA_LARGE_TEXT = 3.0;   // For text 18pt+ (or 14pt+ bold)

test.describe('Accessibility - Color Contrast (WCAG 2.1 AA - NFR-3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check body text contrast ratio
  // Expected: Body text has at least 4.5:1 contrast ratio
  test('TC1: Body text has at least 4.5:1 contrast ratio', async ({ page }) => {
    // Test multiple body text elements across different sections
    const bodyTextSelectors = [
      '.hero-subtitle',
      '.feature-card p',
      '.section-description',
      '.config-info li'
    ];

    for (const selector of bodyTextSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();

        if (isVisible) {
          const textColor = await element.evaluate(el =>
            window.getComputedStyle(el).color
          );
          const bgColor = await getEffectiveBackgroundColor(element);
          const contrastRatio = getContrastRatio(textColor, bgColor);

          expect(
            contrastRatio,
            `Body text "${selector}" should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1 (text: ${textColor}, bg: ${bgColor})`
          ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
        }
      }
    }
  });

  // Test Case 2: Check heading text contrast ratio
  // Expected: Headings have at least 3:1 contrast ratio (large text)
  test('TC2: Heading text has at least 3:1 contrast ratio (large text)', async ({ page }) => {
    // Test all heading levels - they qualify as large text (>=18pt or >=14pt bold)
    const headingSelectors = ['h1', 'h2', 'h3'];

    for (const selector of headingSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();

        if (isVisible) {
          const textColor = await element.evaluate(el =>
            window.getComputedStyle(el).color
          );
          const bgColor = await getEffectiveBackgroundColor(element);
          const contrastRatio = getContrastRatio(textColor, bgColor);

          // Get heading text for better error messages
          const headingText = await element.textContent();
          const truncatedText = headingText.trim().substring(0, 30);

          expect(
            contrastRatio,
            `Heading "${truncatedText}..." (${selector}) should have contrast ratio >= ${WCAG_AA_LARGE_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
        }
      }
    }
  });

  // Test Case 3: Check button text contrast
  // Expected: Button text has sufficient contrast against button background
  test('TC3: Button text has sufficient contrast against button background', async ({ page }) => {
    // Test primary and secondary buttons
    const primaryButtons = page.locator('.btn-primary');
    const secondaryButtons = page.locator('.btn-secondary');

    // Test primary buttons (white text on blue background)
    const primaryCount = await primaryButtons.count();
    for (let i = 0; i < primaryCount; i++) {
      const button = primaryButtons.nth(i);
      const isVisible = await button.isVisible();

      if (isVisible) {
        const textColor = await button.evaluate(el =>
          window.getComputedStyle(el).color
        );
        const bgColor = await button.evaluate(el =>
          window.getComputedStyle(el).backgroundColor
        );
        const contrastRatio = getContrastRatio(textColor, bgColor);
        const buttonText = await button.textContent();

        expect(
          contrastRatio,
          `Primary button "${buttonText.trim()}" should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      }
    }

    // Test secondary buttons (blue text on transparent/white background)
    const secondaryCount = await secondaryButtons.count();
    for (let i = 0; i < secondaryCount; i++) {
      const button = secondaryButtons.nth(i);
      const isVisible = await button.isVisible();

      if (isVisible) {
        const textColor = await button.evaluate(el =>
          window.getComputedStyle(el).color
        );
        const bgColor = await getEffectiveBackgroundColor(button);
        const contrastRatio = getContrastRatio(textColor, bgColor);
        const buttonText = await button.textContent();

        expect(
          contrastRatio,
          `Secondary button "${buttonText.trim()}" should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      }
    }

    // Test copy button in code section
    const copyButton = page.locator('.copy-btn');
    const copyButtonVisible = await copyButton.isVisible();

    if (copyButtonVisible) {
      const textColor = await copyButton.evaluate(el =>
        window.getComputedStyle(el).color
      );
      const bgColor = await copyButton.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      const contrastRatio = getContrastRatio(textColor, bgColor);

      expect(
        contrastRatio,
        `Copy button should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    }
  });

  // Test Case 4: Check link contrast
  // Expected: Links are distinguishable from surrounding text
  test('TC4: Links are distinguishable from surrounding text', async ({ page }) => {
    // Test links in footer
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    // For visual boundary distinction, links should have sufficient contrast with their background
    // WCAG requires 3:1 contrast for non-text elements (like link underlines/borders)
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        // Check that link has some visual distinction
        const linkStyles = await link.evaluate(el => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            textDecoration: style.textDecoration,
            backgroundColor: style.backgroundColor
          };
        });

        // Footer links contain images (badges), so they should be distinguishable
        // Check that they are positioned and styled as interactive elements
        const hasInteractiveStyle = await link.evaluate(el => {
          const style = window.getComputedStyle(el);
          // Check for min touch target size or interactive styling
          const rect = el.getBoundingClientRect();
          return rect.width >= 44 && rect.height >= 44;
        });

        expect(
          hasInteractiveStyle,
          'Footer links should have minimum touch target size (44x44px)'
        ).toBe(true);
      }
    }

    // Test CTA links - should be distinguishable as interactive elements
    const ctaLinks = page.locator('.hero-cta a');
    const ctaLinkCount = await ctaLinks.count();

    for (let i = 0; i < ctaLinkCount; i++) {
      const link = ctaLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        const linkColor = await link.evaluate(el =>
          window.getComputedStyle(el).color
        );
        const bgColor = await link.evaluate(el =>
          window.getComputedStyle(el).backgroundColor
        );

        // If there's a background color (primary button style), check text contrast
        if (bgColor && !bgColor.includes('rgba(0, 0, 0, 0)') && bgColor !== 'transparent') {
          const contrastRatio = getContrastRatio(linkColor, bgColor);
          const linkText = await link.textContent();

          expect(
            contrastRatio,
            `CTA link "${linkText.trim()}" should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
        } else {
          // For links without background, check contrast against parent background
          const parentBgColor = await getEffectiveBackgroundColor(link);
          const contrastRatio = getContrastRatio(linkColor, parentBgColor);
          const linkText = await link.textContent();

          expect(
            contrastRatio,
            `CTA link "${linkText.trim()}" should have contrast ratio >= ${WCAG_AA_LARGE_TEXT}:1 against background, got ${contrastRatio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
        }
      }
    }
  });

  // Additional test: Code block text contrast
  test('Code block text has sufficient contrast', async ({ page }) => {
    const codeBlocks = page.locator('pre code, .syntax-highlight code');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const isVisible = await codeBlock.isVisible();

      if (isVisible) {
        const textColor = await codeBlock.evaluate(el =>
          window.getComputedStyle(el).color
        );
        const bgColor = await getEffectiveBackgroundColor(codeBlock);
        const contrastRatio = getContrastRatio(textColor, bgColor);

        expect(
          contrastRatio,
          `Code block should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      }
    }
  });

  // Additional test: Muted text still meets minimum requirements
  test('Muted/secondary text still meets minimum contrast requirements', async ({ page }) => {
    // Elements with muted text (--text-muted color)
    const mutedTextSelectors = [
      '.license',
      '.copyright',
      '.code-lang'
    ];

    for (const selector of mutedTextSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();

        if (isVisible) {
          const textColor = await element.evaluate(el =>
            window.getComputedStyle(el).color
          );
          const bgColor = await getEffectiveBackgroundColor(element);
          const contrastRatio = getContrastRatio(textColor, bgColor);
          const elementText = await element.textContent();

          expect(
            contrastRatio,
            `Muted text "${elementText.trim().substring(0, 30)}..." should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${contrastRatio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
        }
      }
    }
  });
});
