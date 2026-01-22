// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * WCAG 2.1 AA Color Contrast Requirements:
 * - Normal text (< 18pt or < 14pt bold): 4.5:1 minimum contrast ratio
 * - Large text (>= 18pt or >= 14pt bold): 3:1 minimum contrast ratio
 *
 * Large text is defined as:
 * - 18pt (24px) regular weight or larger
 * - 14pt (18.5px) bold (700+) or larger
 */

/**
 * Calculate relative luminance of an RGB color
 * Formula from WCAG 2.1 specification
 * @param {number} r - Red (0-255)
 * @param {number} g - Green (0-255)
 * @param {number} b - Blue (0-255)
 * @returns {number} Relative luminance (0-1)
 */
function getRelativeLuminance(r, g, b) {
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
 * @param {string} color1 - RGB color string e.g., "rgb(30, 41, 59)"
 * @param {string} color2 - RGB color string e.g., "rgb(255, 255, 255)"
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const parseRgb = (rgb) => {
    const match = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (!match) return [0, 0, 0];
    return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
  };

  const [r1, g1, b1] = parseRgb(color1);
  const [r2, g2, b2] = parseRgb(color2);

  const l1 = getRelativeLuminance(r1, g1, b1);
  const l2 = getRelativeLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Determine if text is "large" per WCAG 2.1 definition
 * @param {number} fontSize - Font size in pixels
 * @param {number} fontWeight - Font weight (100-900)
 * @returns {boolean} True if text qualifies as "large"
 */
function isLargeText(fontSize, fontWeight) {
  // Large text: >= 18pt (24px) OR >= 14pt (18.67px) if bold (>= 700)
  if (fontSize >= 24) return true;
  if (fontSize >= 18.67 && fontWeight >= 700) return true;
  return false;
}

/**
 * Get the minimum required contrast ratio for given text properties
 * @param {number} fontSize - Font size in pixels
 * @param {number} fontWeight - Font weight (100-900)
 * @returns {number} Minimum contrast ratio (3.0 or 4.5)
 */
function getMinContrastRatio(fontSize, fontWeight) {
  return isLargeText(fontSize, fontWeight) ? 3.0 : 4.5;
}

test.describe('Accessibility - Color Contrast (WCAG 2.1 AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Body text has minimum 4.5:1 contrast ratio against background', async ({ page }) => {
    // Test multiple body text elements across the page
    const bodyTextSelectors = [
      '.hero-subtitle',
      '.features-intro',
      '.demo-intro',
      '.quickstart-intro',
      '.feature-description',
      '.roadmap-list li'
    ];

    for (const selector of bodyTextSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      if (count === 0) continue;

      // Test first element of each type
      const element = elements.first();
      const isVisible = await element.isVisible().catch(() => false);
      if (!isVisible) continue;

      // Get computed styles
      const styles = await element.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor,
          fontSize: parseFloat(computed.fontSize),
          fontWeight: parseInt(computed.fontWeight)
        };
      });

      // Get effective background color (traverse up if transparent)
      const bgColor = await element.evaluate((el) => {
        let current = el;
        let bg = 'rgba(0, 0, 0, 0)';
        while (current && current !== document.body) {
          const computed = window.getComputedStyle(current);
          if (computed.backgroundColor !== 'rgba(0, 0, 0, 0)') {
            bg = computed.backgroundColor;
            break;
          }
          current = current.parentElement;
        }
        // Default to body background if still transparent
        if (bg === 'rgba(0, 0, 0, 0)') {
          bg = window.getComputedStyle(document.body).backgroundColor;
          if (bg === 'rgba(0, 0, 0, 0)') bg = 'rgb(255, 255, 255)';
        }
        return bg;
      });

      const contrastRatio = getContrastRatio(styles.color, bgColor);
      const minRequired = getMinContrastRatio(styles.fontSize, styles.fontWeight);

      // Body text should meet at least 4.5:1 for normal text
      expect(
        contrastRatio,
        `${selector} contrast ratio ${contrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
      ).toBeGreaterThanOrEqual(minRequired);
    }
  });

  test('TC2: Large heading text has minimum 3:1 contrast ratio', async ({ page }) => {
    // Test heading elements which should be considered "large text"
    const headingSelectors = [
      '.hero h1',
      '.features h2',
      '.demo h2',
      '.quick-start h2',
      '.roadmap h2',
      '.feature-title'
    ];

    for (const selector of headingSelectors) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible().catch(() => false);
      if (!isVisible) continue;

      const styles = await element.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor,
          fontSize: parseFloat(computed.fontSize),
          fontWeight: parseInt(computed.fontWeight)
        };
      });

      // Get effective background color
      const bgColor = await element.evaluate((el) => {
        let current = el;
        let bg = 'rgba(0, 0, 0, 0)';
        while (current && current !== document.body) {
          const computed = window.getComputedStyle(current);
          if (computed.backgroundColor !== 'rgba(0, 0, 0, 0)') {
            bg = computed.backgroundColor;
            break;
          }
          current = current.parentElement;
        }
        if (bg === 'rgba(0, 0, 0, 0)') {
          bg = window.getComputedStyle(document.body).backgroundColor;
          if (bg === 'rgba(0, 0, 0, 0)') bg = 'rgb(255, 255, 255)';
        }
        return bg;
      });

      const contrastRatio = getContrastRatio(styles.color, bgColor);

      // Verify this is large text (>= 24px or >= 18.67px bold)
      const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
      const minRequired = isLarge ? 3.0 : 4.5;

      expect(
        contrastRatio,
        `${selector} (${styles.fontSize}px, weight ${styles.fontWeight}) contrast ratio ${contrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
      ).toBeGreaterThanOrEqual(minRequired);

      // Additionally verify headings are actually large text
      expect(
        isLarge || contrastRatio >= 4.5,
        `${selector} should be large text (>= 24px or >= 18.67px bold) or meet 4.5:1 contrast`
      ).toBe(true);
    }
  });

  test('TC3: Links are distinguishable from surrounding text', async ({ page }) => {
    // Test link visibility and distinguishability
    const linkContainers = [
      { containerSelector: '.nav-links', linkSelector: '.nav-links a:not(.btn)' },
      { containerSelector: '.footer-author', linkSelector: '.footer-author a' },
      { containerSelector: '.footer-links', linkSelector: '.footer-links a' }
    ];

    for (const { containerSelector, linkSelector } of linkContainers) {
      const link = page.locator(linkSelector).first();
      const isVisible = await link.isVisible().catch(() => false);
      if (!isVisible) continue;

      const linkStyles = await link.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          textDecoration: computed.textDecorationLine,
          textDecorationStyle: computed.textDecorationStyle,
          borderBottom: computed.borderBottom
        };
      });

      // Links should have either:
      // 1. Text decoration (underline) OR
      // 2. Sufficient color contrast from surrounding text (3:1) plus another visual indicator
      const hasUnderline = linkStyles.textDecoration.includes('underline');
      const hasBorderBottom = linkStyles.borderBottom && linkStyles.borderBottom !== 'none';

      // Get parent/surrounding text color for comparison
      const container = page.locator(containerSelector).first();
      const containerStyles = await container.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color
        };
      });

      // Calculate contrast between link color and surrounding text
      const linkToTextContrast = getContrastRatio(linkStyles.color, containerStyles.color);

      // WCAG 2.1 requires links to be distinguishable through:
      // - Color contrast of at least 3:1 between link and surrounding text PLUS
      // - Another visual cue (underline, bold, etc.) when not hovered
      // OR simply have an underline
      const isDistinguishable =
        hasUnderline ||
        hasBorderBottom ||
        linkToTextContrast >= 3.0;

      expect(
        isDistinguishable,
        `Link "${await link.textContent()}" should be distinguishable (underline or 3:1 contrast with surrounding text)`
      ).toBe(true);

      // Additionally check hover state for visual feedback
      await link.hover();
      const hoverStyles = await link.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          textDecoration: computed.textDecorationLine
        };
      });

      // Hover should provide additional visual feedback
      const hasHoverChange =
        hoverStyles.color !== linkStyles.color ||
        hoverStyles.textDecoration !== linkStyles.textDecoration;

      expect(
        hasHoverChange || hasUnderline,
        `Link should have visual hover state or permanent underline`
      ).toBe(true);
    }
  });

  test('TC4: Button text has sufficient contrast against button background', async ({ page }) => {
    // Test primary and secondary buttons
    const buttonSelectors = [
      '.btn-primary',
      '.btn-secondary',
      '.copy-btn'
    ];

    for (const selector of buttonSelectors) {
      const buttons = page.locator(selector);
      const count = await buttons.count();

      if (count === 0) continue;

      const button = buttons.first();
      const isVisible = await button.isVisible().catch(() => false);
      if (!isVisible) continue;

      const styles = await button.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor,
          fontSize: parseFloat(computed.fontSize),
          fontWeight: parseInt(computed.fontWeight)
        };
      });

      const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
      const minRequired = getMinContrastRatio(styles.fontSize, styles.fontWeight);

      expect(
        contrastRatio,
        `${selector} button text contrast ${contrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
      ).toBeGreaterThanOrEqual(minRequired);

      // Test hover state as well
      await button.hover();

      const hoverStyles = await button.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor,
          fontSize: parseFloat(computed.fontSize),
          fontWeight: parseInt(computed.fontWeight)
        };
      });

      const hoverContrastRatio = getContrastRatio(hoverStyles.color, hoverStyles.backgroundColor);

      expect(
        hoverContrastRatio,
        `${selector} button hover state contrast ${hoverContrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
      ).toBeGreaterThanOrEqual(minRequired);
    }
  });

  test('Code block text has sufficient contrast', async ({ page }) => {
    // Navigate to the quick-start section to ensure code blocks are visible
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    const codeBlock = page.locator('.code-block code').first();
    const isVisible = await codeBlock.isVisible().catch(() => false);

    if (isVisible) {
      const styles = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        // Get the code block container's background
        const container = el.closest('.code-block');
        const containerBg = container
          ? window.getComputedStyle(container).backgroundColor
          : computed.backgroundColor;
        return {
          color: computed.color,
          backgroundColor: containerBg,
          fontSize: parseFloat(computed.fontSize),
          fontWeight: parseInt(computed.fontWeight)
        };
      });

      const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
      const minRequired = getMinContrastRatio(styles.fontSize, styles.fontWeight);

      expect(
        contrastRatio,
        `Code block text contrast ${contrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
      ).toBeGreaterThanOrEqual(minRequired);
    }
  });

  test('Footer text has sufficient contrast', async ({ page }) => {
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();

    // Test footer author text
    const footerAuthor = page.locator('.footer-author');
    const isVisible = await footerAuthor.isVisible().catch(() => false);

    if (isVisible) {
      const styles = await footerAuthor.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        // Get footer background
        const footer = el.closest('.footer');
        const footerBg = footer
          ? window.getComputedStyle(footer).backgroundColor
          : computed.backgroundColor;
        return {
          color: computed.color,
          backgroundColor: footerBg,
          fontSize: parseFloat(computed.fontSize),
          fontWeight: parseInt(computed.fontWeight)
        };
      });

      const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
      const minRequired = getMinContrastRatio(styles.fontSize, styles.fontWeight);

      expect(
        contrastRatio,
        `Footer text contrast ${contrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
      ).toBeGreaterThanOrEqual(minRequired);
    }
  });

  test('Navigation text has sufficient contrast', async ({ page }) => {
    const navTitle = page.locator('.nav-title');

    const styles = await navTitle.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      // Get header/nav background
      const header = el.closest('.header');
      const headerBg = header
        ? window.getComputedStyle(header).backgroundColor
        : 'rgb(255, 255, 255)';
      return {
        color: computed.color,
        backgroundColor: headerBg,
        fontSize: parseFloat(computed.fontSize),
        fontWeight: parseInt(computed.fontWeight)
      };
    });

    const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
    const minRequired = getMinContrastRatio(styles.fontSize, styles.fontWeight);

    expect(
      contrastRatio,
      `Navigation title contrast ${contrastRatio.toFixed(2)}:1 should meet ${minRequired}:1 minimum`
    ).toBeGreaterThanOrEqual(minRequired);
  });
});
