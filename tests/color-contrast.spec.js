// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Calculate the relative luminance of an RGB color
 * @param {number} r - Red (0-255)
 * @param {number} g - Green (0-255)
 * @param {number} b - Blue (0-255)
 * @returns {number} Relative luminance (0-1)
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    const sRGB = c / 255;
    return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two luminance values
 * @param {number} l1 - First luminance
 * @param {number} l2 - Second luminance
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(l1, l2) {
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse a CSS color string to RGB values
 * @param {string} color - CSS color string (rgb, rgba, hex)
 * @returns {{r: number, g: number, b: number} | null}
 */
function parseColor(color) {
  if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
    return null;
  }

  // Handle rgb/rgba
  const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10)
    };
  }

  // Handle hex
  const hexMatch = color.match(/^#([0-9a-f]{6}|[0-9a-f]{3})$/i);
  if (hexMatch) {
    let hex = hexMatch[1];
    if (hex.length === 3) {
      hex = hex.split('').map(c => c + c).join('');
    }
    return {
      r: parseInt(hex.slice(0, 2), 16),
      g: parseInt(hex.slice(2, 4), 16),
      b: parseInt(hex.slice(4, 6), 16)
    };
  }

  return null;
}

/**
 * Calculate contrast ratio between foreground and background colors
 * @param {string} fg - Foreground color
 * @param {string} bg - Background color
 * @returns {number} Contrast ratio
 */
function calculateContrast(fg, bg) {
  const fgColor = parseColor(fg);
  const bgColor = parseColor(bg);

  if (!fgColor || !bgColor) {
    return 0;
  }

  const fgLuminance = getLuminance(fgColor.r, fgColor.g, fgColor.b);
  const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

  return getContrastRatio(fgLuminance, bgLuminance);
}

// Minimum contrast ratio for WCAG AA normal text
const MIN_CONTRAST_RATIO = 4.5;

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Body text meets 4.5:1 minimum contrast ratio against background', async ({ page }) => {
    // Get body text elements (paragraphs, descriptions, list items)
    const bodyTextSelectors = [
      '.tagline',
      '.section-description',
      '.feature-card .description',
      '.roadmap-item .description',
      '.ease-message',
      '.footer-tagline',
      '.license'
    ];

    for (const selector of bodyTextSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();

        if (!isVisible) continue;

        // Get the computed color and background color
        const colors = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);

          // Get text color
          const textColor = styles.color;

          // Walk up the DOM tree to find the actual background color
          let bgColor = styles.backgroundColor;
          let currentEl = el;

          while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            currentEl = currentEl.parentElement;
            if (!currentEl) {
              bgColor = 'rgb(13, 17, 23)'; // Default dark background
              break;
            }
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
          }

          return { textColor, bgColor, text: el.textContent?.trim().substring(0, 50) };
        });

        const contrastRatio = calculateContrast(colors.textColor, colors.bgColor);

        expect(
          contrastRatio,
          `Body text "${colors.text}" (selector: ${selector}) should have contrast ratio >= ${MIN_CONTRAST_RATIO}:1. Got ${contrastRatio.toFixed(2)}:1 (fg: ${colors.textColor}, bg: ${colors.bgColor})`
        ).toBeGreaterThanOrEqual(MIN_CONTRAST_RATIO);
      }
    }
  });

  test('TC2: Heading text meets 4.5:1 minimum contrast ratio', async ({ page }) => {
    // Get heading elements
    const headings = page.locator('h1, h2, h3');
    const count = await headings.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const heading = headings.nth(i);
      const isVisible = await heading.isVisible();

      if (!isVisible) continue;

      // Get the computed color and background color
      const colors = await heading.evaluate(el => {
        const styles = window.getComputedStyle(el);

        // Get text color - check for gradient text first
        let textColor = styles.color;
        const webkitTextFillColor = styles.webkitTextFillColor;

        // If using gradient text (transparent fill), get the gradient colors
        if (webkitTextFillColor === 'transparent') {
          // For gradient text, we use the darker color in the gradient for contrast check
          // The gradient is from #3fb950 (green) to #58a6ff (blue)
          // Green has lower luminance, so we use it for the conservative check
          textColor = 'rgb(63, 185, 80)'; // --accent-green
        }

        // Walk up the DOM tree to find the actual background color
        let bgColor = styles.backgroundColor;
        let currentEl = el;

        while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          currentEl = currentEl.parentElement;
          if (!currentEl) {
            bgColor = 'rgb(13, 17, 23)'; // Default dark background
            break;
          }
          bgColor = window.getComputedStyle(currentEl).backgroundColor;
        }

        return {
          textColor,
          bgColor,
          text: el.textContent?.trim().substring(0, 50),
          tagName: el.tagName
        };
      });

      const contrastRatio = calculateContrast(colors.textColor, colors.bgColor);

      expect(
        contrastRatio,
        `Heading <${colors.tagName.toLowerCase()}> "${colors.text}" should have contrast ratio >= ${MIN_CONTRAST_RATIO}:1. Got ${contrastRatio.toFixed(2)}:1 (fg: ${colors.textColor}, bg: ${colors.bgColor})`
      ).toBeGreaterThanOrEqual(MIN_CONTRAST_RATIO);
    }
  });

  test('TC3: Button text meets 4.5:1 contrast ratio against button background', async ({ page }) => {
    // Get all buttons (anchor elements styled as buttons)
    const buttons = page.locator('.btn, [class*="btn-"]');
    const count = await buttons.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible();

      if (!isVisible) continue;

      // Get the computed color and background color
      const colors = await button.evaluate(el => {
        const styles = window.getComputedStyle(el);

        // Get text color
        const textColor = styles.color;

        // Get button background
        let bgColor = styles.backgroundColor;

        // If transparent, walk up to find background
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          let currentEl = el.parentElement;
          while (currentEl && (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent')) {
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
            currentEl = currentEl.parentElement;
          }
        }

        return {
          textColor,
          bgColor,
          text: el.textContent?.trim().substring(0, 50),
          className: el.className
        };
      });

      const contrastRatio = calculateContrast(colors.textColor, colors.bgColor);

      expect(
        contrastRatio,
        `Button "${colors.text}" (class: ${colors.className}) should have contrast ratio >= ${MIN_CONTRAST_RATIO}:1. Got ${contrastRatio.toFixed(2)}:1 (fg: ${colors.textColor}, bg: ${colors.bgColor})`
      ).toBeGreaterThanOrEqual(MIN_CONTRAST_RATIO);
    }
  });

  test('TC4: Run automated accessibility audit - No critical contrast failures in axe', async ({ page }) => {
    // Run axe accessibility audit focused on color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Filter for color contrast violations only
    const contrastViolations = accessibilityScanResults.violations.filter(
      violation => violation.id === 'color-contrast'
    );

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach(violation => {
        console.log(`- ${violation.description}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.target}`);
          console.log(`  Impact: ${node.impact}`);
          console.log(`  Message: ${node.failureSummary}`);
        });
      });
    }

    // Assert no color contrast violations
    expect(
      contrastViolations,
      `Found ${contrastViolations.length} color contrast violation(s). Run with --headed to see details.`
    ).toHaveLength(0);

    // Also check for any serious/critical accessibility issues
    const criticalViolations = accessibilityScanResults.violations.filter(
      violation => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log critical violations
    if (criticalViolations.length > 0) {
      console.log('Critical/serious accessibility violations:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description} (${violation.impact})`);
      });
    }

    // Assert no critical violations
    expect(
      criticalViolations.length,
      `Found ${criticalViolations.length} critical/serious accessibility violation(s)`
    ).toBe(0);
  });
});
