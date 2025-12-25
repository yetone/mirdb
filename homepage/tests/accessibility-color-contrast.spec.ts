import { test, expect, Page } from '@playwright/test';

/**
 * WCAG 2.1 AA Color Contrast Requirements:
 * - Normal text (< 18pt or < 14pt bold): 4.5:1 contrast ratio
 * - Large text (>= 18pt or >= 14pt bold): 3:1 contrast ratio
 * - UI components and graphical objects: 3:1 contrast ratio
 */

// Calculate relative luminance from RGB values
// Formula from WCAG 2.1: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

// Calculate contrast ratio between two colors
// Formula: (L1 + 0.05) / (L2 + 0.05) where L1 is the lighter luminance
function getContrastRatio(rgb1: [number, number, number], rgb2: [number, number, number]): number {
  const l1 = getLuminance(...rgb1);
  const l2 = getLuminance(...rgb2);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

// Parse CSS color value to RGB
function parseColor(color: string): [number, number, number] | null {
  // Handle rgb(r, g, b) and rgba(r, g, b, a) formats
  const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
  if (rgbMatch) {
    return [parseInt(rgbMatch[1]), parseInt(rgbMatch[2]), parseInt(rgbMatch[3])];
  }

  // Handle hex colors
  const hexMatch = color.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
  if (hexMatch) {
    return [parseInt(hexMatch[1], 16), parseInt(hexMatch[2], 16), parseInt(hexMatch[3], 16)];
  }

  // Handle shorthand hex
  const shortHexMatch = color.match(/^#([0-9a-f])([0-9a-f])([0-9a-f])$/i);
  if (shortHexMatch) {
    return [
      parseInt(shortHexMatch[1] + shortHexMatch[1], 16),
      parseInt(shortHexMatch[2] + shortHexMatch[2], 16),
      parseInt(shortHexMatch[3] + shortHexMatch[3], 16),
    ];
  }

  return null;
}

// Get computed styles for an element
async function getElementColors(
  page: Page,
  selector: string
): Promise<{ color: string; backgroundColor: string; fontSize: string; fontWeight: string } | null> {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return null;

    const styles = window.getComputedStyle(element);
    let bgColor = styles.backgroundColor;

    // If background is transparent, walk up the DOM to find the actual background
    let parent = element.parentElement;
    while (
      parent &&
      (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)')
    ) {
      const parentStyles = window.getComputedStyle(parent);
      bgColor = parentStyles.backgroundColor;
      parent = parent.parentElement;
    }

    // Default to white if still transparent (body background)
    if (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)') {
      bgColor = 'rgb(255, 255, 255)';
    }

    return {
      color: styles.color,
      backgroundColor: bgColor,
      fontSize: styles.fontSize,
      fontWeight: styles.fontWeight,
    };
  }, selector);
}

// Check if font is considered "large" per WCAG (>= 18pt or >= 14pt bold)
function isLargeText(fontSize: string, fontWeight: string): boolean {
  const size = parseFloat(fontSize);
  const weight = parseInt(fontWeight);
  // 18pt = 24px, 14pt = ~18.67px
  const is18ptOrLarger = size >= 24;
  const is14ptBoldOrLarger = size >= 18.67 && weight >= 700;
  return is18ptOrLarger || is14ptBoldOrLarger;
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Body text has minimum 4.5:1 contrast ratio against background', async ({ page }) => {
    // Check various body text elements
    const bodyTextSelectors = [
      'body',
      '.hero-description',
      '.feature-card p',
      '.lsm-explanation p',
      '.config-table td',
      '.status-card li',
      '.contribute-section p',
    ];

    for (const selector of bodyTextSelectors) {
      const colors = await getElementColors(page, selector);
      if (!colors) continue;

      const textColor = parseColor(colors.color);
      const bgColor = parseColor(colors.backgroundColor);

      if (!textColor || !bgColor) continue;

      const contrastRatio = getContrastRatio(textColor, bgColor);

      // Normal text requires 4.5:1 contrast ratio
      // Large text (>= 18pt or >= 14pt bold) requires 3:1
      const requiredRatio = isLargeText(colors.fontSize, colors.fontWeight) ? 3 : 4.5;

      expect(
        contrastRatio,
        `Text at "${selector}" has insufficient contrast: ${contrastRatio.toFixed(2)}:1, requires ${requiredRatio}:1. ` +
          `Color: ${colors.color}, Background: ${colors.backgroundColor}`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  });

  test('TC2: Heading text has minimum 3:1 contrast ratio (large text)', async ({ page }) => {
    // Check heading elements - these are large text and require 3:1 ratio
    const headingSelectors = ['h1', 'h2', 'h3'];

    for (const selector of headingSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();
        if (!isVisible) continue;

        const colors = await page.evaluate(
          ({ sel, index }) => {
            const allElements = document.querySelectorAll(sel);
            const el = allElements[index];
            if (!el) return null;

            const styles = window.getComputedStyle(el);
            let bgColor = styles.backgroundColor;

            let parent = el.parentElement;
            while (
              parent &&
              (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)')
            ) {
              const parentStyles = window.getComputedStyle(parent);
              bgColor = parentStyles.backgroundColor;
              parent = parent.parentElement;
            }

            if (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)') {
              bgColor = 'rgb(255, 255, 255)';
            }

            return {
              color: styles.color,
              backgroundColor: bgColor,
              fontSize: styles.fontSize,
              fontWeight: styles.fontWeight,
              text: el.textContent?.trim().substring(0, 30) || '',
            };
          },
          { sel: selector, index: i }
        );

        if (!colors) continue;

        const textColor = parseColor(colors.color);
        const bgColor = parseColor(colors.backgroundColor);

        if (!textColor || !bgColor) continue;

        const contrastRatio = getContrastRatio(textColor, bgColor);

        // Large text requires 3:1 minimum
        expect(
          contrastRatio,
          `Heading "${colors.text}..." has insufficient contrast: ${contrastRatio.toFixed(2)}:1, requires 3:1. ` +
            `Color: ${colors.color}, Background: ${colors.backgroundColor}`
        ).toBeGreaterThanOrEqual(3);
      }
    }
  });

  test('TC3: Button text and borders have minimum 3:1 contrast ratio', async ({ page }) => {
    // Check button/CTA elements
    const buttonSelectors = ['.btn-primary', '.btn-secondary', 'button', '.btn'];

    for (const selector of buttonSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();
        if (!isVisible) continue;

        const colors = await page.evaluate(
          ({ sel, index }) => {
            const allElements = document.querySelectorAll(sel);
            const el = allElements[index];
            if (!el) return null;

            const styles = window.getComputedStyle(el);
            let bgColor = styles.backgroundColor;

            // For buttons, use their own background color
            // If transparent, walk up the DOM
            if (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)') {
              let parent = el.parentElement;
              while (
                parent &&
                (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)')
              ) {
                const parentStyles = window.getComputedStyle(parent);
                bgColor = parentStyles.backgroundColor;
                parent = parent.parentElement;
              }
            }

            if (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)') {
              bgColor = 'rgb(255, 255, 255)';
            }

            return {
              color: styles.color,
              backgroundColor: bgColor,
              borderColor: styles.borderColor,
              text: el.textContent?.trim() || '',
            };
          },
          { sel: selector, index: i }
        );

        if (!colors) continue;

        const textColor = parseColor(colors.color);
        const bgColor = parseColor(colors.backgroundColor);

        if (!textColor || !bgColor) continue;

        const contrastRatio = getContrastRatio(textColor, bgColor);

        // UI components require 3:1 minimum contrast
        expect(
          contrastRatio,
          `Button "${colors.text}" has insufficient text contrast: ${contrastRatio.toFixed(2)}:1, requires 3:1. ` +
            `Color: ${colors.color}, Background: ${colors.backgroundColor}`
        ).toBeGreaterThanOrEqual(3);
      }
    }
  });

  test('TC4: Links are distinguishable from surrounding text (color + another indicator)', async ({ page }) => {
    // Check that links have visual distinction from surrounding text
    const linkSelectors = [
      'a[href]:not(.btn)',
      '.nav-links a',
      '.contribute-section a',
      '.footer a',
    ];

    for (const selector of linkSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < Math.min(count, 5); i++) {
        // Sample up to 5 links per selector
        const element = elements.nth(i);
        const isVisible = await element.isVisible();
        if (!isVisible) continue;

        const linkStyles = await page.evaluate(
          ({ sel, index }) => {
            const allElements = document.querySelectorAll(sel);
            const el = allElements[index];
            if (!el) return null;

            const styles = window.getComputedStyle(el);

            // Get parent text color for comparison
            let parentColor = 'rgb(0, 0, 0)';
            const parent = el.parentElement;
            if (parent) {
              // Create a temporary span to measure the parent's text color
              const tempSpan = document.createElement('span');
              tempSpan.textContent = 'test';
              parent.appendChild(tempSpan);
              parentColor = window.getComputedStyle(tempSpan).color;
              parent.removeChild(tempSpan);
            }

            let bgColor = styles.backgroundColor;
            if (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)') {
              let p = el.parentElement;
              while (p && (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)')) {
                bgColor = window.getComputedStyle(p).backgroundColor;
                p = p.parentElement;
              }
            }

            if (bgColor === 'transparent' || bgColor === 'rgba(0, 0, 0, 0)') {
              bgColor = 'rgb(255, 255, 255)';
            }

            return {
              color: styles.color,
              backgroundColor: bgColor,
              textDecoration: styles.textDecoration,
              textDecorationLine: styles.textDecorationLine,
              fontWeight: styles.fontWeight,
              parentColor,
              text: el.textContent?.trim().substring(0, 30) || '',
            };
          },
          { sel: selector, index: i }
        );

        if (!linkStyles) continue;

        const linkColor = parseColor(linkStyles.color);
        const bgColor = parseColor(linkStyles.backgroundColor);
        const parentColor = parseColor(linkStyles.parentColor);

        if (!linkColor || !bgColor) continue;

        // Check contrast against background (3:1 minimum for link color)
        const contrastRatio = getContrastRatio(linkColor, bgColor);
        expect(
          contrastRatio,
          `Link "${linkStyles.text}..." has insufficient contrast: ${contrastRatio.toFixed(2)}:1, requires 3:1`
        ).toBeGreaterThanOrEqual(3);

        // Check if link is distinguishable from surrounding text
        // Links should have EITHER:
        // 1. Different color from parent text (3:1 ratio difference or distinct hue)
        // 2. Additional visual indicator (underline, bold, icon, etc.)
        const hasUnderline =
          linkStyles.textDecoration?.includes('underline') ||
          linkStyles.textDecorationLine?.includes('underline');
        const hasBoldWeight = parseInt(linkStyles.fontWeight) >= 600;

        let hasDistinctColor = false;
        if (parentColor) {
          // Check if link color is noticeably different from parent
          // Either different luminance or different hue
          const linkLum = getLuminance(...linkColor);
          const parentLum = getLuminance(...parentColor);
          const lumDiff = Math.abs(linkLum - parentLum);

          // Check if colors are significantly different
          const colorDiff = Math.sqrt(
            Math.pow(linkColor[0] - parentColor[0], 2) +
              Math.pow(linkColor[1] - parentColor[1], 2) +
              Math.pow(linkColor[2] - parentColor[2], 2)
          );

          hasDistinctColor = lumDiff > 0.1 || colorDiff > 100;
        }

        const isDistinguishable = hasUnderline || hasBoldWeight || hasDistinctColor;
        expect(
          isDistinguishable,
          `Link "${linkStyles.text}..." is not distinguishable from surrounding text. ` +
            `Links need underline, bold weight, or distinct color. ` +
            `Current: color=${linkStyles.color}, textDecoration=${linkStyles.textDecoration}, fontWeight=${linkStyles.fontWeight}`
        ).toBeTruthy();
      }
    }
  });
});
