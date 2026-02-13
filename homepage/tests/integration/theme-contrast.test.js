/**
 * Integration Tests for Theme Contrast
 * Owner: Scenario 6 - Theme Support
 *
 * Tests:
 * - WCAG 2.1 AA contrast requirements (4.5:1 minimum for normal text)
 * - Both light and dark themes meet accessibility standards
 */

const { test, expect } = require('@playwright/test');

/**
 * Calculate relative luminance of an RGB color
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - RGB color string "rgb(r, g, b)"
 * @param {string} color2 - RGB color string "rgb(r, g, b)"
 * @returns {number} Contrast ratio
 */
function getContrastRatio(color1, color2) {
  const parseRgb = (color) => {
    const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (match) {
      return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
    }
    // Handle hex colors
    const hexMatch = color.match(/#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})/i);
    if (hexMatch) {
      return [parseInt(hexMatch[1], 16), parseInt(hexMatch[2], 16), parseInt(hexMatch[3], 16)];
    }
    return [0, 0, 0];
  };

  const [r1, g1, b1] = parseRgb(color1);
  const [r2, g2, b2] = parseRgb(color2);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Theme Contrast Tests', () => {
  test('Test Case 10: Dark theme text contrast meets WCAG 2.1 AA (4.5:1 minimum)', async ({ page }) => {
    await page.goto('/');

    // Set dark theme
    await page.evaluate(() => {
      window.setTheme('dark');
    });

    // Wait for theme attribute to be set
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'dark');
    // Wait additional time for CSS to apply
    await page.waitForTimeout(300);

    // Get text color and background color
    const colors = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = getComputedStyle(body);
      return {
        textColor: computedStyle.color,
        bgColor: computedStyle.backgroundColor
      };
    });

    // Calculate contrast ratio
    const contrastRatio = getContrastRatio(colors.textColor, colors.bgColor);

    // WCAG 2.1 AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('Light theme text contrast meets WCAG 2.1 AA (4.5:1 minimum)', async ({ page }) => {
    await page.goto('/');

    // Set light theme
    await page.evaluate(() => {
      window.setTheme('light');
    });

    // Wait for theme attribute to be set
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'light');
    // Wait additional time for CSS to apply
    await page.waitForTimeout(300);

    // Get text color and background color
    const colors = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = getComputedStyle(body);
      return {
        textColor: computedStyle.color,
        bgColor: computedStyle.backgroundColor
      };
    });

    // Calculate contrast ratio
    const contrastRatio = getContrastRatio(colors.textColor, colors.bgColor);

    // WCAG 2.1 AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('Dark theme hero title has sufficient contrast', async ({ page }) => {
    await page.goto('/');

    // Set dark theme
    await page.evaluate(() => {
      window.setTheme('dark');
    });

    // Wait for theme attribute to be set
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'dark');
    // Wait additional time for CSS to apply
    await page.waitForTimeout(300);

    // Get hero title color and its background
    const colors = await page.evaluate(() => {
      const heroTitle = document.querySelector('.hero__title');
      if (!heroTitle) return null;

      const titleStyle = getComputedStyle(heroTitle);

      // Walk up to find actual background color
      let bgColor = 'rgb(255, 255, 255)';
      let element = heroTitle;
      while (element && element !== document.documentElement) {
        const style = getComputedStyle(element);
        const bg = style.backgroundColor;
        if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
          bgColor = bg;
          break;
        }
        element = element.parentElement;
      }

      // Also check document root
      if (bgColor === 'rgb(255, 255, 255)') {
        const bodyBg = getComputedStyle(document.body).backgroundColor;
        if (bodyBg && bodyBg !== 'rgba(0, 0, 0, 0)') {
          bgColor = bodyBg;
        }
      }

      return {
        textColor: titleStyle.color,
        bgColor: bgColor
      };
    });

    if (colors) {
      const contrastRatio = getContrastRatio(colors.textColor, colors.bgColor);
      // Large text (>= 18pt or 14pt bold) needs 3:1 minimum
      expect(contrastRatio).toBeGreaterThanOrEqual(3);
    }
  });

  test('Dark theme applies correct CSS custom properties', async ({ page }) => {
    await page.goto('/');

    // Set dark theme
    await page.evaluate(() => {
      window.setTheme('dark');
    });

    // Wait for theme attribute to be set
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'dark');
    // Wait additional time for CSS to apply
    await page.waitForTimeout(300);

    // Check CSS custom properties
    const cssVars = await page.evaluate(() => {
      const root = document.documentElement;
      const style = getComputedStyle(root);
      return {
        colorText: style.getPropertyValue('--color-text').trim(),
        colorBackground: style.getPropertyValue('--color-background').trim(),
        colorSurface: style.getPropertyValue('--color-surface').trim()
      };
    });

    // Dark theme should have light text
    expect(cssVars.colorText).toBe('#e8e8e8');
    // Dark theme should have dark background
    expect(cssVars.colorBackground).toBe('#121212');
    // Dark theme should have dark surface
    expect(cssVars.colorSurface).toBe('#1e1e1e');
  });

  test('Light theme applies correct CSS custom properties', async ({ page }) => {
    await page.goto('/');

    // Set light theme
    await page.evaluate(() => {
      window.setTheme('light');
    });

    // Wait for theme attribute to be set
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'light');
    // Wait additional time for CSS to apply
    await page.waitForTimeout(300);

    // Check CSS custom properties
    const cssVars = await page.evaluate(() => {
      const root = document.documentElement;
      const style = getComputedStyle(root);
      return {
        colorText: style.getPropertyValue('--color-text').trim(),
        colorBackground: style.getPropertyValue('--color-background').trim(),
        colorSurface: style.getPropertyValue('--color-surface').trim()
      };
    });

    // Light theme should have dark text
    expect(cssVars.colorText).toBe('#1a1a1a');
    // Light theme should have light background
    expect(cssVars.colorBackground).toBe('#ffffff');
    // Light theme should have light surface
    expect(cssVars.colorSurface).toBe('#f8f9fa');
  });
});
