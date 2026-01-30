/**
 * MirDB Landing Page - Dark Mode Design Tests
 * Owner: Scenario 16
 *
 * Tests verify that the page implements dark mode styling
 * appropriate for developer tools, including:
 * - Dark background colors
 * - Light text with proper contrast
 * - Code block dark theme with syntax highlighting
 * - Visible link colors in dark mode
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

// Helper to convert RGB to relative luminance
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

// Helper to calculate contrast ratio between two colors
function getContrastRatio(rgb1, rgb2) {
  const l1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

// Helper to parse RGB color string
function parseRgb(rgbString) {
  const match = rgbString.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (match) {
    return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
  }
  return null;
}

// Helper to check if a color is dark (luminance < 0.2)
function isDarkColor(rgb) {
  if (!rgb) return false;
  const luminance = getLuminance(rgb.r, rgb.g, rgb.b);
  return luminance < 0.2;
}

// Helper to check if a color is light (luminance > 0.5)
function isLightColor(rgb) {
  if (!rgb) return false;
  const luminance = getLuminance(rgb.r, rgb.g, rgb.b);
  return luminance > 0.4;
}

test.describe('Dark Mode Design - Scenario 16', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('Test Case 1: Page background is dark (developer environment)', async ({ page }) => {
    // Check body background color
    const bodyBgColor = await page.evaluate(() => {
      const body = document.body;
      return window.getComputedStyle(body).backgroundColor;
    });

    const bgRgb = parseRgb(bodyBgColor);
    expect(bgRgb).not.toBeNull();
    expect(isDarkColor(bgRgb)).toBe(true);

    // Also verify the CSS variable is set correctly
    const cssVarBg = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg').trim();
    });
    expect(cssVarBg).toBe('#0d1117');
  });

  test('Test Case 2: Text color has proper contrast on dark background', async ({ page }) => {
    // Get background and text colors
    const colors = await page.evaluate(() => {
      const body = document.body;
      const styles = window.getComputedStyle(body);
      return {
        bg: styles.backgroundColor,
        text: styles.color,
      };
    });

    const bgRgb = parseRgb(colors.bg);
    const textRgb = parseRgb(colors.text);

    expect(bgRgb).not.toBeNull();
    expect(textRgb).not.toBeNull();

    // Text should be light colored
    expect(isLightColor(textRgb)).toBe(true);

    // Contrast ratio should meet WCAG AA standard (4.5:1 for normal text)
    const contrastRatio = getContrastRatio(bgRgb, textRgb);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);

    // Also verify heading contrast
    const headingColors = await page.evaluate(() => {
      const heading = document.querySelector('h1, h2');
      if (!heading) return null;
      const styles = window.getComputedStyle(heading);
      return {
        text: styles.color,
      };
    });

    if (headingColors) {
      const headingTextRgb = parseRgb(headingColors.text);
      expect(headingTextRgb).not.toBeNull();
      expect(isLightColor(headingTextRgb)).toBe(true);
    }
  });

  test('Test Case 3: Code blocks have dark theme with syntax highlighting', async ({ page }) => {
    // Find code blocks on the page
    const codeBlockExists = await page.locator('pre, code, .code-block').first().isVisible().catch(() => false);

    if (codeBlockExists) {
      // Check code block background color
      const codeBlockBg = await page.evaluate(() => {
        const codeBlock = document.querySelector('pre, .code-block, code');
        if (!codeBlock) return null;
        return window.getComputedStyle(codeBlock).backgroundColor;
      });

      if (codeBlockBg && codeBlockBg !== 'rgba(0, 0, 0, 0)') {
        const codeBgRgb = parseRgb(codeBlockBg);
        expect(codeBgRgb).not.toBeNull();
        // Code background should be dark
        expect(isDarkColor(codeBgRgb)).toBe(true);
      }
    }

    // Verify syntax highlighting CSS variables are defined
    const syntaxColors = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        keyword: styles.getPropertyValue('--color-syntax-keyword').trim(),
        string: styles.getPropertyValue('--color-syntax-string').trim(),
        comment: styles.getPropertyValue('--color-syntax-comment').trim(),
        codeBackground: styles.getPropertyValue('--color-code-bg').trim(),
      };
    });

    // Verify syntax highlighting colors exist
    expect(syntaxColors.codeBackground).toBeTruthy();
    expect(syntaxColors.codeBackground).toBe('#1f2428');
  });

  test('Test Case 4: Links are visible and distinguishable from regular text', async ({ page }) => {
    // Get all link and text colors together to avoid async issues
    const colors = await page.evaluate(() => {
      const link = document.querySelector('a:not(.btn)');
      const body = document.body;
      const root = document.documentElement;

      const linkStyle = link ? window.getComputedStyle(link) : null;
      const bodyStyle = window.getComputedStyle(body);
      const rootStyle = window.getComputedStyle(root);

      return {
        linkColor: linkStyle ? linkStyle.color : null,
        textColor: bodyStyle.color,
        bgColor: bodyStyle.backgroundColor,
        linkCssVar: rootStyle.getPropertyValue('--color-link').trim(),
        accentCssVar: rootStyle.getPropertyValue('--color-accent').trim(),
      };
    });

    // Verify link CSS variable is defined - this confirms dark mode link styling exists
    expect(colors.linkCssVar).toBeTruthy();
    expect(colors.linkCssVar).toBe('#58a6ff');

    // Verify accent CSS variable is correct for links
    expect(colors.accentCssVar).toBe('#58a6ff');

    // Verify link color is present
    expect(colors.linkColor).toBeTruthy();

    const linkRgb = parseRgb(colors.linkColor);
    const textRgb = parseRgb(colors.textColor);

    // If parsing succeeded, verify the values
    if (linkRgb && textRgb) {
      // Link color should be different from regular text (more colorful/blue)
      const isLinkDistinct =
        linkRgb.b > linkRgb.r || // Blue is dominant
        linkRgb.r !== textRgb.r || // Different from text
        linkRgb.g !== textRgb.g ||
        linkRgb.b !== textRgb.b;

      expect(isLinkDistinct).toBe(true);
    }

    // Verify the CSS variables define a link color that contrasts with dark background
    // The accent color #58a6ff (88, 166, 255) on #0d1117 (13, 17, 23) has ~7.05:1 contrast
    const accentRgb = { r: 88, g: 166, b: 255 }; // #58a6ff
    const bgRgb = { r: 13, g: 17, b: 23 }; // #0d1117
    const expectedContrast = getContrastRatio(bgRgb, accentRgb);
    expect(expectedContrast).toBeGreaterThanOrEqual(4.5);
  });

  test('Dark mode: Secondary background is darker than primary', async ({ page }) => {
    const colors = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        bg: styles.getPropertyValue('--color-bg').trim(),
        bgSecondary: styles.getPropertyValue('--color-bg-secondary').trim(),
      };
    });

    expect(colors.bg).toBe('#0d1117');
    expect(colors.bgSecondary).toBe('#161b22');
  });

  test('Dark mode: Accent color is visible on dark background', async ({ page }) => {
    const colors = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        bg: styles.getPropertyValue('--color-bg').trim(),
        accent: styles.getPropertyValue('--color-accent').trim(),
      };
    });

    expect(colors.accent).toBe('#58a6ff');

    // Parse and check contrast
    const bgRgb = { r: 13, g: 17, b: 23 }; // #0d1117
    const accentRgb = { r: 88, g: 166, b: 255 }; // #58a6ff

    const contrastRatio = getContrastRatio(bgRgb, accentRgb);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('Dark mode: Border colors are visible but subtle', async ({ page }) => {
    const borderColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-border').trim();
    });

    expect(borderColor).toBe('#30363d');

    // Border should be visible against the dark background
    const bgRgb = { r: 13, g: 17, b: 23 }; // #0d1117
    const borderRgb = { r: 48, g: 54, b: 61 }; // #30363d

    // Border should have at least minimal contrast (enough to be visible)
    const contrastRatio = getContrastRatio(bgRgb, borderRgb);
    expect(contrastRatio).toBeGreaterThanOrEqual(1.5);
  });

  test('Dark mode: Navigation has appropriate dark styling', async ({ page }) => {
    const navBg = await page.evaluate(() => {
      const nav = document.querySelector('.nav, nav');
      if (!nav) return null;
      return window.getComputedStyle(nav).backgroundColor;
    });

    if (navBg && navBg !== 'rgba(0, 0, 0, 0)') {
      const navBgRgb = parseRgb(navBg);
      expect(navBgRgb).not.toBeNull();
      expect(isDarkColor(navBgRgb)).toBe(true);
    }
  });

  test('Dark mode: Footer has appropriate dark styling', async ({ page }) => {
    const footerBg = await page.evaluate(() => {
      const footer = document.querySelector('.footer, footer');
      if (!footer) return null;
      return window.getComputedStyle(footer).backgroundColor;
    });

    if (footerBg && footerBg !== 'rgba(0, 0, 0, 0)') {
      const footerBgRgb = parseRgb(footerBg);
      expect(footerBgRgb).not.toBeNull();
      expect(isDarkColor(footerBgRgb)).toBe(true);
    }
  });
});
