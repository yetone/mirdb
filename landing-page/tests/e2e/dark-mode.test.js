/**
 * MirDB Landing Page - Dark Mode E2E Tests
 * Owner: Scenario 16 - Dark Mode Design
 *
 * Tests verify the dark mode styling is applied correctly:
 * - Page background is dark
 * - Text has appropriate contrast
 * - Code blocks have dark theme
 * - Links are visible and distinguishable
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Dark Mode Design', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('page background is dark (consistent with developer environments)', async ({ page }) => {
    // Test Case ID: 1
    // Check that the body has a dark background color
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Parse the RGB values from the computed style
    const rgbMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);

    // A dark background should have low RGB values (less than 50 for a typical dark theme)
    // #0d1117 = rgb(13, 17, 23)
    expect(r).toBeLessThan(50);
    expect(g).toBeLessThan(50);
    expect(b).toBeLessThan(50);

    // Also verify that the CSS variable is set correctly
    const bgVariable = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg').trim();
    });
    expect(bgVariable).toBe('#0d1117');
  });

  test('text is light colored and meets contrast requirements', async ({ page }) => {
    // Test Case ID: 2
    // Check that text color is light on the dark background
    const body = page.locator('body');
    const textColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Parse the RGB values from the computed style
    const rgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);

    // Light text should have high RGB values (above 150 for readability)
    // #c9d1d9 = rgb(201, 209, 217)
    expect(r).toBeGreaterThan(150);
    expect(g).toBeGreaterThan(150);
    expect(b).toBeGreaterThan(150);

    // Verify contrast ratio meets WCAG AA requirements (4.5:1 minimum)
    // Calculate relative luminance
    const getLuminance = (rVal, gVal, bVal) => {
      const [rs, gs, bs] = [rVal, gVal, bVal].map((c) => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    };

    // Get background luminance (dark)
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const bgRgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    const [, bgR, bgG, bgB] = bgRgbMatch.map(Number);

    const textLuminance = getLuminance(r, g, b);
    const bgLuminance = getLuminance(bgR, bgG, bgB);

    // Calculate contrast ratio
    const lighter = Math.max(textLuminance, bgLuminance);
    const darker = Math.min(textLuminance, bgLuminance);
    const contrastRatio = (lighter + 0.05) / (darker + 0.05);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('code blocks have appropriate dark theme with syntax highlighting', async ({ page }) => {
    // Test Case ID: 3
    // Navigate to a section with code blocks
    await page.locator('#configuration').scrollIntoViewIfNeeded();

    // Check that code blocks have dark background
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    const codeBlockBg = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Parse the RGB values
    const rgbMatch = codeBlockBg.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);

    // Code block background should be dark (slightly different from main bg)
    // #1f2428 = rgb(31, 36, 40)
    expect(r).toBeLessThan(60);
    expect(g).toBeLessThan(60);
    expect(b).toBeLessThan(60);

    // Verify the code background CSS variable
    const codeBlockBgVariable = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-code-bg').trim();
    });
    expect(codeBlockBgVariable).toBe('#1f2428');

    // Check for syntax highlighting colors
    const syntaxElements = page.locator('.toml-key, .toml-string, .toml-number, .toml-comment');
    const syntaxCount = await syntaxElements.count();

    // There should be syntax highlighting elements
    expect(syntaxCount).toBeGreaterThan(0);

    // Check that syntax highlighting colors are distinct and visible
    const tomlKey = page.locator('.toml-key').first();
    if (await tomlKey.count() > 0) {
      const keyColor = await tomlKey.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Keys should have accent color (blue) - #58a6ff = rgb(88, 166, 255)
      expect(keyColor).toMatch(/rgb\(88,\s*166,\s*255\)/);
    }
  });

  test('links are visible and distinguishable from regular text', async ({ page }) => {
    // Test Case ID: 4
    // Check link color using links that should have the accent color applied
    // Navigate to footer which has clear links with accent color
    await page.locator('.footer').scrollIntoViewIfNeeded();

    // Check footer link for accent color
    const footerLink = page.locator('.footer-license a').first();
    await expect(footerLink).toBeVisible();

    const linkColor = await footerLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Links should have accent color (blue)
    const rgbMatch = linkColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);

    // Accent color #58a6ff = rgb(88, 166, 255)
    // Links should be distinctly blue
    expect(b).toBeGreaterThan(200); // Strong blue component
    expect(b).toBeGreaterThan(r); // Blue should dominate over red

    // Verify that link color is different from regular muted text
    const mutedText = page.locator('.footer-license').first();
    const mutedTextColor = await mutedText.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Link color should be different from muted text color
    expect(linkColor).not.toBe(mutedTextColor);

    // Verify the CSS variable for accent/link color
    const accentVariable = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-accent').trim();
    });
    expect(accentVariable).toBe('#58a6ff');

    // Verify --color-link variable is also defined for link styling
    const linkVariable = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-link').trim();
    });
    expect(linkVariable).toBe('#58a6ff');
  });

  test('secondary background color is consistent across sections', async ({ page }) => {
    // Verify that sections with secondary background are using the correct dark theme color
    const nav = page.locator('.nav');
    const navBg = await nav.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Nav should have secondary background or semi-transparent version
    const rgbMatch = navBg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);

    // Secondary background should be dark but slightly lighter than main
    // #161b22 = rgb(22, 27, 34)
    expect(r).toBeLessThan(50);
    expect(g).toBeLessThan(50);
    expect(b).toBeLessThan(50);
  });

  test('feature cards have proper dark mode styling', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    const cardBg = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Feature card should have secondary background
    const rgbMatch = cardBg.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);

    // Should be dark
    expect(r).toBeLessThan(50);
    expect(g).toBeLessThan(50);
    expect(b).toBeLessThan(50);

    // Check border color is appropriate for dark mode
    const borderColor = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });

    // Border should be visible but subtle (--color-border: #30363d)
    const borderRgbMatch = borderColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(borderRgbMatch).toBeTruthy();
  });

  test('dark mode CSS variables are properly defined', async ({ page }) => {
    // Verify all critical dark mode CSS variables are set
    const cssVariables = await page.evaluate(() => {
      const style = getComputedStyle(document.documentElement);
      return {
        bg: style.getPropertyValue('--color-bg').trim(),
        bgSecondary: style.getPropertyValue('--color-bg-secondary').trim(),
        text: style.getPropertyValue('--color-text').trim(),
        textMuted: style.getPropertyValue('--color-text-muted').trim(),
        accent: style.getPropertyValue('--color-accent').trim(),
        border: style.getPropertyValue('--color-border').trim(),
        codeBg: style.getPropertyValue('--color-code-bg').trim(),
      };
    });

    // All variables should be defined
    expect(cssVariables.bg).toBe('#0d1117');
    expect(cssVariables.bgSecondary).toBe('#161b22');
    expect(cssVariables.text).toBe('#c9d1d9');
    expect(cssVariables.textMuted).toBe('#8b949e');
    expect(cssVariables.accent).toBe('#58a6ff');
    expect(cssVariables.border).toBe('#30363d');
    expect(cssVariables.codeBg).toBe('#1f2428');
  });
});
