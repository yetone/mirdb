/**
 * Syntax Highlighting Integration Tests
 * Owner: Scenario 15 - Code Syntax Highlighting
 *
 * Test cases:
 * - Code blocks render with syntax highlighting visible
 * - Syntax highlighting adapts to dark theme colors
 * - Token colors are visible against code block background
 */

import { test, expect } from '@playwright/test';

test.describe('Code Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to quickstart section where code blocks are
    await page.locator('#quickstart').scrollIntoViewIfNeeded();
  });

  test('TC1: Code blocks have syntax highlighting classes applied', async ({ page }) => {
    // Check that code blocks exist with data-language attribute
    const codeBlocks = page.locator('.code-block[data-language]');
    await expect(codeBlocks).toHaveCount(3); // bash install, bash usage, rust example

    // Check bash code block
    const bashBlock = page.locator('.code-block[data-language="bash"]').first();
    await expect(bashBlock).toBeVisible();

    // Check rust code block
    const rustBlock = page.locator('.code-block[data-language="rust"]');
    await expect(rustBlock).toBeVisible();

    // Check that code elements have language classes
    const bashCode = page.locator('code.language-bash').first();
    await expect(bashCode).toBeVisible();

    const rustCode = page.locator('code.language-rust');
    await expect(rustCode).toBeVisible();
  });

  test('TC2: Rust keywords (fn, let, mut) are highlighted', async ({ page }) => {
    const rustCode = page.locator('code.language-rust');
    await expect(rustCode).toBeVisible();

    // Check for keyword tokens
    const keywordTokens = rustCode.locator('.token.keyword');
    await expect(keywordTokens.first()).toBeVisible();

    // Count keywords - should have fn, let (multiple), mut, use
    const count = await keywordTokens.count();
    expect(count).toBeGreaterThanOrEqual(4);

    // Verify specific keywords are present
    await expect(rustCode).toContainText('fn');
    await expect(rustCode).toContainText('let');
    await expect(rustCode).toContainText('mut');
    await expect(rustCode).toContainText('use');

    // Verify keywords have distinct color from regular text
    const keywordColor = await keywordTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    const codeColor = await rustCode.evaluate(el =>
      window.getComputedStyle(el).color
    );
    // Keywords should have a different color than default code text
    expect(keywordColor).not.toBe(codeColor);
  });

  test('TC3: Shell/bash syntax is highlighted appropriately', async ({ page }) => {
    const bashCode = page.locator('code.language-bash').first();
    await expect(bashCode).toBeVisible();

    // Check for comment tokens (# comments)
    const commentTokens = bashCode.locator('.token.comment');
    await expect(commentTokens.first()).toBeVisible();
    await expect(commentTokens.first()).toContainText('#');

    // Check for command tokens
    const commandTokens = bashCode.locator('.token.command');
    await expect(commandTokens.first()).toBeVisible();

    // Verify comment has distinct styling (italic and different color)
    const commentStyle = await commentTokens.first().evaluate(el => ({
      color: window.getComputedStyle(el).color,
      fontStyle: window.getComputedStyle(el).fontStyle
    }));
    expect(commentStyle.fontStyle).toBe('italic');

    // Verify command has distinct color
    const commandColor = await commandTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    const commentColor = commentStyle.color;
    expect(commandColor).not.toBe(commentColor);
  });

  test('TC4: Toggle to dark mode - syntax highlighting adapts to dark theme colors', async ({ page }) => {
    // Get initial colors in light mode
    const rustCode = page.locator('code.language-rust');
    await expect(rustCode).toBeVisible();

    const keywordToken = rustCode.locator('.token.keyword').first();
    const lightKeywordColor = await keywordToken.evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Toggle to dark mode
    const themeToggle = page.locator('#theme-toggle');
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(350);

    // Verify dark mode is active
    const htmlTheme = await page.locator('html').getAttribute('data-theme');
    expect(htmlTheme).toBe('dark');

    // Get colors in dark mode
    const darkKeywordColor = await keywordToken.evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Colors should be different between light and dark mode
    expect(darkKeywordColor).not.toBe(lightKeywordColor);

    // Verify code block background also changed
    const codeBlock = page.locator('.code-block').first();
    const darkBgColor = await codeBlock.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );

    // Dark mode should have a darker background (lower RGB values)
    // Parse RGB values to verify darkness
    const rgbMatch = darkBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Dark background should have low RGB values
      expect(r + g + b).toBeLessThan(150);
    }
  });

  test('Rust types are highlighted with distinct color', async ({ page }) => {
    const rustCode = page.locator('code.language-rust');
    const typeTokens = rustCode.locator('.token.type');
    await expect(typeTokens.first()).toBeVisible();

    // Verify types have distinct color
    const typeColor = await typeTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    const keywordColor = await rustCode.locator('.token.keyword').first().evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Types should have different color than keywords
    expect(typeColor).not.toBe(keywordColor);
  });

  test('Rust macros are highlighted', async ({ page }) => {
    const rustCode = page.locator('code.language-rust');
    const macroToken = rustCode.locator('.token.macro');
    await expect(macroToken).toBeVisible();
    await expect(macroToken).toContainText('println!');

    // Verify macro has distinct color
    const macroColor = await macroToken.evaluate(el =>
      window.getComputedStyle(el).color
    );
    const keywordColor = await rustCode.locator('.token.keyword').first().evaluate(el =>
      window.getComputedStyle(el).color
    );

    expect(macroColor).not.toBe(keywordColor);
  });

  test('Rust strings are highlighted with distinct color', async ({ page }) => {
    const rustCode = page.locator('code.language-rust');
    const stringTokens = rustCode.locator('.token.string');
    await expect(stringTokens.first()).toBeVisible();

    // Verify string content
    await expect(stringTokens.first()).toContainText('memcache://');

    // Verify string has distinct color
    const stringColor = await stringTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    const keywordColor = await rustCode.locator('.token.keyword').first().evaluate(el =>
      window.getComputedStyle(el).color
    );

    expect(stringColor).not.toBe(keywordColor);
  });

  test('Rust comments have italic styling', async ({ page }) => {
    const rustCode = page.locator('code.language-rust');
    const commentToken = rustCode.locator('.token.comment').first();
    await expect(commentToken).toBeVisible();

    const fontStyle = await commentToken.evaluate(el =>
      window.getComputedStyle(el).fontStyle
    );
    expect(fontStyle).toBe('italic');
  });

  test('Code block has proper contrast in both themes', async ({ page }) => {
    const codeBlock = page.locator('.code-block').first();
    const codeElement = page.locator('code.language-bash').first();

    // Check light mode contrast
    const lightBg = await codeBlock.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    const lightText = await codeElement.evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Toggle to dark mode
    await page.locator('#theme-toggle').click();
    await page.waitForTimeout(350);

    const darkBg = await codeBlock.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    const darkText = await codeElement.evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Backgrounds should be different between themes
    expect(lightBg).not.toBe(darkBg);

    // Text colors should be different between themes
    expect(lightText).not.toBe(darkText);
  });

  test('All three code blocks are visible and highlighted', async ({ page }) => {
    // Install code block
    const installCode = page.locator('#install-code');
    await expect(installCode).toBeVisible();
    await expect(installCode.locator('.token.command').first()).toBeVisible();

    // Usage code block
    const usageCode = page.locator('#usage-code');
    await expect(usageCode).toBeVisible();
    await expect(usageCode.locator('.token.command').first()).toBeVisible();

    // Rust code block
    const rustCode = page.locator('#rust-code');
    await expect(rustCode).toBeVisible();
    await expect(rustCode.locator('.token.keyword').first()).toBeVisible();
  });

  test('Language indicator shows correct language', async ({ page }) => {
    // Check bash language indicator
    const bashLang = page.locator('.code-block[data-language="bash"] .code-block-lang').first();
    await expect(bashLang).toHaveText('bash');

    // Check rust language indicator
    const rustLang = page.locator('.code-block[data-language="rust"] .code-block-lang');
    await expect(rustLang).toHaveText('rust');
  });

  test('Syntax highlighting persists after theme toggle back to light', async ({ page }) => {
    const rustCode = page.locator('code.language-rust');
    const keywordToken = rustCode.locator('.token.keyword').first();

    // Helper to parse RGB color to compare approximately
    const parseRgb = (rgbStr) => {
      const match = rgbStr.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
      }
      return null;
    };

    // Check color similarity (allowing for minor transition timing differences)
    const colorsAreSimilar = (color1, color2, tolerance = 20) => {
      const rgb1 = parseRgb(color1);
      const rgb2 = parseRgb(color2);
      if (!rgb1 || !rgb2) return false;
      return Math.abs(rgb1.r - rgb2.r) <= tolerance &&
             Math.abs(rgb1.g - rgb2.g) <= tolerance &&
             Math.abs(rgb1.b - rgb2.b) <= tolerance;
    };

    // Get initial light mode color
    const initialColor = await keywordToken.evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Toggle to dark mode
    await page.locator('#theme-toggle').click();
    await page.waitForTimeout(400);

    // Toggle back to light mode
    await page.locator('#theme-toggle').click();
    await page.waitForTimeout(400);

    // Get final light mode color
    const finalColor = await keywordToken.evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Colors should be similar after toggle cycle (with small tolerance for transition timing)
    expect(colorsAreSimilar(finalColor, initialColor)).toBe(true);
  });
});
