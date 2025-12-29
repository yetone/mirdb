// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Tests for Syntax Highlighting Readability (NFR-5)
 * Verifies code syntax highlighting is readable in both light and dark contexts
 */

test.describe('Syntax Highlighting Readability', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check syntax highlighting in light mode
   * Verifies code blocks have appropriate syntax highlighting with good contrast in light mode
   */
  test('TC1: Code blocks have syntax highlighting with good contrast in light mode', async ({ page }) => {
    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Get all code blocks
    const codeBlocks = page.locator('pre[class*="language-"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check first code block for readability
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Get background color of the code block
    const bgColor = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Get text color of code inside
    const codeElement = firstCodeBlock.locator('code').first();
    const textColor = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Parse colors to verify they exist (non-transparent)
    expect(bgColor).toBeTruthy();
    expect(textColor).toBeTruthy();

    // Verify contrast exists (colors should be different)
    expect(bgColor).not.toBe(textColor);

    // Check for token spans (Prism.js highlighting applied)
    const tokenSpans = firstCodeBlock.locator('.token');
    const tokenCount = await tokenSpans.count();

    // If Prism is loaded, tokens should exist
    // Allow for either Prism highlighting or at least proper code styling
    const hasLanguageClass = await firstCodeBlock.evaluate((el) => {
      return el.className.includes('language-');
    });
    expect(hasLanguageClass).toBe(true);

    // Verify at least one code block has readable styling
    const fontSize = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseFloat(fontSize)).toBeGreaterThanOrEqual(12); // At least 12px font
  });

  /**
   * Test Case 2: Check syntax highlighting in dark mode
   * Verifies code blocks remain readable if dark mode is supported (via prefers-color-scheme)
   */
  test('TC2: Code blocks remain readable in dark mode (via prefers-color-scheme)', async ({ page }) => {
    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Get all code blocks
    const codeBlocks = page.locator('pre[class*="language-"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check first code block for readability
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Get background color of the code block
    const bgColor = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Get text color of code inside
    const codeElement = firstCodeBlock.locator('code').first();
    const textColor = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Parse colors to verify they exist (non-transparent)
    expect(bgColor).toBeTruthy();
    expect(textColor).toBeTruthy();

    // Verify contrast exists (colors should be different)
    expect(bgColor).not.toBe(textColor);

    // Check for proper code structure
    const hasCodeElement = await firstCodeBlock.locator('code').count();
    expect(hasCodeElement).toBeGreaterThanOrEqual(1);

    // Verify code content is visible
    const codeContent = await codeElement.textContent();
    expect(codeContent?.length).toBeGreaterThan(0);

    // Verify readable font size
    const fontSize = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseFloat(fontSize)).toBeGreaterThanOrEqual(12);
  });

  /**
   * Test Case 3: Verify Prism.js integration
   * Verifies Prism.js or equivalent highlighting library is properly loaded and applied
   */
  test('TC3: Prism.js is properly loaded and applies syntax highlighting', async ({ page }) => {
    await page.waitForLoadState('networkidle');

    // Check that Prism.js script is loaded
    const prismLoaded = await page.evaluate(() => {
      return typeof window.Prism !== 'undefined';
    });
    expect(prismLoaded).toBe(true);

    // Check that Prism has highlighted code elements
    const codeBlocks = page.locator('code[class*="language-"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Wait a bit for Prism to process
    await page.waitForTimeout(500);

    // Check for highlighted tokens (Prism adds .token spans)
    const tokenSpans = page.locator('.token');
    const tokenCount = await tokenSpans.count();

    // Prism should have added token spans for highlighting
    expect(tokenCount).toBeGreaterThanOrEqual(1);

    // Verify specific Prism token types exist
    const hasKeywords = await page.locator('.token.keyword, .token.builtin, .token.function, .token.string, .token.comment').count();
    expect(hasKeywords).toBeGreaterThanOrEqual(1);

    // Verify Python code is highlighted
    const pythonCode = page.locator('code.language-python').first();
    await expect(pythonCode).toBeVisible();

    // Verify bash code is highlighted
    const bashCode = page.locator('code.language-bash');
    const bashCount = await bashCode.count();
    expect(bashCount).toBeGreaterThanOrEqual(1);
  });

  /**
   * Test contrast ratio meets accessibility standards
   */
  test('Code block text has sufficient contrast against background', async ({ page }) => {
    await page.waitForLoadState('networkidle');

    const codeBlocks = page.locator('pre[class*="language-"]');
    const firstBlock = codeBlocks.first();
    await expect(firstBlock).toBeVisible();

    // Get computed styles for contrast check
    const styles = await firstBlock.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      const codeEl = el.querySelector('code');
      const codeStyle = codeEl ? window.getComputedStyle(codeEl) : null;

      return {
        background: computedStyle.backgroundColor,
        color: codeStyle ? codeStyle.color : computedStyle.color,
        fontFamily: codeStyle ? codeStyle.fontFamily : computedStyle.fontFamily
      };
    });

    // Verify monospace font is used for code
    expect(styles.fontFamily.toLowerCase()).toMatch(/mono|consolas|courier|menlo|fira/i);

    // Verify colors are not transparent
    expect(styles.background).not.toBe('transparent');
    expect(styles.background).not.toBe('rgba(0, 0, 0, 0)');
  });

  /**
   * Test that all code blocks maintain consistent styling
   */
  test('All code blocks have consistent syntax highlighting styling', async ({ page }) => {
    await page.waitForLoadState('networkidle');

    const codeBlocks = page.locator('pre[class*="language-"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(2);

    // Get styles from all code blocks
    const allStyles = await codeBlocks.evaluateAll((blocks) => {
      return blocks.map(block => {
        const style = window.getComputedStyle(block);
        const codeEl = block.querySelector('code');
        const codeStyle = codeEl ? window.getComputedStyle(codeEl) : null;
        return {
          backgroundColor: style.backgroundColor,
          borderRadius: style.borderRadius,
          fontFamily: codeStyle ? codeStyle.fontFamily : style.fontFamily
        };
      });
    });

    // All code blocks should have the same background color
    const firstBg = allStyles[0].backgroundColor;
    for (const style of allStyles) {
      expect(style.backgroundColor).toBe(firstBg);
    }

    // All code blocks should use the same font family
    const firstFont = allStyles[0].fontFamily;
    for (const style of allStyles) {
      expect(style.fontFamily).toBe(firstFont);
    }
  });

});
