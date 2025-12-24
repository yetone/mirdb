// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Syntax Highlighting in Code Examples E2E Tests (NFR-5)
 * Verifies that code examples have proper syntax highlighting
 */

test.describe('Syntax Highlighting in Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for highlight.js to initialize
    await page.waitForFunction(() => {
      return typeof window.hljs !== 'undefined';
    }, { timeout: 10000 });
  });

  test('TC1: TOML configuration code block has syntax highlighting', async ({ page }) => {
    // Navigate to Quick Start section where TOML config is displayed
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find the TOML code block (has language-toml class)
    const tomlCodeBlock = quickStartSection.locator('pre code.language-toml');
    await expect(tomlCodeBlock).toBeVisible();

    // After highlight.js processes, it adds hljs class
    const codeClass = await tomlCodeBlock.getAttribute('class');
    expect(codeClass).toContain('hljs');

    // Verify TOML syntax is highlighted with distinct colors
    // Check for highlighted elements within the code block
    const highlightedElements = tomlCodeBlock.locator('.hljs-attr, .hljs-string, .hljs-number, .hljs-comment, .hljs-section, .hljs-literal');
    const count = await highlightedElements.count();
    expect(count).toBeGreaterThan(0);

    // Verify TOML-specific syntax elements are present
    // Check for key highlighting (hljs-attr for TOML keys like addr, work_dir)
    const keyElements = tomlCodeBlock.locator('.hljs-attr');
    const keyCount = await keyElements.count();
    expect(keyCount).toBeGreaterThan(0);

    // Check for string value highlighting
    const stringElements = tomlCodeBlock.locator('.hljs-string');
    const stringCount = await stringElements.count();
    expect(stringCount).toBeGreaterThan(0);

    // Verify the content contains TOML configuration
    const tomlContent = await tomlCodeBlock.textContent();
    expect(tomlContent).toContain('addr');
    expect(tomlContent).toContain('work_dir');
  });

  test('TC2: Shell command code blocks have syntax highlighting', async ({ page }) => {
    // Navigate to Quick Start section where shell commands are displayed
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find shell/bash code blocks
    const bashCodeBlocks = quickStartSection.locator('pre code.language-bash');
    const count = await bashCodeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check the first bash code block
    const firstBashBlock = bashCodeBlocks.first();
    await expect(firstBashBlock).toBeVisible();

    // Verify highlight.js has processed it
    const codeClass = await firstBashBlock.getAttribute('class');
    expect(codeClass).toContain('hljs');

    // Verify bash syntax highlighting elements are present
    // Comments should be highlighted
    const commentElements = firstBashBlock.locator('.hljs-comment');
    const commentCount = await commentElements.count();
    expect(commentCount).toBeGreaterThan(0);

    // Verify the content contains shell commands
    const bashContent = await firstBashBlock.textContent();
    expect(bashContent).toMatch(/git|cargo|install|clone/i);
  });

  test('TC3: Code blocks have proper styling (monospace font, background, padding)', async ({ page }) => {
    // Check code blocks across different sections
    const preElements = page.locator('pre');
    const codeElements = page.locator('pre code');

    const preCount = await preElements.count();
    expect(preCount).toBeGreaterThan(0);

    // Check first pre element for styling
    const firstPre = preElements.first();
    await expect(firstPre).toBeVisible();

    // Verify background color is set
    const bgColor = await firstPre.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should not be transparent (rgba(0, 0, 0, 0)) or default white
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bgColor).not.toBe('rgb(255, 255, 255)');

    // Verify padding is applied
    const padding = await firstPre.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        top: parseInt(style.paddingTop),
        right: parseInt(style.paddingRight),
        bottom: parseInt(style.paddingBottom),
        left: parseInt(style.paddingLeft)
      };
    });
    expect(padding.top).toBeGreaterThan(0);
    expect(padding.left).toBeGreaterThan(0);

    // Verify monospace font family on code element
    const firstCode = codeElements.first();
    const fontFamily = await firstCode.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    // Check if font family contains monospace-related keywords
    const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                        fontFamily.toLowerCase().includes('consolas') ||
                        fontFamily.toLowerCase().includes('courier') ||
                        fontFamily.toLowerCase().includes('fira') ||
                        fontFamily.toLowerCase().includes('jetbrains');
    expect(isMonospace).toBe(true);

    // Verify border-radius for rounded corners
    const borderRadius = await firstPre.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });
    // Should have some border-radius (not 0px)
    expect(borderRadius).not.toBe('0px');
  });

  test('TC4: Code blocks are scrollable on overflow', async ({ page }) => {
    // Check that pre elements have overflow-x: auto for horizontal scrolling
    const preElements = page.locator('pre');
    const count = await preElements.count();
    expect(count).toBeGreaterThan(0);

    // Check each pre element for overflow handling
    for (let i = 0; i < Math.min(count, 5); i++) {
      const pre = preElements.nth(i);
      await expect(pre).toBeVisible();

      const overflowX = await pre.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      // Should be 'auto' or 'scroll' to enable horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowX);
    }

    // Additionally verify that code blocks have proper max-width/width handling
    const quickStartSection = page.locator('#quick-start');
    const quickStartPre = quickStartSection.locator('pre').first();
    await expect(quickStartPre).toBeVisible();

    // Verify the element doesn't overflow its container
    const containerWidth = await quickStartSection.evaluate((el) => el.offsetWidth);
    const preWidth = await quickStartPre.evaluate((el) => el.scrollWidth);

    // Pre element's scroll width might be larger than container if content is long,
    // but the visible width should fit within the container
    const visibleWidth = await quickStartPre.evaluate((el) => el.offsetWidth);
    expect(visibleWidth).toBeLessThanOrEqual(containerWidth);
  });

  test('Code blocks across all sections have consistent highlighting', async ({ page }) => {
    // Verify highlight.js is loaded and initialized
    const hljsLoaded = await page.evaluate(() => {
      return typeof window.hljs !== 'undefined' && typeof window.hljs.highlightAll === 'function';
    });
    expect(hljsLoaded).toBe(true);

    // Check all code blocks on the page
    const allCodeBlocks = page.locator('pre code');
    const count = await allCodeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify each code block has been processed by highlight.js
    for (let i = 0; i < count; i++) {
      const codeBlock = allCodeBlocks.nth(i);
      const classList = await codeBlock.getAttribute('class');

      // Each processed code block should have hljs class
      expect(classList).toContain('hljs');
    }
  });
});
