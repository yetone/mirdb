// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Copy-to-Clipboard Cross-Browser Tests
 * Test Case 5: Test copy-to-clipboard in all browsers
 *
 * Tests the copy-to-clipboard functionality across Chrome, Firefox, Safari (WebKit), and Edge.
 * Note: The current page may not have a copy button implemented, so we test that
 * the infrastructure supports clipboard operations if/when added.
 */

test.describe('Copy-to-Clipboard Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case: Verify code block content is selectable
  test('code block content is selectable', async ({ page }) => {
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const codeElement = codeBlock.locator('code');
    await expect(codeElement).toBeVisible();

    // Get the code content
    const codeContent = await codeElement.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent).toContain('mirdb');
  });

  // Test Case: Verify browser supports Clipboard API
  test('browser supports Clipboard API', async ({ page }) => {
    const hasClipboardAPI = await page.evaluate(() => {
      return typeof navigator.clipboard !== 'undefined' &&
        typeof navigator.clipboard.writeText === 'function';
    });

    // All modern browsers should support Clipboard API
    expect(hasClipboardAPI).toBe(true);
  });

  // Test Case: Simulate clipboard copy operation
  test('clipboard write operation works in secure context', async ({ page, context, browserName }) => {
    // Skip clipboard permission test for Firefox (it doesn't support granting these permissions)
    // Firefox handles clipboard differently - requires user gesture
    if (browserName === 'firefox') {
      test.skip();
      return;
    }

    // Grant clipboard permissions for Chromium-based browsers
    try {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    } catch (e) {
      // WebKit may not support clipboard permissions, skip the test
      test.skip();
      return;
    }

    // Test clipboard write functionality
    const testText = 'MirDB test copy';

    const copyResult = await page.evaluate(async (text) => {
      try {
        await navigator.clipboard.writeText(text);
        return { success: true };
      } catch (error) {
        return { success: false, error: error.message };
      }
    }, testText);

    // Clipboard write should succeed in secure context (localhost)
    expect(copyResult.success).toBe(true);

    // Verify we can read back the content
    const clipboardContent = await page.evaluate(async () => {
      try {
        return await navigator.clipboard.readText();
      } catch (error) {
        return null;
      }
    });

    expect(clipboardContent).toBe(testText);
  });

  // Test Case: Code block has proper user-select CSS
  test('code content has proper CSS for text selection', async ({ page }) => {
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const userSelect = await codeBlock.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.userSelect || style.webkitUserSelect || style.mozUserSelect;
    });

    // User select should not be 'none' for code blocks (should be selectable)
    expect(userSelect).not.toBe('none');
  });

  // Test Case: Pre element preserves whitespace for copy
  test('pre element preserves whitespace correctly', async ({ page }) => {
    const preElement = page.locator('.code-block pre');
    await expect(preElement).toBeVisible();

    const whiteSpace = await preElement.evaluate((el) =>
      window.getComputedStyle(el).whiteSpace
    );

    // Pre elements should preserve whitespace
    expect(['pre', 'pre-wrap', 'pre-line']).toContain(whiteSpace);
  });

  // Test Case: Code block overflow handling
  test('code block handles overflow correctly', async ({ page }) => {
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const overflowX = await codeBlock.evaluate((el) =>
      window.getComputedStyle(el).overflowX
    );

    // Code block should have auto or scroll for horizontal overflow
    expect(['auto', 'scroll']).toContain(overflowX);
  });
});

test.describe('Copy Functionality Infrastructure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case: Document execCommand fallback support
  test('document.execCommand copy fallback is available', async ({ page }) => {
    const hasExecCommand = await page.evaluate(() => {
      return typeof document.execCommand === 'function';
    });

    // execCommand should be available as fallback for older approaches
    expect(hasExecCommand).toBe(true);
  });

  // Test Case: Selection API is available
  test('Selection API is available for text selection', async ({ page }) => {
    const hasSelectionAPI = await page.evaluate(() => {
      return typeof window.getSelection === 'function' &&
        typeof document.createRange === 'function';
    });

    expect(hasSelectionAPI).toBe(true);
  });

  // Test Case: Can create temporary textarea for copy
  test('can create temporary elements for copy operations', async ({ page }) => {
    const canCreateElements = await page.evaluate(() => {
      const textarea = document.createElement('textarea');
      textarea.value = 'test';
      document.body.appendChild(textarea);
      textarea.select();
      const selected = textarea.selectionStart !== textarea.selectionEnd;
      document.body.removeChild(textarea);
      return selected;
    });

    expect(canCreateElements).toBe(true);
  });
});
