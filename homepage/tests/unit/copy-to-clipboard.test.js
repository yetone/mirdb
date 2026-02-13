/**
 * Unit Tests for Copy to Clipboard Module
 * Owner: Scenario 3 - Code Example Section
 *
 * Tests code block structure, copy button, and clipboard functionality
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Block Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Code block contains required Memcached commands per PRD', async ({ page }) => {
    const codeContent = page.locator('#code-content');
    await expect(codeContent).toBeVisible();

    const text = await codeContent.textContent();

    // Contains 'set key 0 3600 5' and 'get key' commands as per PRD
    expect(text).toContain('set key 0 3600 5');
    expect(text).toContain('get key');
    expect(text).toContain('VALUE key 0 5');
    expect(text).toContain('hello');
    expect(text).toContain('STORED');
    expect(text).toContain('END');
  });

  test('Test Case 2: Copy button is visible and accessible within code block', async ({ page }) => {
    const copyButton = page.locator('.code-block__copy');

    // Copy button should be visible
    await expect(copyButton).toBeVisible();

    // Copy button should have correct data attribute
    await expect(copyButton).toHaveAttribute('data-copy-target', 'code-content');

    // Copy button should have accessible label
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');

    // Copy button should have proper type
    await expect(copyButton).toHaveAttribute('type', 'button');
  });

  test('Test Case 5: Code has monospace font and terminal appearance', async ({ page }) => {
    const codeContent = page.locator('.code-block__content');
    await expect(codeContent).toBeVisible();

    // Check computed font family includes monospace
    const fontFamily = await codeContent.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily.toLowerCase()).toMatch(/(mono|courier|consolas|monaco)/i);

    // Check dark terminal-style background
    const codeBlock = page.locator('.code-block');
    const bgColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bgColor).toMatch(/rgb\(30,\s*30,\s*30\)/);
  });

  test('Test Case 7: copyText function works correctly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Test the copyText function directly
    const result = await page.evaluate(async () => {
      // The copyText function should be available globally
      if (typeof window.copyText === 'function') {
        return await window.copyText('test text');
      }
      return false;
    });

    expect(result).toBe(true);

    // Verify clipboard contains the text
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardContent).toBe('test text');
  });

  test('Code block has syntax highlighting classes', async ({ page }) => {
    // Check for syntax highlighting elements
    const commentHighlight = page.locator('.code-comment');
    const commandHighlight = page.locator('.code-command');
    const responseHighlight = page.locator('.code-response');
    const valueHighlight = page.locator('.code-value');

    await expect(commentHighlight.first()).toBeVisible();
    await expect(commandHighlight.first()).toBeVisible();
    await expect(responseHighlight.first()).toBeVisible();
    await expect(valueHighlight.first()).toBeVisible();
  });

  test('Code section has proper semantic structure', async ({ page }) => {
    // Verify code section is a section element
    const codeSection = page.locator('section#code-example');
    await expect(codeSection).toBeVisible();

    // Verify it has aria-labelledby for accessibility
    await expect(codeSection).toHaveAttribute('aria-labelledby', 'code-title');

    // Verify title exists and is visible
    const title = page.locator('#code-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Memcached Protocol');
  });

  test('Code block has description text', async ({ page }) => {
    const description = page.locator('.code-section__description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Connect with any Memcached client');
  });
});
