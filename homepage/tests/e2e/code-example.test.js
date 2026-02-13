/**
 * E2E Tests for Code Example Section
 * Owner: Scenario 3 - Code Example Section
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Example Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 3: Click copy button shows visual feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Click copy button
    const copyButton = page.locator('.code-block__copy');
    await copyButton.click();

    // Check visual feedback is shown
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);

    // Check text changes to "Copied!"
    const copyText = page.locator('.code-block__copy-text');
    await expect(copyText).toHaveText('Copied!');
  });

  test('Test Case 4: Clipboard contains complete code example after copy', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Click copy button
    const copyButton = page.locator('.code-block__copy');
    await copyButton.click();

    // Read clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify clipboard contains the code example
    expect(clipboardContent).toContain('set key 0 3600 5');
    expect(clipboardContent).toContain('get key');
    expect(clipboardContent).toContain('hello');
    expect(clipboardContent).toContain('VALUE key 0 5');
  });

  test('Test Case 6: Copy button is focusable via keyboard', async ({ page }) => {
    const copyButton = page.locator('.code-block__copy');

    // Focus the copy button
    await copyButton.focus();
    await expect(copyButton).toBeFocused();
  });

  test('Test Case 6: Copy button activatable via Enter key', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('.code-block__copy');
    await copyButton.focus();
    await page.keyboard.press('Enter');

    // Check visual feedback is shown
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);
  });

  test('Test Case 6: Copy button activatable via Space key', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('.code-block__copy');
    await copyButton.focus();
    await page.keyboard.press('Space');

    // Check visual feedback is shown
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);
  });

  test('Copy feedback resets after delay', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('.code-block__copy');
    await copyButton.click();

    // Initially shows "Copied!"
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);

    // Wait for reset (2 seconds + buffer)
    await page.waitForTimeout(2500);

    // Should reset back to normal state
    await expect(copyButton).not.toHaveClass(/code-block__copy--copied/);
    const copyText = page.locator('.code-block__copy-text');
    await expect(copyText).toHaveText('Copy');
  });
});
