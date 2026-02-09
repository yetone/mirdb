/**
 * E2E tests for QuickStart section copy functionality.
 * Owner: Scenario 3 - Quick Start Section
 *
 * Test cases 6 and 7: Copy button functionality and visual feedback
 */

import { test, expect } from '@playwright/test';

test.describe('QuickStart copy functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for quickstart section to be rendered
    await page.waitForSelector('#quickstart');
  });

  test('Test case 6: clicking copy button copies code to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first copy button
    const copyButton = page.locator('.code-block-wrapper .copy-button').first();
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify the clipboard contains the code
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('cargo install mirdb');
  });

  test('Test case 7: visual feedback indicates successful copy', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first copy button
    const copyButton = page.locator('.code-block-wrapper .copy-button').first();
    await expect(copyButton).toBeVisible();

    // Verify initial state - should show "Copy" text
    const copyText = copyButton.locator('.copy-text');
    await expect(copyText).toHaveText('Copy');

    // Click the copy button
    await copyButton.click();

    // Wait for visual feedback - should show "Copied!" text
    await expect(copyText).toHaveText('Copied!');

    // Button should have 'copied' class
    await expect(copyButton).toHaveClass(/copied/);

    // Check icon changes - copy icon hidden, check icon visible
    const copyIcon = copyButton.locator('.copy-icon');
    const checkIcon = copyButton.locator('.check-icon');
    await expect(copyIcon).toHaveCSS('display', 'none');
    await expect(checkIcon).toHaveCSS('display', 'block');

    // Wait for feedback to reset (2 seconds)
    await page.waitForTimeout(2100);

    // Verify reset state
    await expect(copyText).toHaveText('Copy');
    await expect(copyButton).not.toHaveClass(/copied/);
  });

  test('copy button works for all code blocks', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Get all copy buttons
    const copyButtons = page.locator('.code-block-wrapper .copy-button');
    const count = await copyButtons.count();

    // Should have at least 3 copy buttons (install, start, example commands)
    expect(count).toBeGreaterThanOrEqual(3);

    // Test each copy button works
    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      await button.click();

      // Verify visual feedback
      const copyText = button.locator('.copy-text');
      await expect(copyText).toHaveText('Copied!');

      // Wait a bit before next click
      await page.waitForTimeout(500);
    }
  });
});
