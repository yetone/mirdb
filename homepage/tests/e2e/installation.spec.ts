/**
 * Installation Section E2E Tests
 * Owner: Scenario 3 - Installation Instructions
 *
 * Test cases:
 * 4. Click copy button in Installation section - code is copied to clipboard and visual feedback is shown
 */

import { test, expect } from '@playwright/test';

test.describe('Installation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 4: Code is copied to clipboard and visual feedback is shown
  test('clicking copy button copies code and shows visual feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to installation section
    const installationSection = page.locator('#installation');
    await installationSection.scrollIntoViewIfNeeded();

    // Find the first copy button
    const copyButton = page.locator('.copy-button').first();
    await expect(copyButton).toBeVisible();

    // Verify initial state shows "Copy"
    await expect(copyButton).toContainText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback - button should show "Copied!"
    await expect(copyButton).toContainText('Copied!');

    // Verify the clipboard contains the expected command
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('git clone');
  });

  test('installation section is visible and contains code blocks', async ({ page }) => {
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Check for code elements
    const codeBlocks = page.locator('.code-block');
    await expect(codeBlocks).toHaveCount(3); // 3 installation steps
  });

  test('all copy buttons are functional', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const installationSection = page.locator('#installation');
    await installationSection.scrollIntoViewIfNeeded();

    const copyButtons = page.locator('.copy-button');
    const count = await copyButtons.count();

    // Click each copy button and verify feedback
    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      await button.click();
      await expect(button).toContainText('Copied!');

      // Wait for the "Copied!" message to reset before clicking next button
      await page.waitForTimeout(2100);
    }
  });

  test('copy buttons have proper accessibility attributes', async ({ page }) => {
    const copyButtons = page.locator('.copy-button');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      await expect(button).toHaveAttribute('aria-label', /copy/i);
      await expect(button).toHaveAttribute('type', 'button');
    }
  });

  test('installation section has proper heading structure', async ({ page }) => {
    const heading = page.locator('#installation-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Installation');
    await expect(heading).toHaveAttribute('id', 'installation-title');
  });
});
