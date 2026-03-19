/**
 * E2E tests for Quick Start section.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to the Quick Start section
    await page.locator('#quick-start').scrollIntoViewIfNeeded();
  });

  // Test Case 4: Click copy button on code block
  test('TC4: Copy button copies command to clipboard and shows success state', async ({
    page,
    context,
  }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first code block with a copy button
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Find the copy button
    const copyButton = codeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Verify initial state shows "Copy"
    await expect(copyButton).toContainText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify the button shows "Copied!" state
    await expect(copyButton).toContainText('Copied!');

    // Verify aria-label is updated
    await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');

    // Verify clipboard contains the command (git clone command)
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardText).toContain('git clone https://github.com/yetone/mirdb.git');
  });

  test('Quick Start section displays prerequisites', async ({ page }) => {
    const prerequisitesSection = page.locator('[data-testid="prerequisites-section"]');
    await expect(prerequisitesSection).toBeVisible();

    // Check for Rust toolchain
    await expect(prerequisitesSection).toContainText('Rust toolchain');
  });

  test('Quick Start section displays installation steps', async ({ page }) => {
    const installationSection = page.locator('[data-testid="installation-section"]');
    await expect(installationSection).toBeVisible();

    // Verify step descriptions
    await expect(page.getByText('1. Clone the repository')).toBeVisible();
    await expect(page.getByText('2. Navigate to the project directory')).toBeVisible();
    await expect(page.getByText('3. Build and run MirDB')).toBeVisible();
  });

  test('Quick Start section displays memcached connection example', async ({ page }) => {
    const usageSection = page.locator('[data-testid="usage-section"]');
    await expect(usageSection).toBeVisible();

    // Verify heading
    await expect(page.getByText('Connect Using Memcached Protocol')).toBeVisible();
  });

  test('All code blocks have copy buttons', async ({ page }) => {
    const codeBlocks = page.locator('#quick-start [data-testid="code-block"]');
    const count = await codeBlocks.count();

    // Should have at least 4 code blocks (3 installation steps + 1 usage example)
    expect(count).toBeGreaterThanOrEqual(4);

    // Each code block should have a copy button
    for (let i = 0; i < count; i++) {
      const copyButton = codeBlocks.nth(i).locator('[data-testid="copy-button"]');
      await expect(copyButton).toBeVisible();
    }
  });

  test('Code blocks display language indicators', async ({ page }) => {
    const languageLabels = page.locator('#quick-start [data-testid="code-language"]');
    const count = await languageLabels.count();

    expect(count).toBeGreaterThanOrEqual(4);

    // All should show "bash" language
    for (let i = 0; i < count; i++) {
      await expect(languageLabels.nth(i)).toContainText('bash');
    }
  });

  test('Copy button resets to initial state after delay', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page
      .locator('[data-testid="code-block"]')
      .first()
      .locator('[data-testid="copy-button"]');

    // Click copy
    await copyButton.click();
    await expect(copyButton).toContainText('Copied!');

    // Wait for reset (default is 2 seconds + buffer)
    await page.waitForTimeout(2500);

    // Should reset to "Copy"
    await expect(copyButton).toContainText('Copy');
  });

  // Accessibility tests
  test('Quick Start section has proper heading structure', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toHaveAttribute('aria-labelledby', 'quick-start-heading');

    const heading = page.locator('#quick-start-heading');
    await expect(heading).toHaveText('Quick Start');
  });

  test('Prerequisites list has proper accessibility attributes', async ({ page }) => {
    const prerequisitesList = page.locator('[aria-label="Prerequisites list"]');
    await expect(prerequisitesList).toBeVisible();
  });

  test('Copy buttons have accessible labels', async ({ page }) => {
    const copyButtons = page.locator('#quick-start [data-testid="copy-button"]');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBe('Copy code to clipboard');
    }
  });
});
