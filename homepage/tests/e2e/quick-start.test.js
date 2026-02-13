/**
 * E2E Tests for Quick Start Section
 * Owner: Scenario 4 - Quick Start Section Implementation
 *
 * Tests user interactions including copy-to-clipboard functionality
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 6: Click copy button for first command - command is copied successfully', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find the first copy button
    const firstCopyButton = quickstartSection.locator('[data-copy-target]').first();
    await expect(firstCopyButton).toBeVisible();

    // Get the target content that should be copied
    const targetId = await firstCopyButton.getAttribute('data-copy-target');
    const targetElement = page.locator(`#${targetId}`);
    const expectedText = await targetElement.textContent();

    // Click the copy button
    await firstCopyButton.click();

    // Verify visual feedback (button shows "Copied!" state)
    const copyText = firstCopyButton.locator('.code-block__copy-text');
    await expect(copyText).toHaveText('Copied!', { timeout: 2000 });

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText.trim()).toBe(expectedText.trim());

    // Wait for feedback to reset
    await expect(copyText).toHaveText('Copy', { timeout: 3000 });
  });

  test('All copy buttons work correctly', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get all copy buttons
    const copyButtons = quickstartSection.locator('[data-copy-target]');
    const buttonCount = await copyButtons.count();

    expect(buttonCount).toBeGreaterThanOrEqual(3); // At least 3 commands

    // Test each copy button
    for (let i = 0; i < buttonCount; i++) {
      const button = copyButtons.nth(i);
      await button.scrollIntoViewIfNeeded();

      // Get expected text
      const targetId = await button.getAttribute('data-copy-target');
      const targetElement = page.locator(`#${targetId}`);
      const expectedText = await targetElement.textContent();

      // Click copy
      await button.click();

      // Verify clipboard
      const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardText.trim()).toBe(expectedText.trim());

      // Wait a moment before next copy to allow feedback reset
      await page.waitForTimeout(300);
    }
  });

  test('Copy button keyboard accessibility', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Focus the first copy button
    const firstCopyButton = quickstartSection.locator('[data-copy-target]').first();
    await firstCopyButton.focus();

    // Get expected text
    const targetId = await firstCopyButton.getAttribute('data-copy-target');
    const targetElement = page.locator(`#${targetId}`);
    const expectedText = await targetElement.textContent();

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Verify copy worked
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText.trim()).toBe(expectedText.trim());
  });

  test('Navigation from hero Get Started button to Quick Start section', async ({ page }) => {
    // Click Get Started button in hero
    const getStartedBtn = page.locator('.hero__cta--primary');
    await getStartedBtn.click();

    // Verify URL has #quickstart
    await expect(page).toHaveURL(/#quickstart/);

    // Verify quick start section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport({ ratio: 0.5 });
  });

  test('Quick Start section is accessible via nav link', async ({ page }) => {
    // Click Quick Start link in nav
    const navLink = page.locator('.nav__link', { hasText: 'Quick Start' });
    await navLink.click();

    // Verify navigation worked
    await expect(page).toHaveURL(/#quickstart/);

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport({ ratio: 0.5 });
  });

  test('Commands are displayed in terminal-styled blocks', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check command blocks have dark background (terminal style)
    const commandBlocks = quickstartSection.locator('.quickstart__command');
    const firstCommandBlock = commandBlocks.first();

    // Verify terminal styling
    const bgColor = await firstCommandBlock.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Should be dark color (rgb value for #1e1e1e is rgb(30, 30, 30))
    expect(bgColor).toMatch(/rgb\(30, 30, 30\)|#1e1e1e/);

    // Verify monospace font on command content
    const commandContent = firstCommandBlock.locator('.quickstart__command-content');
    const fontFamily = await commandContent.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|menlo|courier/);
  });
});
