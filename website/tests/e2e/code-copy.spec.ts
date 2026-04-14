/**
 * Code Copy Functionality E2E Tests
 * Owner: Scenario 14 - Code Copy Functionality
 *
 * Tests for:
 * - Copy button presence on code blocks
 * - Copy functionality for installation code
 * - Copy functionality for usage examples
 * - Visual feedback on successful copy
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, CONTENT } from '../fixtures/test-data';

test.describe('Code Copy Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Copy Button Presence', () => {
    test('copy button is visible on code blocks in Quick Start section', async ({ page }) => {
      // Navigate to Quick Start section
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Get all code blocks in Quick Start section
      const codeBlocks = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeBlock}`);
      const count = await codeBlocks.count();

      // There should be at least 3 code blocks (install, start server, connect and use)
      expect(count).toBeGreaterThanOrEqual(3);

      // Each code block should have a visible copy button
      for (let i = 0; i < count; i++) {
        const copyButton = codeBlocks.nth(i).locator(SELECTORS.copyButton);
        await expect(copyButton).toBeVisible();
      }
    });

    test('copy button has proper accessibility attributes', async ({ page }) => {
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const firstCopyButton = page.locator(`${SELECTORS.quickStart} ${SELECTORS.copyButton}`).first();

      // Check aria-label for accessibility
      await expect(firstCopyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
      await expect(firstCopyButton).toHaveAttribute('type', 'button');
    });

    test('copy button contains copy icon and text', async ({ page }) => {
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const firstCopyButton = page.locator(`${SELECTORS.quickStart} ${SELECTORS.copyButton}`).first();

      // Check for copy icon
      const copyIcon = firstCopyButton.locator('.copy-icon');
      await expect(copyIcon).toBeVisible();

      // Check for copy text
      const copyText = firstCopyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copy');
    });
  });

  test.describe('Installation Code Copy', () => {
    test('clicking copy button copies installation command to clipboard', async ({
      page,
      context,
    }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Find the first code block (installation)
      const installCodeBlock = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeBlock}`).first();
      const copyButton = installCodeBlock.locator(SELECTORS.copyButton);

      // Click the copy button
      await copyButton.click();

      // Read clipboard content
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());

      // Should contain the cargo install command
      expect(clipboardContent).toContain('cargo install mirdb');
    });
  });

  test.describe('Usage Example Copy', () => {
    test('clicking copy button on usage example copies code to clipboard', async ({
      page,
      context,
    }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Find the third code block (Connect and Use)
      const usageCodeBlock = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeBlock}`).nth(2);
      const copyButton = usageCodeBlock.locator(SELECTORS.copyButton);

      // Click the copy button
      await copyButton.click();

      // Read clipboard content
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());

      // Should contain SET command
      expect(clipboardContent).toContain('SET');
      // Should contain GET command
      expect(clipboardContent).toContain('GET');
      // Should contain telnet command
      expect(clipboardContent).toContain('telnet');
    });
  });

  test.describe('Copy Feedback', () => {
    test('copy button shows visual feedback on successful copy', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const firstCopyButton = page.locator(`${SELECTORS.quickStart} ${SELECTORS.copyButton}`).first();

      // Verify initial state
      await expect(firstCopyButton).not.toHaveClass(/copied/);

      // Click the copy button
      await firstCopyButton.click();

      // Button should have 'copied' class
      await expect(firstCopyButton).toHaveClass(/copied/);

      // Text should change to "Copied!"
      const copyText = firstCopyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copied!');

      // Check icon displays check icon
      const checkIcon = firstCopyButton.locator('.check-icon');
      await expect(checkIcon).toBeVisible();
    });

    test('copy button resets after feedback duration', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const firstCopyButton = page.locator(`${SELECTORS.quickStart} ${SELECTORS.copyButton}`).first();

      // Click the copy button
      await firstCopyButton.click();

      // Wait for the feedback duration (2 seconds) plus some buffer
      await page.waitForTimeout(2500);

      // Button should no longer have 'copied' class
      await expect(firstCopyButton).not.toHaveClass(/copied/);

      // Text should be back to "Copy"
      const copyText = firstCopyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copy');
    });

    test('multiple copy buttons work independently', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const copyButtons = page.locator(`${SELECTORS.quickStart} ${SELECTORS.copyButton}`);
      const firstButton = copyButtons.first();
      const secondButton = copyButtons.nth(1);

      // Click first button
      await firstButton.click();

      // First button should show "copied"
      await expect(firstButton).toHaveClass(/copied/);

      // Second button should still show "Copy"
      const secondButtonText = secondButton.locator('.copy-text');
      await expect(secondButtonText).toHaveText('Copy');

      // Click second button
      await secondButton.click();

      // Both buttons should now show "copied"
      await expect(secondButton).toHaveClass(/copied/);
    });
  });

  test.describe('Code Block Styling', () => {
    test('code blocks have proper header with language label', async ({ page }) => {
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const firstCodeBlock = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeBlock}`).first();

      // Check for code header
      const header = firstCodeBlock.locator(SELECTORS.codeHeader);
      await expect(header).toBeVisible();

      // Check for language label
      const language = firstCodeBlock.locator(SELECTORS.codeLanguage);
      await expect(language).toBeVisible();
      await expect(language).toHaveText(/bash/i);
    });

    test('code content is selectable and readable', async ({ page }) => {
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const codeContent = page.locator(`${SELECTORS.quickStart} ${SELECTORS.codeContent}`).first();

      // Code content should be visible
      await expect(codeContent).toBeVisible();

      // Should have actual text content
      const text = await codeContent.textContent();
      expect(text).toBeTruthy();
      expect(text!.length).toBeGreaterThan(10);
    });
  });

  test.describe('Keyboard Accessibility', () => {
    test('copy button is focusable with keyboard', async ({ page }) => {
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Tab to the first copy button
      await page.keyboard.press('Tab');

      // Keep tabbing until we reach a copy button
      let attempts = 0;
      const maxAttempts = 30;

      while (attempts < maxAttempts) {
        const focused = await page.evaluate(() => document.activeElement?.classList.contains('copy-button'));
        if (focused) break;
        await page.keyboard.press('Tab');
        attempts++;
      }

      // Verify a copy button is focused
      const focusedElement = page.locator(':focus');
      await expect(focusedElement).toHaveClass(/copy-button/);
    });

    test('copy button can be activated with Enter key', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Find and focus a copy button
      const firstCopyButton = page.locator(`${SELECTORS.quickStart} ${SELECTORS.copyButton}`).first();
      await firstCopyButton.focus();

      // Verify it's focused
      await expect(firstCopyButton).toBeFocused();

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Should show copied state
      await expect(firstCopyButton).toHaveClass(/copied/);
    });
  });
});
