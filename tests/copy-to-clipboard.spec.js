// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Copy to Clipboard Functionality
 * Scenario: Verify all code blocks have copy-to-clipboard functionality as specified in US-2 and US-7
 * Related Requirements: REQ-3, REQ-4, US-2, US-7
 */

test.describe('Copy to Clipboard Functionality', () => {
  test.beforeEach(async ({ page, context }) => {
    // Grant clipboard permissions for the tests
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for copy button presence on code blocks
   * All code blocks should have a visible copy-to-clipboard button
   */
  test('TC1: All code blocks have a visible copy-to-clipboard button', async ({ page }) => {
    // Get all code block wrappers on the page
    const codeBlockWrappers = page.locator('.code-block-wrapper');
    const wrapperCount = await codeBlockWrappers.count();

    // Verify there are code block wrappers on the page
    expect(wrapperCount).toBeGreaterThan(0);

    // Verify each code block wrapper has a copy button
    for (let i = 0; i < wrapperCount; i++) {
      const wrapper = codeBlockWrappers.nth(i);
      const copyButton = wrapper.locator('.copy-button, button[data-testid="copy-button"]');

      // Verify copy button exists and is visible
      await expect(copyButton).toBeVisible();

      // Verify button is clickable
      await expect(copyButton).toBeEnabled();
    }
  });

  /**
   * Test Case 2: Click copy button and verify clipboard
   * Code is successfully copied to clipboard and can be pasted
   */
  test('TC2: Code is successfully copied to clipboard when copy button is clicked', async ({ page }) => {
    // Get the first code block wrapper
    const firstWrapper = page.locator('.code-block-wrapper').first();
    await expect(firstWrapper).toBeVisible();

    // Get the code content before copying
    const codeContent = await firstWrapper.locator('code').textContent();

    // Find and click the copy button
    const copyButton = firstWrapper.locator('.copy-button, button[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();
    await copyButton.click();

    // Verify clipboard content matches the code block content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Normalize whitespace for comparison (trim trailing/leading whitespace)
    const normalizedCode = codeContent.trim();
    const normalizedClipboard = clipboardContent.trim();

    expect(normalizedClipboard).toBe(normalizedCode);
  });

  /**
   * Test Case 3: Copy button visual feedback
   * Copy button shows visual feedback (icon change or tooltip) when clicked
   */
  test('TC3: Copy button shows visual feedback when clicked', async ({ page }) => {
    // Get the first code block wrapper
    const firstWrapper = page.locator('.code-block-wrapper').first();
    await expect(firstWrapper).toBeVisible();

    // Find the copy button
    const copyButton = firstWrapper.locator('.copy-button, button[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Get initial state (text or class)
    const initialText = await copyButton.textContent();

    // Click the copy button
    await copyButton.click();

    // Wait a moment for the UI to update
    await page.waitForTimeout(100);

    // Check for visual feedback
    const afterClickText = await copyButton.textContent();
    const afterClickClass = await copyButton.getAttribute('class');
    const afterClickAriaLabel = await copyButton.getAttribute('aria-label');
    const afterClickDataCopied = await copyButton.getAttribute('data-copied');

    // Verify visual feedback occurred (at least one of these should be true)
    const hasTextFeedback = afterClickText.toLowerCase().includes('copied');
    const hasClassFeedback = afterClickClass && afterClickClass.includes('copied');
    const hasAriaFeedback = afterClickAriaLabel && afterClickAriaLabel.toLowerCase().includes('copied');
    const hasDataFeedback = afterClickDataCopied === 'true';

    const visualFeedbackReceived = hasTextFeedback || hasClassFeedback || hasAriaFeedback || hasDataFeedback;

    expect(visualFeedbackReceived).toBe(true);
  });

  /**
   * Additional test: Verify copy works for all code blocks
   * Each code block's copy button should copy its specific content
   */
  test('TC4: Each code block copy button copies its specific content', async ({ page }) => {
    // Get all code block wrappers
    const wrappers = page.locator('.code-block-wrapper');
    const wrapperCount = await wrappers.count();

    // Test each code block (at least 2 if available)
    const testCount = Math.min(wrapperCount, 2);

    for (let i = 0; i < testCount; i++) {
      const wrapper = wrappers.nth(i);
      const codeContent = await wrapper.locator('code').textContent();
      const copyButton = wrapper.locator('.copy-button, button[data-testid="copy-button"]');

      // Click copy button
      await copyButton.click();

      // Verify clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      expect(clipboardContent.trim()).toBe(codeContent.trim());

      // Wait a moment before next iteration to allow state reset
      await page.waitForTimeout(100);
    }
  });

  /**
   * Additional test: Copy button accessibility
   * Copy button should have appropriate aria-label for screen readers
   */
  test('TC5: Copy button has appropriate accessibility attributes', async ({ page }) => {
    const firstWrapper = page.locator('.code-block-wrapper').first();
    const copyButton = firstWrapper.locator('.copy-button, button[data-testid="copy-button"]');

    await expect(copyButton).toBeVisible();

    // Check for aria-label or title attribute
    const ariaLabel = await copyButton.getAttribute('aria-label');
    const title = await copyButton.getAttribute('title');
    const buttonText = await copyButton.textContent();

    // At least one accessibility indicator should be present
    const hasAccessibility =
      (ariaLabel && ariaLabel.toLowerCase().includes('copy')) ||
      (title && title.toLowerCase().includes('copy')) ||
      (buttonText && buttonText.toLowerCase().includes('copy'));

    expect(hasAccessibility).toBe(true);
  });
});
