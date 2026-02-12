/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section with Copy Functionality
 *
 * Test cases:
 * - Quick start section exists
 * - Code blocks with commands are present
 * - Cargo command is shown
 * - Copy buttons are present
 * - Click copy button copies to clipboard
 * - Visual feedback shown after copy
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = `file://${path.resolve(process.cwd(), 'index.html')}`;

test.describe('Quick Start Section with Copy Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  test('Test Case 1: Quick start section element exists', async ({ page }) => {
    // Section with id='quickstart' or 'quick-start' or 'getting-started' exists
    const quickstartSection = page.locator('#quickstart, #quick-start, #getting-started');
    await expect(quickstartSection).toBeVisible();

    // Verify the section has a heading
    const heading = quickstartSection.locator('h2');
    await expect(heading).toContainText(/Quick Start|Getting Started/i);
  });

  test('Test Case 2: Code block elements exist with command examples', async ({ page }) => {
    // Pre or code elements exist containing command examples
    const codeBlocks = page.locator('#quickstart .code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check for pre or code elements
    const codeElements = page.locator('#quickstart code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);
  });

  test('Test Case 3: Cargo command is present', async ({ page }) => {
    // Code block contains 'cargo' command for installation or running
    const quickstartSection = page.locator('#quickstart');
    const text = await quickstartSection.textContent();
    expect(text).toContain('cargo');

    // Specifically check for installation command
    const installCommand = page.locator('#quickstart code', { hasText: 'cargo install' });
    await expect(installCommand).toBeVisible();
  });

  test('Test Case 4: Copy button elements exist near code blocks', async ({ page }) => {
    // Button elements with copy functionality exist near code blocks
    const copyButtons = page.locator('#quickstart .copy-btn');
    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);

    // Each copy button should have data-clipboard attribute
    const firstButton = copyButtons.first();
    const clipboard = await firstButton.getAttribute('data-clipboard');
    expect(clipboard).toBeTruthy();
    expect(clipboard.length).toBeGreaterThan(0);
  });

  test('Test Case 5: Click copy button copies command to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first copy button
    const copyButton = page.locator('#quickstart .copy-btn').first();
    const expectedText = await copyButton.getAttribute('data-clipboard');

    // Click the copy button
    await copyButton.click();

    // Verify clipboard content via the page's clipboard API
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardText).toBe(expectedText);
  });

  test('Test Case 6: Visual feedback indicates successful copy', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first copy button
    const copyButton = page.locator('#quickstart .copy-btn').first();

    // Store original text
    const originalText = await copyButton.textContent();
    expect(originalText).toBe('Copy');

    // Click the copy button
    await copyButton.click();

    // Check that text changed to indicate success
    await expect(copyButton).toHaveText('Copied!');

    // Wait and verify it reverts back
    await page.waitForTimeout(2500);
    await expect(copyButton).toHaveText('Copy');
  });

  test('Copy buttons have accessible labels', async ({ page }) => {
    const copyButtons = page.locator('#quickstart .copy-btn');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      // Should have aria-label for accessibility
      expect(ariaLabel).toBeTruthy();
    }
  });

  test('Quick start section contains installation and usage commands', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const text = await quickstartSection.textContent();

    // Check for installation-related content
    expect(text).toMatch(/install|Installation/i);

    // Check for usage or run commands
    expect(text).toMatch(/run|usage|set|get/i);
  });

  test('Multiple code blocks with different commands', async ({ page }) => {
    const codeBlocks = page.locator('#quickstart .code-block');
    const count = await codeBlocks.count();

    // Should have multiple code blocks for different purposes
    expect(count).toBeGreaterThanOrEqual(2);

    // Verify each has a copy button
    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      const copyBtn = block.locator('.copy-btn');
      await expect(copyBtn).toBeVisible();
    }
  });

  test('Code blocks have proper structure (pre and code elements)', async ({ page }) => {
    const preElements = page.locator('#quickstart pre');
    const preCount = await preElements.count();

    // Each code block should have pre element for proper formatting
    expect(preCount).toBeGreaterThan(0);

    // Each pre should contain code element
    for (let i = 0; i < preCount; i++) {
      const pre = preElements.nth(i);
      const code = pre.locator('code');
      await expect(code).toBeVisible();
    }
  });
});
