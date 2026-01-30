/**
 * Getting Started E2E Tests.
 * Owner: Scenario 5 - Getting Started Section
 *
 * Tests:
 * - Code blocks are copyable
 * - Copy button functionality works
 */

import { test, expect } from '@playwright/test';

test.describe('Getting Started Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the Getting Started section to be rendered
    await page.waitForSelector('[data-testid="getting-started-section"]');
  });

  test('should display Getting Started section', async ({ page }) => {
    const section = page.locator('[data-testid="getting-started-section"]');
    await expect(section).toBeVisible();
  });

  test('should display installation options', async ({ page }) => {
    const cargoOption = page.locator('[data-testid="install-option-cargo"]');
    const sourceOption = page.locator('[data-testid="install-option-source"]');

    await expect(cargoOption).toBeVisible();
    await expect(sourceOption).toBeVisible();
  });

  test('should have selectable code blocks', async ({ page }) => {
    const codeContent = page.locator('[data-testid="code-content"]').first();
    await expect(codeContent).toBeVisible();

    // Verify the code block contains text that can be selected
    const text = await codeContent.textContent();
    expect(text).toBeTruthy();
    expect(text?.length).toBeGreaterThan(0);
  });

  test('code blocks should be copyable via selection', async ({ page }) => {
    const codeContent = page.locator('[data-testid="code-content"]').first();
    await expect(codeContent).toBeVisible();

    // Triple-click to select all text in the code block
    await codeContent.click({ clickCount: 3 });

    // Check that there's selected text
    const selectedText = await page.evaluate(() => window.getSelection()?.toString());
    expect(selectedText).toBeTruthy();
    expect(selectedText?.trim().length).toBeGreaterThan(0);
  });

  test('copy button should be visible on hover', async ({ page }) => {
    const codeBlockContainer = page.locator('.code-block-container').first();
    const copyButton = codeBlockContainer.locator('[data-testid="copy-button"]');

    // Hover over the code block container
    await codeBlockContainer.hover();

    // Copy button should be visible
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveText('Copy');
  });

  test('copy button should copy code to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const codeBlockContainer = page.locator('.code-block-container').first();
    const copyButton = codeBlockContainer.locator('[data-testid="copy-button"]');
    const codeContent = codeBlockContainer.locator('[data-testid="code-content"]');

    // Get expected code text
    const expectedCode = await codeContent.textContent();

    // Click copy button
    await codeBlockContainer.hover();
    await copyButton.click();

    // Button text should change to "Copied!"
    await expect(copyButton).toHaveText('Copied!');

    // Verify clipboard contents
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedCode);

    // Button text should revert after timeout
    await expect(copyButton).toHaveText('Copy', { timeout: 3000 });
  });

  test('should display default configuration values', async ({ page }) => {
    const portConfig = page.locator('[data-testid="config-port"]');
    const workDirConfig = page.locator('[data-testid="config-workdir"]');

    await expect(portConfig).toBeVisible();
    await expect(portConfig).toContainText('12333');

    await expect(workDirConfig).toBeVisible();
    await expect(workDirConfig).toContainText('/tmp/mirdb');
  });

  test('should display quick start section', async ({ page }) => {
    const quickStart = page.locator('[data-testid="quick-start"]');
    await expect(quickStart).toBeVisible();

    const quickStartCode = page.locator('[data-testid="code-block-quick-start"]');
    await expect(quickStartCode).toBeVisible();
  });
});
