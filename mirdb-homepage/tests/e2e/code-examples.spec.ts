/**
 * Code Examples E2E Tests.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 *
 * Tests:
 * - Code block presence
 * - Syntax highlighting
 * - Copy to clipboard
 * - Usage GIF display
 */

import { test, expect } from '@playwright/test';

test.describe('Usage Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display code block with Memcached commands (SET, GET)', async ({ page }) => {
    // Navigate to usage section
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check for code block presence
    const codeBlock = usageSection.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Verify code content contains SET, GET, DELETE commands
    const codeText = await codeBlock.textContent();
    expect(codeText).toContain('SET');
    expect(codeText).toContain('GET');
    expect(codeText).toContain('DELETE');
  });

  test('should have syntax highlighting classes applied', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check for code element with language class (Prism.js applies language-* classes)
    const codeBlock = usageSection.locator('code[class*="language-"]');
    await expect(codeBlock).toBeVisible();

    // Verify syntax highlighting is applied (Prism adds token classes)
    const codeElement = await codeBlock.evaluate((el) => {
      return el.className;
    });
    expect(codeElement).toMatch(/language-/);
  });

  test('should copy code to clipboard when copy button is clicked', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Find and click the copy button
    const copyButton = usageSection.locator('.copy-button').first();
    await expect(copyButton).toBeVisible();
    await copyButton.click();

    // Verify copy button shows success state
    await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');

    // Read clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('SET');
    expect(clipboardContent).toContain('GET');
  });

  test('should display usage GIF image', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Find the usage GIF
    const usageGif = usageSection.locator('img[data-testid="usage-gif"]');
    await expect(usageGif).toBeVisible();

    // Verify GIF source
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');

    // Verify GIF loads successfully (naturalWidth > 0)
    const isLoaded = await usageGif.evaluate((img: HTMLImageElement) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('should have filename displayed in code block header', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check for filename in code block header
    const filename = usageSection.locator('.code-block-header');
    await expect(filename).toBeVisible();
    await expect(filename).toContainText('memcached-commands.txt');
  });

  test('should have proper accessibility attributes on copy button', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    const copyButton = usageSection.locator('.copy-button').first();
    await expect(copyButton).toBeVisible();

    // Check for aria-label
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');
  });
});
