/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section with Installation Instructions
 *
 * Test coverage:
 * - Quick Start section presence
 * - Installation commands verification
 * - Basic usage examples (set/get operations)
 * - Syntax highlighting
 * - Copy-to-clipboard functionality
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://127.0.0.1:1111/');
  });

  test('Quick Start section exists with heading and content', async ({ page }) => {
    // Test case 1: Check Quick Start section exists

    // Verify the section exists with the correct id
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify the section has the h2 heading
    const heading = quickStartSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');

    // Verify the section has content (installation and usage subsections)
    const installationHeading = quickStartSection.locator('h3', { hasText: 'Installation' });
    await expect(installationHeading).toBeVisible();

    const usageHeading = quickStartSection.locator('h3', { hasText: 'Basic Usage' });
    await expect(usageHeading).toBeVisible();
  });

  test('Verify installation command for cargo', async ({ page }) => {
    // Test case 2: Verify cargo install or cargo build command

    const quickStartSection = page.locator('#quick-start');

    // Look for code block containing cargo install
    const installCodeBlock = quickStartSection.locator('#install-cargo');
    await expect(installCodeBlock).toBeVisible();

    const codeContent = await installCodeBlock.textContent();
    expect(codeContent).toBeTruthy();

    // Verify it contains cargo install command
    const hasCargoInstall = codeContent!.includes('cargo install') || codeContent!.includes('cargo build');
    expect(hasCargoInstall).toBeTruthy();
  });

  test('Verify basic usage examples with set/get operations', async ({ page }) => {
    // Test case 3: Verify memcached protocol set/get examples

    const quickStartSection = page.locator('#quick-start');

    // Look for code block containing memcached examples
    const memcachedExample = quickStartSection.locator('#memcached-example');
    await expect(memcachedExample).toBeVisible();

    const codeContent = await memcachedExample.textContent();
    expect(codeContent).toBeTruthy();

    // Verify it contains set command
    expect(codeContent!.toLowerCase()).toContain('set');

    // Verify it contains get command
    expect(codeContent!.toLowerCase()).toContain('get');

    // Verify it shows the memcached protocol response
    expect(codeContent).toContain('STORED');
  });

  test('Verify syntax highlighting is applied', async ({ page }) => {
    // Test case 4: Verify code blocks have syntax highlighting CSS classes

    const quickStartSection = page.locator('#quick-start');

    // Check that code blocks have the language class
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify code blocks have data-language attribute for highlighting
    const firstCodeBlock = codeBlocks.first();
    const language = await firstCodeBlock.getAttribute('data-language');
    expect(language).toBeTruthy();

    // Verify code elements have language class
    const codeElement = firstCodeBlock.locator('code');
    await expect(codeElement).toBeVisible();

    // Check that the code has a class that indicates syntax highlighting
    const classList = await codeElement.getAttribute('class');
    expect(classList).toContain('language-');
  });

  test('Copy-to-clipboard button exists and works', async ({ page, context }) => {
    // Test case 5: Test copy button functionality

    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickStartSection = page.locator('#quick-start');

    // Find copy buttons
    const copyButtons = quickStartSection.locator('.copy-button');
    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    // Get the first copy button
    const firstCopyButton = copyButtons.first();
    await expect(firstCopyButton).toBeVisible();

    // Verify it has an aria-label for accessibility
    const ariaLabel = await firstCopyButton.getAttribute('aria-label');
    expect(ariaLabel).toContain('Copy');

    // Click the copy button
    await firstCopyButton.click();

    // Verify the button shows "Copied!" feedback
    const copiedIcon = firstCopyButton.locator('.copied-icon');
    await expect(copiedIcon).not.toHaveAttribute('hidden');

    // Verify the clipboard contains the code content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // The clipboard should contain some text from the code block
    expect(clipboardContent).toBeTruthy();
    expect(clipboardContent.length).toBeGreaterThan(0);
  });

  test('All code blocks have copy buttons', async ({ page }) => {
    // Additional test: Verify all code blocks have copy functionality

    const quickStartSection = page.locator('#quick-start');
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const copyButton = codeBlock.locator('.copy-button');
      await expect(copyButton).toBeVisible();

      // Verify each button has a data-copy-target
      const targetId = await copyButton.getAttribute('data-copy-target');
      expect(targetId).toBeTruthy();

      // Verify the target element exists
      const targetElement = page.locator(`#${targetId}`);
      await expect(targetElement).toBeVisible();
    }
  });

  test('Navigation to Quick Start section works', async ({ page }) => {
    // Test navigating to Quick Start from header

    // Click the Get Started button in hero section
    const getStartedButton = page.locator('a.cta-button', { hasText: 'Get Started' });
    await expect(getStartedButton).toBeVisible();

    // Verify it links to #quick-start
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBe('#quick-start');

    // Click and verify scroll to section
    await getStartedButton.click();

    // The Quick Start section should be in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });
});
