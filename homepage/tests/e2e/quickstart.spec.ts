/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('section with heading containing Quick Start exists', async ({ page }) => {
    // Test Case 1: Query for Quick Start section heading
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const heading = quickStartSection.getByRole('heading', { name: /quick start/i });
    await expect(heading).toBeVisible();
  });

  test('code block containing cargo installation command is displayed', async ({ page }) => {
    // Test Case 2: Query for code block with installation command
    const quickStartSection = page.locator('#quick-start');

    // Find code blocks with cargo command
    const codeBlocks = quickStartSection.locator('[data-testid="code-block"]');
    await expect(codeBlocks.first()).toBeVisible();

    // Check for cargo build command
    const installationCode = quickStartSection.locator('code');
    const codeText = await installationCode.allTextContents();
    const hasCargoCommand = codeText.some(text => text.includes('cargo'));
    expect(hasCargoCommand).toBe(true);
  });

  test('code block showing set and get operations is displayed', async ({ page }) => {
    // Test Case 3: Query for usage example code block
    const quickStartSection = page.locator('#quick-start');

    // Check for set operation
    const setCommand = quickStartSection.locator('code', { hasText: 'set' });
    await expect(setCommand.first()).toBeVisible();

    // Check for get operation
    const getCommand = quickStartSection.locator('code', { hasText: 'get' });
    await expect(getCommand.first()).toBeVisible();
  });

  test('code blocks are displayed in monospace font with proper formatting', async ({ page }) => {
    // Test Case 4: Verify code blocks have syntax highlighting or monospace formatting
    const quickStartSection = page.locator('#quick-start');
    const codeBlocks = quickStartSection.locator('[data-testid="code-block"]');

    // Verify at least one code block exists
    await expect(codeBlocks.first()).toBeVisible();

    // Check that pre element exists (proper code formatting)
    const preElement = quickStartSection.locator('pre').first();
    await expect(preElement).toBeVisible();
    await expect(preElement).toHaveClass(/code-block-pre/);

    // Check that code element has monospace font class
    const codeElement = quickStartSection.locator('code').first();
    await expect(codeElement).toHaveClass(/code-block-code/);

    // Verify font-family is monospace
    const fontFamily = await codeElement.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|courier|menlo|fira/);
  });

  test('installation section has title', async ({ page }) => {
    const installationTitle = page.getByRole('heading', { name: /installation/i });
    await expect(installationTitle).toBeVisible();
  });

  test('usage section has title', async ({ page }) => {
    const usageTitle = page.getByRole('heading', { name: /usage/i });
    await expect(usageTitle).toBeVisible();
  });

  test('quick start section is accessible with proper ARIA attributes', async ({ page }) => {
    const section = page.locator('section#quick-start');
    await expect(section).toHaveAttribute('aria-labelledby', 'quick-start-heading');

    const heading = page.locator('#quick-start-heading');
    await expect(heading).toBeVisible();
  });
});
