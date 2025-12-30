const { test, expect } = require('@playwright/test');

/**
 * Quick Start Guide Section Tests
 * Scenario: Validate the quick start guide provides clear installation and usage instructions
 */

test.describe('Quick Start Guide Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Quick start section contains code block elements with installation commands', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart, [id*="quickstart"], [id*="quick-start"], .quickstart, [class*="quickstart"], [class*="quick-start"]');
    await expect(quickstartSection.first()).toBeVisible();

    // Check that code block elements exist within the section
    const codeBlocks = quickstartSection.locator('pre, code, [class*="code-block"], .code');
    const codeBlockCount = await codeBlocks.count();

    // Should have at least one code block
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify at least one code block is visible
    await expect(codeBlocks.first()).toBeVisible();
  });

  test('Test Case 2: Page displays SET command example in code format', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart, [id*="quickstart"], [id*="quick-start"], .quickstart, [class*="quickstart"], [class*="quick-start"]');
    await expect(quickstartSection.first()).toBeVisible();

    // Check for SET command in code blocks
    const codeBlocks = quickstartSection.locator('pre, code, [class*="code-block"], .code');

    // Get all code block text content
    let foundSetCommand = false;
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && text.toLowerCase().includes('set ')) {
        foundSetCommand = true;
        break;
      }
    }

    expect(foundSetCommand).toBe(true);
  });

  test('Test Case 3: Page displays GET command example in code format', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart, [id*="quickstart"], [id*="quick-start"], .quickstart, [class*="quickstart"], [class*="quick-start"]');
    await expect(quickstartSection.first()).toBeVisible();

    // Check for GET command in code blocks
    const codeBlocks = quickstartSection.locator('pre, code, [class*="code-block"], .code');

    // Get all code block text content
    let foundGetCommand = false;
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && text.toLowerCase().includes('get ')) {
        foundGetCommand = true;
        break;
      }
    }

    expect(foundGetCommand).toBe(true);
  });

  test('Test Case 4: Page displays DELETE command example in code format', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart, [id*="quickstart"], [id*="quick-start"], .quickstart, [class*="quickstart"], [class*="quick-start"]');
    await expect(quickstartSection.first()).toBeVisible();

    // Check for DELETE command in code blocks
    const codeBlocks = quickstartSection.locator('pre, code, [class*="code-block"], .code');

    // Get all code block text content
    let foundDeleteCommand = false;
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && text.toLowerCase().includes('delete ')) {
        foundDeleteCommand = true;
        break;
      }
    }

    expect(foundDeleteCommand).toBe(true);
  });
});
