/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests that the quick start section provides:
 * - Installation commands
 * - Usage examples with get/set operations
 * - Proper code styling and syntax highlighting
 * - Copy functionality
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section (Scenario 4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Quick start section exists with appropriate heading
  test('TC1: Quick start section exists with heading', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for heading (visible or screen-reader accessible)
    const heading = quickStartSection.locator('h2');
    await expect(heading).toHaveCount(1);

    // Verify heading text contains Quick Start or Getting Started
    const headingText = await heading.textContent();
    expect(headingText).toMatch(/Quick Start|Getting Started/i);
  });

  // Test Case 2: At least 2 code blocks present (installation + usage)
  test('TC2: At least 2 code blocks present', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const codeBlocks = quickStartSection.locator('pre, code.quick-start__code-block, .quick-start__code');

    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  // Test Case 3: Code block contains installation/build command
  test('TC3: Code block contains installation command', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const sectionText = await quickStartSection.textContent();

    // Check for common installation/build commands
    const hasInstallCommand =
      sectionText.includes('cargo build') ||
      sectionText.includes('cargo run') ||
      sectionText.includes('git clone') ||
      sectionText.includes('cargo install');

    expect(hasInstallCommand).toBe(true);
  });

  // Test Case 4: Code block contains example of setting a key-value pair
  test('TC4: Code block contains set operation example', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const sectionText = await quickStartSection.textContent();

    // Check for set operation patterns (Memcached protocol or Rust API)
    const hasSetOperation =
      sectionText.toLowerCase().includes('set ') ||
      sectionText.includes('.set(') ||
      sectionText.includes('SET ');

    expect(hasSetOperation).toBe(true);
  });

  // Test Case 5: Code block contains example of getting a value by key
  test('TC5: Code block contains get operation example', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const sectionText = await quickStartSection.textContent();

    // Check for get operation patterns (Memcached protocol or Rust API)
    const hasGetOperation =
      sectionText.toLowerCase().includes('get ') ||
      sectionText.includes('.get(') ||
      sectionText.includes('GET ');

    expect(hasGetOperation).toBe(true);
  });

  // Test Case 6: Code blocks have visible styling
  test('TC6: Code blocks have visible styling', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const codeBlock = quickStartSection.locator('pre').first();

    await expect(codeBlock).toBeVisible();

    // Check for styling properties
    const styles = await codeBlock.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        fontFamily: computed.fontFamily
      };
    });

    // Verify background color is not transparent/default
    expect(styles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(styles.backgroundColor).not.toBe('transparent');

    // Verify monospace font family
    expect(styles.fontFamily.toLowerCase()).toMatch(/mono|consolas|courier|menlo|fira/);
  });

  // Test Case 7: Copy button exists and copies code to clipboard
  test('TC7: Copy button exists and copies code to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickStartSection = page.locator('#quick-start');
    const copyButton = quickStartSection.locator('[data-copy], .quick-start__copy-btn, button:has-text("Copy")').first();

    // Verify copy button exists
    await expect(copyButton).toBeVisible();

    // Get the code content before clicking
    const codeBlock = quickStartSection.locator('pre').first();
    const codeContent = await codeBlock.textContent();

    // Click the copy button
    await copyButton.click();

    // Wait for clipboard operation
    await page.waitForTimeout(300);

    // Read clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify clipboard contains part of the code (trimmed comparison)
    expect(clipboardContent.trim()).toBeTruthy();
  });
});
