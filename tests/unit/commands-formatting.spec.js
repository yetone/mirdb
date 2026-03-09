/**
 * Commands Section Formatting Unit Tests
 * Owner: Scenario 5 - Supported Commands Section
 *
 * Tests:
 * - Commands displayed in monospace font with pre/code elements
 * - Code blocks have proper CSS styling
 */

const { test, expect } = require('@playwright/test');

test.describe('Commands Section Code Formatting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 5: Verify command examples use proper code formatting
  test('commands displayed in monospace font with pre/code elements', async ({ page }) => {
    const commandsSection = page.locator('#commands');

    // Verify pre elements exist in commands section
    const preElements = commandsSection.locator('pre');
    await expect(preElements.first()).toBeVisible();

    // Verify code elements exist within pre elements
    const codeElements = commandsSection.locator('pre code');
    await expect(codeElements.first()).toBeVisible();

    // Check font-family for code elements - should be monospace
    const codeElement = codeElements.first();
    const fontFamily = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Verify monospace font is applied (checking for common monospace font names)
    const isMonospace = fontFamily.toLowerCase().includes('monospace') ||
                        fontFamily.includes('SFMono') ||
                        fontFamily.includes('Consolas') ||
                        fontFamily.includes('Liberation Mono') ||
                        fontFamily.includes('Menlo') ||
                        fontFamily.includes('Courier');
    expect(isMonospace).toBe(true);
  });

  // Verify pre elements have proper background styling
  test('code blocks have proper background styling', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const preElement = commandsSection.locator('pre').first();

    // Check background color
    const bgColor = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Background should be a dark color (not transparent or white)
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bgColor).not.toBe('rgb(255, 255, 255)');
  });

  // Verify pre elements have border-radius
  test('code blocks have rounded corners', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const preElement = commandsSection.locator('pre').first();

    // Check border-radius
    const borderRadius = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });

    // Border radius should be set (not 0)
    expect(borderRadius).not.toBe('0px');
  });

  // Verify code blocks handle overflow properly
  test('code blocks handle overflow with scroll', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const preElement = commandsSection.locator('pre').first();

    // Check overflow-x
    const overflowX = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    // Should have auto scroll for long code
    expect(overflowX).toBe('auto');
  });

  // Verify inline code elements have proper styling
  test('inline code elements in response section have proper styling', async ({ page }) => {
    const responseCodes = page.locator('.response-codes');
    const codeElement = responseCodes.locator('code').first();

    // Check font-family for inline code
    const fontFamily = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Verify monospace font is applied
    const isMonospace = fontFamily.toLowerCase().includes('monospace') ||
                        fontFamily.includes('SFMono') ||
                        fontFamily.includes('Consolas') ||
                        fontFamily.includes('Liberation Mono') ||
                        fontFamily.includes('Menlo') ||
                        fontFamily.includes('Courier');
    expect(isMonospace).toBe(true);
  });

  // Verify all command items have code blocks
  test('each command item contains code syntax block', async ({ page }) => {
    const commandItems = page.locator('.command-item');
    const count = await commandItems.count();

    // Each command should have at least one code block
    for (let i = 0; i < count; i++) {
      const commandItem = commandItems.nth(i);
      const codeBlocks = commandItem.locator('pre code');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);
    }
  });
});
