// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

test.describe('Quick Start Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  test('Test Case 1: Quick start section exists with at least 2 code example blocks', async ({ page }) => {
    // Locate quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for code blocks within quick start section
    const codeBlocks = quickStartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    // Should have at least 2 code example blocks
    expect(codeBlockCount).toBeGreaterThanOrEqual(2);
  });

  test('Test Case 2: Code block contains mirdb command or equivalent startup instruction', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for server startup code block
    const codeBlocks = quickStartSection.locator('.code-block');
    const allCodeText = await codeBlocks.allTextContents();

    // Check if any code block contains 'mirdb' command
    const hasMirdbCommand = allCodeText.some(text => text.includes('mirdb'));
    expect(hasMirdbCommand).toBe(true);
  });

  test('Test Case 3: Code block demonstrates SET command usage with expected STORED response', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for SET operation in code blocks
    const codeBlocks = quickStartSection.locator('.code-block');
    const allCodeText = await codeBlocks.allTextContents();

    // Check if any code block contains SET command and STORED response
    const hasSetCommand = allCodeText.some(text =>
      text.toLowerCase().includes('set') && text.includes('STORED')
    );
    expect(hasSetCommand).toBe(true);
  });

  test('Test Case 4: Code block demonstrates GET command usage with VALUE response', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for GET operation in code blocks
    const codeBlocks = quickStartSection.locator('.code-block');
    const allCodeText = await codeBlocks.allTextContents();

    // Check if any code block contains GET command and VALUE response
    const hasGetCommand = allCodeText.some(text =>
      text.toLowerCase().includes('get') && text.includes('VALUE')
    );
    expect(hasGetCommand).toBe(true);
  });

  test('Test Case 5: Code blocks have syntax highlighting applied via CSS classes or inline styles', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for syntax highlighting classes within code blocks
    const highlightedElements = quickStartSection.locator('.code-block [class^="code-"]');
    const highlightCount = await highlightedElements.count();

    // Should have syntax highlighting elements (spans with code-* classes)
    expect(highlightCount).toBeGreaterThan(0);

    // Verify specific highlighting classes exist
    const codeKeyword = quickStartSection.locator('.code-keyword');
    const codeComment = quickStartSection.locator('.code-comment');
    const codeString = quickStartSection.locator('.code-string');
    const codeResponse = quickStartSection.locator('.code-response');

    // At least some highlighting classes should be present
    const keywordCount = await codeKeyword.count();
    const commentCount = await codeComment.count();
    const stringCount = await codeString.count();
    const responseCount = await codeResponse.count();

    const totalHighlightedElements = keywordCount + commentCount + stringCount + responseCount;
    expect(totalHighlightedElements).toBeGreaterThan(0);

    // Verify that highlighted elements have actual styles applied
    if (keywordCount > 0) {
      const firstKeyword = codeKeyword.first();
      const color = await firstKeyword.evaluate(el => getComputedStyle(el).color);
      // Color should not be default black/inherit - should have a distinct color
      expect(color).not.toBe('rgb(0, 0, 0)');
    }
  });
});
