import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Code Examples Section
 *
 * This test suite verifies that code examples demonstrate basic operations
 * with existing memcached clients, with syntax highlighting and copy functionality.
 *
 * Requirement: REQ-6 - Provide code examples demonstrating basic operations
 */

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for the code example section to be visible
    await page.waitForSelector('#code-example');
  });

  test('Test Case 1: Check for code example presence showing memcached client usage', async ({ page }) => {
    // Verify the code example section exists
    const codeExampleSection = page.locator('#code-example');
    await expect(codeExampleSection).toBeVisible();

    // Verify section has a heading
    const heading = codeExampleSection.locator('h2');
    await expect(heading).toHaveText('Usage Example');

    // Verify the code block exists
    const codeBlock = page.locator('#example-code');
    await expect(codeBlock).toBeVisible();

    // Verify the code contains memcached client usage
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toBeTruthy();

    // Verify it shows memcached client connection
    expect(codeContent).toContain('pymemcache');
    expect(codeContent).toContain('client');
    expect(codeContent).toContain('localhost');
  });

  test('Test Case 2: Verify syntax highlighting is applied to code blocks', async ({ page }) => {
    // Check that the code element has the language class for Prism.js
    const codeBlock = page.locator('#example-code');
    await expect(codeBlock).toBeVisible();

    // Verify the code block has language-python class
    await expect(codeBlock).toHaveClass(/language-python/);

    // Wait for Prism.js to apply syntax highlighting
    // Prism.js wraps keywords in span elements with token classes
    await page.waitForFunction(() => {
      const code = document.getElementById('example-code');
      return code && code.querySelector('.token') !== null;
    }, { timeout: 5000 });

    // Verify syntax highlighting tokens are present
    const tokens = codeBlock.locator('.token');
    const tokenCount = await tokens.count();
    expect(tokenCount).toBeGreaterThan(0);

    // Verify specific token types exist (keywords, strings, comments, etc.)
    const keywordTokens = codeBlock.locator('.token.keyword');
    const stringTokens = codeBlock.locator('.token.string');
    const commentTokens = codeBlock.locator('.token.comment');

    // At least one of these token types should exist for Python highlighting
    const hasKeywords = await keywordTokens.count() > 0;
    const hasStrings = await stringTokens.count() > 0;
    const hasComments = await commentTokens.count() > 0;

    expect(hasKeywords || hasStrings || hasComments).toBeTruthy();
  });

  test('Test Case 3: Test copy-to-clipboard button functionality', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the copy button
    const copyBtn = page.locator('.copy-btn');
    await expect(copyBtn).toBeVisible();
    await expect(copyBtn).toHaveText('Copy');

    // Get the original code content
    const codeBlock = page.locator('#example-code');
    const originalCode = await codeBlock.textContent();

    // Click the copy button
    await copyBtn.click();

    // Verify button text changes to 'Copied!'
    await expect(copyBtn).toHaveText('Copied!');

    // Read from clipboard and verify content was copied
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardContent).toBe(originalCode);

    // Verify button text reverts back to 'Copy' after timeout
    await expect(copyBtn).toHaveText('Copy', { timeout: 3000 });
  });

  test('Test Case 4: Verify code example demonstrates SET and GET operations', async ({ page }) => {
    // Get the code example content
    const codeBlock = page.locator('#example-code');
    await expect(codeBlock).toBeVisible();

    const codeContent = await codeBlock.textContent();
    expect(codeContent).toBeTruthy();

    // Verify SET operation is demonstrated
    // The Python memcached client uses .set() method
    expect(codeContent).toContain('.set(');
    expect(codeContent).toMatch(/client\.set\s*\(/);

    // Verify GET operation is demonstrated
    // The Python memcached client uses .get() method
    expect(codeContent).toContain('.get(');
    expect(codeContent).toMatch(/client\.get\s*\(/);

    // Verify the example shows storing and retrieving a value
    expect(codeContent).toContain('mykey');
    expect(codeContent).toContain('Hello, MirDB!');
  });

  test('Code example section should have proper visual structure', async ({ page }) => {
    // Verify the code example section has proper structure
    const codeExampleSection = page.locator('#code-example');
    await expect(codeExampleSection).toBeVisible();

    // Verify section has a description
    const description = codeExampleSection.locator('.section-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('memcached client');

    // Verify code block container exists with header
    const codeBlockContainer = codeExampleSection.locator('.code-block');
    await expect(codeBlockContainer).toBeVisible();

    // Verify code header with language indicator
    const codeHeader = codeExampleSection.locator('.code-header');
    await expect(codeHeader).toBeVisible();

    // Verify language indicator shows Python
    const languageIndicator = codeHeader.locator('.code-language');
    await expect(languageIndicator).toHaveText('Python');
  });

  test('Code block should be readable with proper styling', async ({ page }) => {
    const codeBlock = page.locator('#example-code');
    await expect(codeBlock).toBeVisible();

    // Verify the code block has a dark background (code-bg color)
    const preElement = page.locator('#code-example pre');
    await expect(preElement).toBeVisible();

    // Check the code uses monospace font family
    const fontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Font family should include monospace or a known monospace font
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|courier|sf mono|fira code|inconsolata/i);
  });
});
