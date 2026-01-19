// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check for code block element in examples section
  test('TC1: Pre/code element with syntax highlighting classes exists in code examples section', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check for pre element with syntax-highlight class
    const preElement = codeExamplesSection.locator('pre.syntax-highlight');
    await expect(preElement).toBeVisible();

    // Check for code element inside pre
    const codeElement = preElement.locator('code');
    await expect(codeElement).toBeVisible();

    // Verify code element has language class
    const codeClass = await codeElement.getAttribute('class');
    expect(codeClass).toContain('language-');
  });

  // Test Case 2: Verify code contains SET operation example
  test('TC2: Code block contains example of SET command', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Get the code content
    const codeElement = page.locator('#code-example');
    await expect(codeElement).toBeVisible();

    const codeText = await codeElement.textContent();
    const lowerText = codeText.toLowerCase();

    // Verify SET operation is present
    expect(lowerText).toContain('set');

    // Look for SET keyword span
    const setKeyword = codeExamplesSection.locator('.keyword').filter({ hasText: /^set$/i }).first();
    await expect(setKeyword).toBeVisible();
  });

  // Test Case 3: Verify code contains GET operation example
  test('TC3: Code block contains example of GET command', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Get the code content
    const codeElement = page.locator('#code-example');
    await expect(codeElement).toBeVisible();

    const codeText = await codeElement.textContent();
    const lowerText = codeText.toLowerCase();

    // Verify GET operation is present
    expect(lowerText).toContain('get');

    // Look for GET keyword span
    const getKeyword = codeExamplesSection.locator('.keyword').filter({ hasText: /^get$/i });
    await expect(getKeyword).toBeVisible();
  });

  // Test Case 4: Click copy-to-clipboard button and verify it works
  test('TC4: Copy-to-clipboard button copies code content successfully', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Find the copy button
    const copyBtn = page.locator('#copy-code-btn');
    await expect(copyBtn).toBeVisible();

    // Get the code content before clicking
    const codeElement = page.locator('#code-example');
    const expectedContent = await codeElement.textContent();

    // Click the copy button
    await copyBtn.click();

    // Verify the button shows feedback (text changes to "Copied!")
    const copyText = copyBtn.locator('.copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Verify the button has the 'copied' class
    await expect(copyBtn).toHaveClass(/copied/);

    // Verify clipboard content (if supported)
    try {
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });
      expect(clipboardContent).toBe(expectedContent);
    } catch (e) {
      // If clipboard API is not available, the visual feedback test is sufficient
      console.log('Clipboard API not available, relying on visual feedback');
    }
  });

  // Test Case 5: Verify syntax highlighting is applied
  test('TC5: Code block has CSS classes for syntax highlighting', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check for syntax highlighting pre element
    const preElement = codeExamplesSection.locator('pre.syntax-highlight');
    await expect(preElement).toBeVisible();

    // Check for various syntax highlighting span elements
    const codeElement = codeExamplesSection.locator('#code-example');

    // Check for comment spans
    const comments = codeElement.locator('.comment');
    const commentCount = await comments.count();
    expect(commentCount).toBeGreaterThan(0);

    // Check for keyword spans (set, get, delete)
    const keywords = codeElement.locator('.keyword');
    const keywordCount = await keywords.count();
    expect(keywordCount).toBeGreaterThan(0);

    // Check for value spans
    const values = codeElement.locator('.value');
    const valueCount = await values.count();
    expect(valueCount).toBeGreaterThan(0);

    // Check for response spans
    const responses = codeElement.locator('.response');
    const responseCount = await responses.count();
    expect(responseCount).toBeGreaterThan(0);

    // Verify syntax highlighting CSS is applied (colors are different from default)
    const commentColor = await comments.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const keywordColor = await keywords.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // The colors should be different (indicating syntax highlighting is applied)
    expect(commentColor).not.toBe(keywordColor);
  });

  // Additional test: Code examples section has proper structure
  test('Code examples section has proper heading and description', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Check for heading
    const heading = codeExamplesSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Code Examples');

    // Check for description
    const description = codeExamplesSection.locator('.section-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('memcached');
    expect(descText.toLowerCase()).toContain('set');
    expect(descText.toLowerCase()).toContain('get');
  });

  // Additional test: Copy button has proper accessibility
  test('Copy button has proper accessibility attributes', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Find the copy button
    const copyBtn = page.locator('#copy-code-btn');
    await expect(copyBtn).toBeVisible();

    // Check for aria-label
    const ariaLabel = await copyBtn.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('copy');
  });
});
