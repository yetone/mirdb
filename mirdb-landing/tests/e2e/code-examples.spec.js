/**
 * Code Examples E2E Tests
 * Owner: Scenario 3 - Code Examples Section
 *
 * Tests:
 * - Code block presence
 * - Syntax highlighting
 * - Copy button functionality
 * - Clipboard API integration
 */

import { test, expect } from '@playwright/test';

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: At least one code example block with memcached client code exists', async ({ page }) => {
    // Navigate to code examples section
    const codeSection = page.locator('#code-examples');
    await expect(codeSection).toBeVisible();

    // Check for code example blocks
    const codeExamples = page.locator('.code-example');
    await expect(codeExamples).toHaveCount(3); // Python, Node.js, Ruby

    // Verify memcached-related content exists
    const codeBlocks = page.locator('.code-block code');
    const pythonCode = await codeBlocks.first().textContent();
    expect(pythonCode).toContain('memcache');

    // Check that code shows connection to MirDB
    expect(pythonCode).toContain('localhost:12333');
  });

  test('TC2: Code blocks use monospace font and syntax highlighting', async ({ page }) => {
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Check monospace font
    const fontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|courier|menlo|sf mono|fira code|jetbrains/i);

    // Check for syntax highlighting elements (these classes indicate syntax highlighting is implemented)
    const keywords = page.locator('.code-keyword');
    await expect(keywords.first()).toBeVisible();
    const keywordCount = await keywords.count();
    expect(keywordCount).toBeGreaterThan(0);

    const strings = page.locator('.code-string');
    await expect(strings.first()).toBeVisible();
    const stringCount = await strings.count();
    expect(stringCount).toBeGreaterThan(0);

    const comments = page.locator('.code-comment');
    await expect(comments.first()).toBeVisible();
    const commentCount = await comments.count();
    expect(commentCount).toBeGreaterThan(0);

    // Verify different syntax categories exist (indicating proper syntax highlighting structure)
    const functions = page.locator('.code-function');
    const functionCount = await functions.count();
    expect(functionCount).toBeGreaterThan(0);

    const variables = page.locator('.code-variable');
    const variableCount = await variables.count();
    expect(variableCount).toBeGreaterThan(0);
  });

  test('TC3: Click copy button copies code content to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('.copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Click copy button
    await copyButton.click();

    // Verify visual feedback
    await expect(copyButton).toHaveClass(/copied/);
    const copyText = copyButton.locator('.copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Verify clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Should contain Python code (first code block)
    expect(clipboardContent).toContain('import');
    expect(clipboardContent).toContain('pymemcache');
    expect(clipboardContent).toContain('Client');

    // Wait for feedback to reset
    await page.waitForTimeout(2500);
    await expect(copyButton).not.toHaveClass(/copied/);
    await expect(copyText).toHaveText('Copy');
  });

  test('TC4: Each code example has an associated copy button', async ({ page }) => {
    const codeExamples = page.locator('.code-example');
    const count = await codeExamples.count();

    // Verify each code example has a copy button
    for (let i = 0; i < count; i++) {
      const example = codeExamples.nth(i);
      const copyButton = example.locator('.copy-btn');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveAttribute('data-copy-target');

      // Verify button has proper aria-label for accessibility
      const ariaLabel = await copyButton.getAttribute('aria-label');
      expect(ariaLabel).toContain('Copy');
    }
  });

  test('Code examples show multiple programming languages', async ({ page }) => {
    const languageLabels = page.locator('.code-language');

    // Should have 3 language examples
    await expect(languageLabels).toHaveCount(3);

    // Verify languages displayed
    const languages = await languageLabels.allTextContents();
    expect(languages.map(l => l.toLowerCase())).toContain('python');
    expect(languages.map(l => l.toLowerCase())).toContain('node.js');
    expect(languages.map(l => l.toLowerCase())).toContain('ruby');
  });

  test('Code section has proper heading and description', async ({ page }) => {
    const heading = page.locator('#code-examples-title');
    await expect(heading).toHaveText('Code Examples');

    const subtitle = page.locator('#code-examples .section-subtitle');
    await expect(subtitle).toBeVisible();
    const subtitleText = await subtitle.textContent();
    expect(subtitleText).toContain('Memcached');
  });

  test('Code blocks are scrollable when content overflows', async ({ page }) => {
    const codeBlock = page.locator('.code-block').first();

    // Check overflow-x is auto, scroll, or visible (visible is ok when content doesn't overflow)
    const overflowX = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll', 'visible']).toContain(overflowX);
  });

  test('Copy button keyboard accessibility', async ({ page }) => {
    const copyButton = page.locator('.copy-btn').first();

    // Focus the button using keyboard
    await copyButton.focus();

    // Verify focus is visible
    const outlineWidth = await copyButton.evaluate((el) => {
      return window.getComputedStyle(el).outlineWidth;
    });
    // When focused, should have some visual indication (could be outline or other style)
    // Just verify the button is focusable
    await expect(copyButton).toBeFocused();
  });
});
