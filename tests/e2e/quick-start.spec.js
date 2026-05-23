/**
 * E2E tests for Quick Start Section.
 * Owner: Scenario 5 - Quick Start Section
 *
 * Tests:
 * - Quick Start section exists with h2 heading
 * - Code blocks are present with usage examples
 * - Set operation is demonstrated
 * - Get operation is demonstrated
 * - Copy buttons are present on each code block
 * - Copy button copies exact code content to clipboard
 * - Syntax highlighting CSS classes are applied
 */

const { test, expect } = require('@playwright/test');

const filePath = 'file://' + process.cwd() + '/index.html';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(filePath);
  });

  test('Quick Start section exists with h2 heading "Quick Start"', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const heading = quickStartSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');
  });

  test('At least one code block exists with usage examples', async ({ page }) => {
    const codeBlocks = page.locator('#quick-start pre.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('Set operation is demonstrated in a code block', async ({ page }) => {
    const quickStart = page.locator('#quick-start');
    const text = await quickStart.textContent();
    const hasSet = text.toLowerCase().includes('set ') || text.toLowerCase().includes('set\n');
    expect(hasSet).toBe(true);
  });

  test('Get operation is demonstrated in a code block', async ({ page }) => {
    const quickStart = page.locator('#quick-start');
    const text = await quickStart.textContent();
    const hasGet = text.toLowerCase().includes('get ') || text.toLowerCase().includes('get\n');
    expect(hasGet).toBe(true);
  });

  test('Each code block has a copy button', async ({ page }) => {
    const codeBlocks = page.locator('#quick-start .code-block-wrapper');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const wrapper = codeBlocks.nth(i);
      const copyButton = wrapper.locator('button.copy-button');
      await expect(copyButton).toBeVisible();
    }
  });

  test('Clicking copy button copies exact code content to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const firstWrapper = page.locator('#quick-start .code-block-wrapper').first();
    const copyButton = firstWrapper.locator('button.copy-button');
    await expect(copyButton).toBeVisible();

    // Get the code content before clicking
    const codeElement = firstWrapper.locator('code');
    const expectedText = await codeElement.textContent();

    // Click the copy button
    await copyButton.click();

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('Copy button shows "Copied!" feedback after click', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const firstWrapper = page.locator('#quick-start .code-block-wrapper').first();
    const copyButton = firstWrapper.locator('button.copy-button');

    await copyButton.click();

    const label = copyButton.locator('.copy-label');
    await expect(label).toHaveText('Copied!');
    await expect(copyButton).toHaveClass(/copied/);
  });

  test('Syntax highlighting classes are present on code elements', async ({ page }) => {
    const codeBlocks = page.locator('#quick-start .code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check for at least one syntax class
    const syntaxClasses = [
      '.syntax-command',
      '.syntax-key',
      '.syntax-string',
      '.syntax-number',
      '.syntax-response',
      '.syntax-keyword',
      '.syntax-method',
      '.syntax-comment',
    ];

    let hasAnySyntaxClass = false;
    for (const cls of syntaxClasses) {
      const elements = page.locator(`#quick-start ${cls}`);
      const elementCount = await elements.count();
      if (elementCount > 0) {
        hasAnySyntaxClass = true;
        break;
      }
    }

    expect(hasAnySyntaxClass).toBe(true);
  });

  test('Set example code block contains set command and STORED response', async ({ page }) => {
    const setExample = page.locator('[data-testid="quick-start-set-example"]');
    await expect(setExample).toBeVisible();

    const code = setExample.locator('code');
    const text = await code.textContent();
    expect(text.toLowerCase()).toContain('set');
    expect(text).toContain('STORED');
  });

  test('Get example code block contains get command and VALUE response', async ({ page }) => {
    const getExample = page.locator('[data-testid="quick-start-get-example"]');
    await expect(getExample).toBeVisible();

    const code = getExample.locator('code');
    const text = await code.textContent();
    expect(text.toLowerCase()).toContain('get');
    expect(text).toContain('VALUE');
  });
});
