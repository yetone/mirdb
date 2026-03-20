/**
 * Code Examples Section E2E Tests
 * Owner: Scenario 4 - Code Examples Display
 *
 * Test cases:
 * - Code examples visible showing SET, GET, DELETE commands
 * - Code blocks have syntax highlighting applied
 * - Copy button functionality with visual feedback
 * - Code examples are syntactically correct
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('code examples section is visible on homepage', async ({ page }) => {
    const examplesSection = page.locator('#examples');
    await expect(examplesSection).toBeVisible();

    const headline = page.locator('#examples-headline');
    await expect(headline).toHaveText('Code Examples');
  });

  test('code examples show SET, GET, DELETE commands', async ({ page }) => {
    const setExample = page.locator('[data-testid="code-example-set"]');
    const getExample = page.locator('[data-testid="code-example-get"]');
    const deleteExample = page.locator('[data-testid="code-example-delete"]');

    await expect(setExample).toBeVisible();
    await expect(getExample).toBeVisible();
    await expect(deleteExample).toBeVisible();

    await expect(setExample.locator('.code-example__title')).toHaveText('SET Command');
    await expect(getExample.locator('.code-example__title')).toHaveText('GET Command');
    await expect(deleteExample.locator('.code-example__title')).toHaveText('DELETE Command');

    await expect(setExample.locator('code')).toContainText('SET mykey');
    await expect(getExample.locator('code')).toContainText('GET mykey');
    await expect(deleteExample.locator('code')).toContainText('DELETE mykey');
  });

  test('code blocks have syntax highlighting applied', async ({ page }) => {
    const codeExample = page.locator('[data-testid="code-example-set"]');

    const keywordSpan = codeExample.locator('.code-keyword');
    await expect(keywordSpan).toBeVisible();

    const keywordColor = await keywordSpan.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    const bodyColor = await page.evaluate(() => {
      const defaultEl = document.createElement('span');
      defaultEl.style.color = 'inherit';
      document.body.appendChild(defaultEl);
      const color = window.getComputedStyle(defaultEl).color;
      document.body.removeChild(defaultEl);
      return color;
    });

    expect(keywordColor).not.toBe(bodyColor);

    const highlightClasses = [
      '.code-keyword',
      '.code-key',
      '.code-number',
      '.code-value',
      '.code-response',
    ];

    for (const className of highlightClasses) {
      const elements = codeExample.locator(className);
      const count = await elements.count();
      expect(count).toBeGreaterThan(0);
    }
  });

  test('copy button provides visual feedback on click', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-testid="copy-btn-set"]');

    const initialText = await copyBtn.locator('.code-example__copy-text').textContent();
    expect(initialText).toBe('Copy');

    await copyBtn.click();

    await expect(copyBtn).toHaveClass(/copied/);
    await expect(copyBtn.locator('.code-example__copy-text')).toHaveText('Copied!');

    const checkIcon = copyBtn.locator('.code-example__check-icon');
    await expect(checkIcon).toBeVisible();
  });

  test('code is copied to clipboard on button click', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-testid="copy-btn-get"]');
    await copyBtn.click();

    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());

    expect(clipboardText).toContain('GET mykey');
    expect(clipboardText).toContain('VALUE mykey');
    expect(clipboardText).toContain('hello');
    expect(clipboardText).toContain('END');
  });

  test('code examples have proper styling', async ({ page }) => {
    const codeExample = page.locator('[data-testid="code-example-set"]');

    const bgColor = await codeExample.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bgColor).not.toBe('transparent');

    const codeBlock = codeExample.locator('.code-example__code');
    const fontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|menlo|courier/);
  });

  test('code examples section has proper accessibility', async ({ page }) => {
    const headline = page.locator('#examples-headline');
    await expect(headline).toBeVisible();

    const section = page.locator('#examples');
    const ariaLabel = await section.getAttribute('aria-labelledby');
    expect(ariaLabel).toBe('examples-headline');

    const copyButtons = page.locator('.code-example__copy-btn');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const btn = copyButtons.nth(i);
      const ariaLabelBtn = await btn.getAttribute('aria-label');
      expect(ariaLabelBtn).toBeTruthy();
      expect(ariaLabelBtn).toContain('clipboard');
    }
  });

  test('all three command types are displayed in grid layout', async ({ page }) => {
    const grid = page.locator('.examples__grid');
    await expect(grid).toBeVisible();

    const examples = grid.locator('.code-example');
    expect(await examples.count()).toBe(3);

    const gridDisplay = await grid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');
  });

  test('responsive grid adapts on smaller screens', async ({ page }) => {
    await page.setViewportSize({ width: 400, height: 800 });

    const examples = page.locator('.code-example');
    expect(await examples.count()).toBe(3);

    for (let i = 0; i < 3; i++) {
      const example = examples.nth(i);
      await expect(example).toBeVisible();
    }
  });
});
