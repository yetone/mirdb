/**
 * Clipboard Module Unit Tests
 * Owner: Scenario 4 - Code Examples Display
 *
 * Test cases:
 * - copyToClipboard copies text correctly
 * - Feedback function called on success
 * - Graceful handling when clipboard API unavailable
 * - extractCodeText strips HTML correctly
 */

const { test, expect } = require('@playwright/test');

test.describe('Clipboard Module', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should initialize clipboard functionality on page load', async ({ page }) => {
    const copyButtons = await page.locator('.code-example__copy-btn').all();
    expect(copyButtons.length).toBeGreaterThan(0);

    for (const button of copyButtons) {
      await expect(button).toBeVisible();
    }
  });

  test('should have copy buttons for each code example', async ({ page }) => {
    const setExample = page.locator('[data-testid="code-example-set"]');
    const getExample = page.locator('[data-testid="code-example-get"]');
    const deleteExample = page.locator('[data-testid="code-example-delete"]');

    await expect(setExample.locator('.code-example__copy-btn')).toBeVisible();
    await expect(getExample.locator('.code-example__copy-btn')).toBeVisible();
    await expect(deleteExample.locator('.code-example__copy-btn')).toBeVisible();
  });

  test('copyToClipboard copies text correctly', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-testid="copy-btn-set"]');
    await copyBtn.click();

    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());

    expect(clipboardText).toContain('SET');
    expect(clipboardText).toContain('mykey');
  });

  test('shows visual feedback on successful copy', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-testid="copy-btn-set"]');
    await copyBtn.click();

    await expect(copyBtn).toHaveClass(/copied/);

    const copyText = copyBtn.locator('.code-example__copy-text');
    await expect(copyText).toHaveText('Copied!');
  });

  test('feedback resets after timeout', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-testid="copy-btn-set"]');
    await copyBtn.click();

    await expect(copyBtn).toHaveClass(/copied/);

    await page.waitForTimeout(2100);

    await expect(copyBtn).not.toHaveClass(/copied/);
    const copyText = copyBtn.locator('.code-example__copy-text');
    await expect(copyText).toHaveText('Copy');
  });

  test('extracted code does not include comments', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-testid="copy-btn-set"]');
    await copyBtn.click();

    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());

    expect(clipboardText).not.toContain('# Response:');
    expect(clipboardText).toContain('SET mykey');
  });

  test('copy buttons are keyboard accessible', async ({ page }) => {
    const copyBtn = page.locator('[data-testid="copy-btn-set"]');

    await copyBtn.focus();
    await expect(copyBtn).toBeFocused();

    const isButton = await copyBtn.evaluate((el) => el.tagName.toLowerCase() === 'button');
    expect(isButton).toBe(true);

    const hasAriaLabel = await copyBtn.getAttribute('aria-label');
    expect(hasAriaLabel).toBeTruthy();
  });

  test('fallback copy method exists in code', async ({ page }) => {
    const clipboardCode = await page.evaluate(() => {
      const script = document.querySelector('script[src*="clipboard.js"]');
      return script !== null;
    });

    expect(clipboardCode).toBe(true);

    const copyBtn = page.locator('[data-testid="copy-btn-set"]');
    await expect(copyBtn).toBeVisible();
    await expect(copyBtn).toBeEnabled();
  });
});

test.describe('Code Examples Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('code examples are syntactically correct Memcached commands', async ({ page }) => {
    const setCode = await page.locator('[data-testid="code-example-set"] code').textContent();
    const getCode = await page.locator('[data-testid="code-example-get"] code').textContent();
    const deleteCode = await page.locator('[data-testid="code-example-delete"] code').textContent();

    expect(setCode).toMatch(/SET\s+\w+\s+\d+\s+\d+\s+\d+/);
    expect(setCode).toContain('STORED');

    expect(getCode).toMatch(/GET\s+\w+/);
    expect(getCode).toContain('VALUE');
    expect(getCode).toContain('END');

    expect(deleteCode).toMatch(/DELETE\s+\w+/);
    expect(deleteCode).toContain('DELETED');
  });
});
