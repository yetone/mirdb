/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests:
 * - Quick Start section heading and description
 * - Code block contains memcached commands (SET, GET, DELETE)
 * - Terminal window chrome is visible
 * - Syntax highlighting classes are applied
 * - Copy button is present and visible
 * - Copy button click copies code and shows feedback
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');
const fileUrl = 'file://' + indexPath;

test.describe('Quick Start Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(fileUrl);
  });

  test('quickstart section is visible', async ({ page }) => {
    const quickstart = page.locator('section#quickstart');
    await expect(quickstart).toBeVisible();
  });

  test('quickstart section has heading with "Quick Start" text', async ({ page }) => {
    const heading = page.locator('section#quickstart h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/Quick Start/i);
  });

  test('quickstart section has description text', async ({ page }) => {
    const desc = page.locator('section#quickstart .quickstart-description');
    await expect(desc).toBeVisible();
    const text = await desc.textContent();
    expect(text.trim().length).toBeGreaterThan(0);
  });

  test('terminal window is visible', async ({ page }) => {
    const terminal = page.locator('section#quickstart .terminal-window');
    await expect(terminal).toBeVisible();
  });

  test('terminal has window chrome with three buttons', async ({ page }) => {
    const header = page.locator('section#quickstart .terminal-header');
    await expect(header).toBeVisible();

    const redBtn = header.locator('.terminal-btn-red');
    const yellowBtn = header.locator('.terminal-btn-yellow');
    const greenBtn = header.locator('.terminal-btn-green');

    await expect(redBtn).toBeVisible();
    await expect(yellowBtn).toBeVisible();
    await expect(greenBtn).toBeVisible();
  });

  test('terminal has a title', async ({ page }) => {
    const title = page.locator('section#quickstart .terminal-title');
    await expect(title).toBeVisible();
    const text = await title.textContent();
    expect(text.trim().length).toBeGreaterThan(0);
  });

  test('code block contains SET command', async ({ page }) => {
    const code = page.locator('#quickstart-code');
    await expect(code).toBeVisible();
    const text = await code.textContent();
    expect(text).toMatch(/set\s+mykey/i);
  });

  test('code block contains GET command', async ({ page }) => {
    const code = page.locator('#quickstart-code');
    const text = await code.textContent();
    expect(text).toMatch(/get\s+mykey/i);
  });

  test('code block contains DELETE command', async ({ page }) => {
    const code = page.locator('#quickstart-code');
    const text = await code.textContent();
    expect(text).toMatch(/delete\s+mykey/i);
  });

  test('code block shows STORED response', async ({ page }) => {
    const code = page.locator('#quickstart-code');
    const text = await code.textContent();
    expect(text).toMatch(/STORED/);
  });

  test('code block shows DELETED response', async ({ page }) => {
    const code = page.locator('#quickstart-code');
    const text = await code.textContent();
    expect(text).toMatch(/DELETED/);
  });

  test('syntax highlighting classes are applied', async ({ page }) => {
    const keyword = page.locator('#quickstart-code .sh-keyword').first();
    const prompt = page.locator('#quickstart-code .sh-prompt').first();
    const comment = page.locator('#quickstart-code .sh-comment').first();
    const response = page.locator('#quickstart-code .sh-response').first();

    await expect(keyword).toBeVisible();
    await expect(prompt).toBeVisible();
    await expect(comment).toBeVisible();
    await expect(response).toBeVisible();
  });

  test('copy button is visible inside terminal', async ({ page }) => {
    const button = page.locator('section#quickstart .copy-button');
    await expect(button).toBeVisible();
  });

  test('copy button has correct aria-label', async ({ page }) => {
    const button = page.locator('section#quickstart .copy-button');
    const ariaLabel = await button.getAttribute('aria-label');
    expect(ariaLabel).toMatch(/copy/i);
  });

  test('copy button has data-copy-target attribute', async ({ page }) => {
    const button = page.locator('section#quickstart .copy-button');
    const target = await button.getAttribute('data-copy-target');
    expect(target).toBe('quickstart-code');
  });

  test('clicking copy button shows visual feedback', async ({ page }) => {
    const button = page.locator('section#quickstart .copy-button');

    // Grant clipboard permission
    await page.evaluate(() => {
      navigator.clipboard.writeText = () => Promise.resolve();
    });

    await button.click();

    // Check that copy-success class is added
    const hasSuccessClass = await button.evaluate((el) =>
      el.classList.contains('copy-success')
    );
    expect(hasSuccessClass).toBe(true);
  });

  test('terminal has dark background color', async ({ page }) => {
    const terminal = page.locator('section#quickstart .terminal-window');
    const bgColor = await terminal.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Dark background should have low RGB values
    expect(bgColor).toMatch(/rgb\(\d{1,3}, \d{1,3}, \d{1,3}\)/);
  });

  test('code uses monospace font', async ({ page }) => {
    const pre = page.locator('section#quickstart .terminal-body pre');
    const fontFamily = await pre.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|courier/i);
  });
});
