/**
 * E2E tests for Dark Mode Support.
 * Owner: Scenario 11 - Dark Mode Support
 *
 * Tests:
 * - Page loads in dark mode as default
 * - Page respects system dark mode preference
 * - Background and text colors match PRD specifications
 */
import { test, expect } from '@playwright/test';

test.describe('Dark Mode Support', () => {
  test('page loads in dark mode when system prefers dark', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const html = page.locator('html');
    await expect(html).toHaveClass(/dark/);

    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(backgroundColor).toBe('rgb(13, 17, 23)');

    await context.close();
  });

  test('respects system prefers-color-scheme: dark preference', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const html = page.locator('html');
    await expect(html).toHaveClass(/dark/);

    await context.close();
  });

  test('respects system prefers-color-scheme: light preference when no stored theme', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'light',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const toggle = page.getByTestId('theme-toggle');
    await expect(toggle).toBeVisible();
    await page.waitForTimeout(100);
    const theme = await toggle.getAttribute('data-theme');
    expect(theme).toBe('light');

    await context.close();
  });

  test('dark mode background color is #0d1117', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(backgroundColor).toBe('rgb(13, 17, 23)');

    await context.close();
  });

  test('dark mode text color is #c9d1d9', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const body = page.locator('body');
    const textColor = await body.evaluate((el) =>
      getComputedStyle(el).color
    );
    expect(textColor).toBe('rgb(201, 209, 217)');

    await context.close();
  });

  test('theme toggle button is visible and accessible', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.goto('/');

    const toggle = page.getByTestId('theme-toggle');
    await expect(toggle).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-label');

    await context.close();
  });

  test('theme toggle switches between dark and light modes', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const toggle = page.getByTestId('theme-toggle');
    await expect(toggle).toBeVisible();
    await expect(toggle).toHaveAttribute('data-theme', 'dark');

    await toggle.click();

    await expect(toggle).toHaveAttribute('data-theme', 'light');

    const html = page.locator('html');
    await expect(html).toHaveClass(/light/);

    await toggle.click();

    await expect(toggle).toHaveAttribute('data-theme', 'dark');
    await expect(html).toHaveClass(/dark/);

    await context.close();
  });

  test('theme persists after page reload via localStorage', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'dark',
    });
    const page = await context.newPage();

    await page.goto('/');

    await page.evaluate(() => localStorage.removeItem('mirdb-theme'));

    await page.reload();

    const toggle = page.getByTestId('theme-toggle');
    await expect(toggle).toBeVisible();

    await toggle.click();
    await expect(toggle).toHaveAttribute('data-theme', 'light');

    const storedTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedTheme).toBe('light');

    await page.reload();

    await expect(toggle).toHaveAttribute('data-theme', 'light');

    const html = page.locator('html');
    await expect(html).toHaveClass(/light/);

    await context.close();
  });

  test('light mode has correct colors', async ({ browser }) => {
    const context = await browser.newContext({
      colorScheme: 'light',
    });
    const page = await context.newPage();

    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await page.goto('/');

    const body = page.locator('body');

    const backgroundColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(backgroundColor).toBe('rgb(255, 255, 255)');

    const textColor = await body.evaluate((el) =>
      getComputedStyle(el).color
    );
    expect(textColor).toBe('rgb(36, 41, 47)');

    await context.close();
  });
});
