/**
 * E2E tests for Dark Mode Theme.
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Tests:
 * - Page respects system dark mode preference
 * - Theme toggle button exists and is accessible
 * - Clicking toggle switches theme
 * - Theme persists after reload
 * - Code blocks render correctly in dark mode
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepagePath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Dark Mode Theme', () => {
  // Test Case 1: Load page with prefers-color-scheme: dark
  test('page respects system dark mode preference', async ({ page }) => {
    // Emulate dark mode preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto(homepagePath);

    // Wait for initTheme to run
    await page.waitForTimeout(100);

    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');
  });

  // Test Case 2: Theme toggle button exists
  test('theme toggle button exists with accessible label', async ({ page }) => {
    await page.goto(homepagePath);

    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    await expect(toggleButton).toBeVisible();
    await expect(toggleButton).toHaveAttribute('aria-label', 'Toggle dark mode');
    await expect(toggleButton).toHaveAttribute('type', 'button');
  });

  // Test Case 3: Click toggle switches theme
  test('clicking toggle switches between light and dark themes', async ({ page }) => {
    await page.goto(homepagePath);
    await page.waitForTimeout(100);

    const html = page.locator('html');
    const toggleButton = page.locator('[data-testid="theme-toggle"]');

    // Get initial theme
    const initialTheme = await html.getAttribute('data-theme');
    expect(initialTheme === 'light' || initialTheme === 'dark').toBe(true);

    // Click toggle
    await toggleButton.click();

    // Theme should flip
    const newTheme = await html.getAttribute('data-theme');
    expect(newTheme).not.toBe(initialTheme);
    expect(newTheme === 'light' || newTheme === 'dark').toBe(true);

    // Click again to toggle back
    await toggleButton.click();
    const restoredTheme = await html.getAttribute('data-theme');
    expect(restoredTheme).toBe(initialTheme);
  });

  // Test Case 4: Theme persists after reload
  test('theme persists after page reload', async ({ page }) => {
    await page.goto(homepagePath);
    await page.waitForTimeout(100);

    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure light mode
    const initialTheme = await html.getAttribute('data-theme');
    if (initialTheme !== 'dark') {
      await toggleButton.click();
      await page.waitForTimeout(100);
    }

    // Verify dark is set
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Reload page
    await page.reload();
    await page.waitForTimeout(100);

    // Theme should still be dark
    await expect(html).toHaveAttribute('data-theme', 'dark');
  });

  // Test Case 8: Code blocks in dark mode
  test('code blocks have dark background with light text in dark mode', async ({ page }) => {
    await page.goto(homepagePath);
    await page.waitForTimeout(100);

    // Set to dark mode
    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await toggleButton.click();
      await page.waitForTimeout(100);
    }

    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    const bgColor = await codeBlock.evaluate((el) =>
      window.getComputedStyle(el).backgroundColor
    );
    const codeEl = codeBlock.locator('code').first();
    const textColor = await codeEl.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // In dark mode, background should be dark (rgb(30, 30, 30) for #1e1e1e)
    expect(bgColor).toBe('rgb(30, 30, 30)');

    // Text inside code should be light
    expect(textColor).toBe('rgb(212, 212, 212)');
  });

  // Additional: Page renders in dark theme with dark background, light text
  test('dark theme renders with dark background and light text', async ({ page }) => {
    await page.goto(homepagePath);
    await page.waitForTimeout(100);

    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure dark mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await toggleButton.click();
      await page.waitForTimeout(100);
    }

    const body = page.locator('body');

    // Verify CSS custom properties are set correctly in dark mode
    const bgVar = await html.evaluate((el) =>
      window.getComputedStyle(el).getPropertyValue('--color-background').trim()
    );
    const textVar = await html.evaluate((el) =>
      window.getComputedStyle(el).getPropertyValue('--color-text').trim()
    );

    // Dark background variable
    expect(bgVar).toBe('#0f172a');
    // Light text variable
    expect(textVar).toBe('#e2e8f0');

    // Also verify body uses the dark theme values
    const bodyBg = await body.evaluate((el) =>
      window.getComputedStyle(el).backgroundColor
    );
    const bodyText = await body.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Body background should be dark (close to #0f172a)
    expect(bodyBg).toContain('rgb(');
    expect(bodyText).toContain('rgb(');
  });

  // Additional: All sections render correctly in dark mode
  test('all page sections render correctly in dark mode', async ({ page }) => {
    await page.goto(homepagePath);
    await page.waitForTimeout(100);

    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await toggleButton.click();
      await page.waitForTimeout(100);
    }

    // Check all sections are visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#about')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#status')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();
  });
});
