/**
 * E2E Tests for Theme Support
 * Owner: Scenario 6 - Theme Support
 *
 * Tests:
 * - System preference detection
 * - Theme toggle functionality
 * - Theme persistence
 * - Smooth transitions
 */

const { test, expect } = require('@playwright/test');

test.describe('Theme E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
  });

  test('Test Case 1: Page loads in dark theme with prefers-color-scheme: dark', async ({ page }) => {
    // Clear localStorage to ensure system preference is used
    await page.evaluate(() => localStorage.removeItem('mirdb-theme'));

    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });

    // Reload page to apply system preference
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Give time for theme initialization
    await page.waitForTimeout(100);

    // Check that page loads in dark theme
    const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('dark');

    // Verify dark background color is applied
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Dark theme background should be dark (close to #121212 = rgb(18, 18, 18))
    expect(bgColor).toMatch(/rgb\(18,\s*18,\s*18\)|#121212/i);
  });

  test('Test Case 2: Page loads in light theme with prefers-color-scheme: light', async ({ page }) => {
    // Clear localStorage to ensure system preference is used
    await page.evaluate(() => localStorage.removeItem('mirdb-theme'));

    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });

    // Reload page to apply system preference
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Give time for theme initialization
    await page.waitForTimeout(100);

    // Check that page loads in light theme
    const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('light');

    // Verify light background color is applied
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Light theme background should be white (rgb(255, 255, 255))
    expect(bgColor).toMatch(/rgb\(255,\s*255,\s*255\)|#ffffff|white/i);
  });

  test('Test Case 4: Click theme toggle from light mode switches to dark theme', async ({ page }) => {
    // Set light theme first
    await page.evaluate(() => {
      window.setTheme('light');
    });

    // Verify starting theme
    let theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('light');

    // Click the toggle button
    await page.click('[data-theme-toggle]');

    // Wait for theme change
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'dark');

    // Verify theme switched to dark
    theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('dark');
  });

  test('Test Case 5: Click theme toggle from dark mode switches to light theme', async ({ page }) => {
    // Set dark theme first
    await page.evaluate(() => {
      window.setTheme('dark');
    });

    // Verify starting theme
    let theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('dark');

    // Click the toggle button
    await page.click('[data-theme-toggle]');

    // Wait for theme change
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'light');

    // Verify theme switched to light
    theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('light');
  });

  test('Test Case 6: Theme preference persists after page refresh', async ({ page }) => {
    // Set dark theme
    await page.evaluate(() => {
      window.setTheme('dark');
      localStorage.setItem('mirdb-theme', 'dark');
    });

    // Verify dark theme is set
    let theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('dark');

    // Refresh the page
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Wait for theme initialization
    await page.waitForTimeout(100);

    // Verify dark theme is still applied
    theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('dark');

    // Verify localStorage still has the preference
    const storedTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedTheme).toBe('dark');
  });

  test('Theme toggle has smooth transition effect', async ({ page }) => {
    // Check that transitions are applied
    const transition = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).transition;
    });

    // Transition should include background-color
    expect(transition).toContain('background-color');
  });

  test('Theme toggle button has accessible ARIA attributes', async ({ page }) => {
    const toggleButton = page.locator('[data-theme-toggle]');

    // Check button has aria-label
    await expect(toggleButton).toHaveAttribute('aria-label', /Switch to (dark|light) theme/);

    // Check button has title
    await expect(toggleButton).toHaveAttribute('title', 'Toggle theme');

    // Check button type
    await expect(toggleButton).toHaveAttribute('type', 'button');
  });

  test('Theme toggle updates ARIA label after toggling', async ({ page }) => {
    // Set light theme first
    await page.evaluate(() => {
      window.setTheme('light');
    });

    // Check initial aria-label (should indicate switching to dark)
    const toggleButton = page.locator('[data-theme-toggle]');
    await expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark theme');

    // Click toggle
    await page.click('[data-theme-toggle]');

    // Wait for update
    await page.waitForTimeout(100);

    // Check updated aria-label (should indicate switching to light)
    await expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light theme');
  });
});
