/**
 * Unit Tests for Theme Toggle Module
 * Owner: Scenario 6 - Theme Support
 *
 * Tests:
 * - initTheme function
 * - getTheme function
 * - setTheme function
 * - toggleTheme function
 */

const { test, expect } = require('@playwright/test');

test.describe('Theme Toggle Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Clear localStorage before each test
    await page.evaluate(() => localStorage.clear());
  });

  test('Test Case 3: Theme toggle button is visible in header', async ({ page }) => {
    // Check for theme toggle button
    const toggleButton = page.locator('[data-theme-toggle]');
    await expect(toggleButton).toBeVisible();

    // Verify it has appropriate ARIA attributes
    await expect(toggleButton).toHaveAttribute('aria-label', /Switch to (dark|light) theme/);
  });

  test('Test Case 7: initTheme correctly reads from localStorage', async ({ page }) => {
    // Set dark theme in localStorage
    await page.evaluate(() => {
      localStorage.setItem('mirdb-theme', 'dark');
    });

    // Reload page to trigger initTheme
    await page.reload();

    // Wait for DOM to be ready
    await page.waitForLoadState('domcontentloaded');

    // Verify theme is applied from localStorage
    const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('dark');
  });

  test('Test Case 7: initTheme correctly reads from system preference when no localStorage', async ({ page, context }) => {
    // Clear any saved preference
    await page.evaluate(() => localStorage.removeItem('mirdb-theme'));

    // Reload page - system preference will be detected
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify theme attribute exists
    const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme === 'light' || theme === 'dark').toBeTruthy();
  });

  test('Test Case 8: getTheme returns current theme as light or dark', async ({ page }) => {
    // Set light theme
    await page.evaluate(() => {
      window.setTheme('light');
    });

    let theme = await page.evaluate(() => window.getTheme());
    expect(theme).toBe('light');

    // Set dark theme
    await page.evaluate(() => {
      window.setTheme('dark');
    });

    theme = await page.evaluate(() => window.getTheme());
    expect(theme).toBe('dark');
  });

  test('Test Case 9: setTheme sets data-theme attribute and stores in localStorage', async ({ page }) => {
    // Set dark theme
    await page.evaluate(() => {
      window.setTheme('dark');
    });

    // Verify data-theme attribute
    const dataTheme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(dataTheme).toBe('dark');

    // Verify localStorage
    const storedTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedTheme).toBe('dark');
  });

  test('Test Case 9: setTheme handles invalid theme gracefully', async ({ page }) => {
    // Try to set invalid theme
    await page.evaluate(() => {
      window.setTheme('invalid');
    });

    // Should default to light
    const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(theme).toBe('light');
  });

  test('toggleTheme switches between themes correctly', async ({ page }) => {
    // Start with light theme
    await page.evaluate(() => {
      window.setTheme('light');
    });

    // Toggle to dark
    await page.evaluate(() => {
      window.toggleTheme();
    });

    let theme = await page.evaluate(() => window.getTheme());
    expect(theme).toBe('dark');

    // Toggle back to light
    await page.evaluate(() => {
      window.toggleTheme();
    });

    theme = await page.evaluate(() => window.getTheme());
    expect(theme).toBe('light');
  });

  test('Theme toggle button updates icon when clicked', async ({ page }) => {
    // Start with light theme
    await page.evaluate(() => {
      window.setTheme('light');
      const toggleBtn = document.querySelector('[data-theme-toggle]');
      const sunIcon = toggleBtn.querySelector('.theme-toggle__icon--sun');
      const moonIcon = toggleBtn.querySelector('.theme-toggle__icon--moon');
      sunIcon.style.display = 'none';
      moonIcon.style.display = 'block';
    });

    // Click the toggle button
    await page.click('[data-theme-toggle]');

    // Wait for theme change
    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'dark');

    // Verify the sun icon is now visible (to switch back to light)
    const sunDisplay = await page.evaluate(() => {
      const toggleBtn = document.querySelector('[data-theme-toggle]');
      const sunIcon = toggleBtn.querySelector('.theme-toggle__icon--sun');
      return getComputedStyle(sunIcon).display;
    });

    expect(sunDisplay).not.toBe('none');
  });
});
