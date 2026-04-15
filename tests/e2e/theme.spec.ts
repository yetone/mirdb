/**
 * Theme Switching E2E Tests
 * Owner: Scenario 8 - Theme Switching
 *
 * Tests for dark/light theme switching functionality
 * and localStorage persistence.
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, THEMES, THEME_STORAGE_KEY, COLORS } from '../fixtures/test-data';

test.describe('Theme Switching', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
  });

  test('TC1: Theme toggle element exists with accessible label', async ({ page }) => {
    await page.goto('/');

    // Locate theme toggle
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await expect(themeToggle).toBeVisible();

    // Check accessible label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/toggle|switch|mode|theme/i);

    // Check it's a button for accessibility
    const role = await themeToggle.evaluate(el => el.tagName.toLowerCase());
    expect(role).toBe('button');

    // Check aria-pressed attribute exists
    const ariaPressed = await themeToggle.getAttribute('aria-pressed');
    expect(ariaPressed).toBeTruthy();
  });

  test('TC2: Click theme toggle to enable dark mode', async ({ page }) => {
    await page.goto('/');

    // Start with light theme
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', THEMES.light);

    // Click theme toggle
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await themeToggle.click();

    // Verify dark theme is applied
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);
  });

  test('TC3: Verify dark mode colors are applied', async ({ page }) => {
    await page.goto('/');

    // Switch to dark mode
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(300);

    // Check background color is dark
    const body = page.locator('body');
    const bgColor = await body.evaluate(el => getComputedStyle(el).backgroundColor);

    // Dark background should be dark (low RGB values)
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [_, r, g, b] = rgbMatch.map(Number);
      // Dark backgrounds typically have low values
      expect(r).toBeLessThan(50);
      expect(g).toBeLessThan(50);
      expect(b).toBeLessThan(80);
    }

    // Check text color is light
    const heroTitle = page.locator('.hero__title');
    const textColor = await heroTitle.evaluate(el => getComputedStyle(el).color);

    const textRgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (textRgbMatch) {
      const [_, r, g, b] = textRgbMatch.map(Number);
      // Light text should have high values
      expect(r).toBeGreaterThan(200);
      expect(g).toBeGreaterThan(200);
      expect(b).toBeGreaterThan(200);
    }
  });

  test('TC4: Click theme toggle to enable light mode', async ({ page }) => {
    await page.goto('/');

    // First switch to dark
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await themeToggle.click();

    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);

    // Switch back to light
    await themeToggle.click();

    // Verify light theme is applied
    await expect(html).toHaveAttribute('data-theme', THEMES.light);
  });

  test('TC5: Theme preference persists after page reload', async ({ page }) => {
    await page.goto('/');

    // Switch to dark mode
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await themeToggle.click();

    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);

    // Verify localStorage was set
    const storedTheme = await page.evaluate((key) => localStorage.getItem(key), THEME_STORAGE_KEY);
    expect(storedTheme).toBe(THEMES.dark);

    // Reload the page
    await page.reload();

    // Verify dark mode is still applied
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);
  });

  test('All sections update color scheme in dark mode', async ({ page }) => {
    await page.goto('/');

    // Switch to dark mode
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await themeToggle.click();

    await page.waitForTimeout(300);

    // Check header background
    const header = page.locator('.header');
    const headerBg = await header.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(headerBg).not.toBe('rgb(255, 255, 255)');

    // Check features section background
    const features = page.locator('.features');
    const featuresBg = await features.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(featuresBg).not.toBe('rgb(255, 255, 255)');

    // Check footer background
    const footer = page.locator('.footer');
    const footerBg = await footer.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(footerBg).not.toBe('rgb(255, 255, 255)');
  });

  test('Theme toggle has correct aria-pressed state', async ({ page }) => {
    await page.goto('/');

    const themeToggle = page.locator(SELECTORS.themeToggle);

    // Initially should be false (light mode)
    await expect(themeToggle).toHaveAttribute('aria-pressed', 'false');

    // After clicking (dark mode)
    await themeToggle.click();
    await expect(themeToggle).toHaveAttribute('aria-pressed', 'true');

    // After clicking again (light mode)
    await themeToggle.click();
    await expect(themeToggle).toHaveAttribute('aria-pressed', 'false');
  });

  test('Theme icons are correctly shown based on current theme', async ({ page }) => {
    await page.goto('/');

    // In light mode, light icon should be visible
    const lightIcon = page.locator('.theme-toggle__icon--light');
    const darkIcon = page.locator('.theme-toggle__icon--dark');

    // Light mode - sun icon visible
    await expect(lightIcon).toBeVisible();
    await expect(darkIcon).not.toBeVisible();

    // Switch to dark mode
    const themeToggle = page.locator(SELECTORS.themeToggle);
    await themeToggle.click();

    // Dark mode - moon icon visible
    await expect(lightIcon).not.toBeVisible();
    await expect(darkIcon).toBeVisible();
  });
});
