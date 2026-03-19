/**
 * E2E tests for Dark/Light mode theme toggle.
 * Owner: Scenario 11 - Dark and Light Mode Toggle
 */

import { test, expect } from '@playwright/test';

test.describe('Theme Toggle Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
  });

  // Test Case 2: Set system preference to dark mode
  test('TC2: Homepage loads with dark theme when system prefers dark mode', async ({
    page,
  }) => {
    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });

    // Reload to apply system preference
    await page.goto('/');

    // Wait for theme to be applied
    await page.waitForTimeout(500);

    // Check that dark mode class is applied to HTML element
    const htmlElement = page.locator('html');
    await expect(htmlElement).toHaveClass(/dark/);

    // Check body background is dark
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );

    // Parse RGB values - dark background should have low values
    const rgbMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Dark mode background should be dark (low RGB values)
      expect(r).toBeLessThan(100);
      expect(g).toBeLessThan(100);
      expect(b).toBeLessThan(100);
    }
  });

  // Test Case 3: Click theme toggle to switch to dark mode
  test('TC3: Click theme toggle switches background and text colors', async ({
    page,
  }) => {
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Find the theme toggle button
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial background color
    const body = page.locator('body');
    const initialBgColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );

    // Click the theme toggle
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(300);

    // Get new background color
    const newBgColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );

    // Colors should be different after toggle
    expect(newBgColor).not.toBe(initialBgColor);

    // If we toggled to dark mode, verify dark class is present
    const htmlElement = page.locator('html');
    const hasDarkClass = await htmlElement.evaluate((el) =>
      el.classList.contains('dark')
    );

    if (hasDarkClass) {
      // Verify dark mode: background should be dark
      const rgbMatch = newBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        expect(r).toBeLessThan(100);
        expect(g).toBeLessThan(100);
        expect(b).toBeLessThan(100);
      }

      // Check text color is light
      const textColor = await body.evaluate((el) =>
        getComputedStyle(el).color
      );
      const textRgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (textRgbMatch) {
        const [, r, g, b] = textRgbMatch.map(Number);
        // Light text should have high RGB values
        expect(r).toBeGreaterThan(150);
        expect(g).toBeGreaterThan(150);
        expect(b).toBeGreaterThan(150);
      }
    }
  });

  // Test Case 4: Toggle theme and reload page
  test('TC4: Selected theme persists after page reload via localStorage', async ({
    page,
  }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Find the theme toggle button
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Click to toggle theme
    await themeToggle.click();

    // Wait for localStorage to be updated
    await page.waitForTimeout(300);

    // Check localStorage has theme value
    const storedTheme = await page.evaluate(() =>
      localStorage.getItem('mirdb-theme')
    );
    expect(storedTheme).not.toBeNull();

    // Get current dark mode state
    const isDarkBeforeReload = await page.evaluate(() =>
      document.documentElement.classList.contains('dark')
    );

    // Reload the page
    await page.reload();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(500);

    // Check theme persisted after reload
    const isDarkAfterReload = await page.evaluate(() =>
      document.documentElement.classList.contains('dark')
    );

    expect(isDarkAfterReload).toBe(isDarkBeforeReload);

    // Verify localStorage still has the theme
    const storedThemeAfterReload = await page.evaluate(() =>
      localStorage.getItem('mirdb-theme')
    );
    expect(storedThemeAfterReload).toBe(storedTheme);
  });

  // Test Case 5: Verify dark mode color contrast
  test('TC5: Dark mode maintains WCAG AA contrast ratio (4.5:1)', async ({
    page,
  }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Enable dark mode
    const themeToggle = page.getByTestId('theme-toggle');
    await themeToggle.click();
    await page.waitForTimeout(300);

    // Verify we're in dark mode
    const htmlElement = page.locator('html');
    await expect(htmlElement).toHaveClass(/dark/);

    // Get body colors
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    const textColor = await body.evaluate((el) => getComputedStyle(el).color);

    // Parse RGB values
    const bgMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    const textMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);

    expect(bgMatch).not.toBeNull();
    expect(textMatch).not.toBeNull();

    if (bgMatch && textMatch) {
      const bgRgb = {
        r: parseInt(bgMatch[1]),
        g: parseInt(bgMatch[2]),
        b: parseInt(bgMatch[3]),
      };
      const textRgb = {
        r: parseInt(textMatch[1]),
        g: parseInt(textMatch[2]),
        b: parseInt(textMatch[3]),
      };

      // Calculate relative luminance
      const getLuminance = (rgb: { r: number; g: number; b: number }) => {
        const [rs, gs, bs] = [rgb.r / 255, rgb.g / 255, rgb.b / 255].map(
          (c) => (c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4))
        );
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      };

      const bgLuminance = getLuminance(bgRgb);
      const textLuminance = getLuminance(textRgb);

      // Calculate contrast ratio
      const lighter = Math.max(bgLuminance, textLuminance);
      const darker = Math.min(bgLuminance, textLuminance);
      const contrastRatio = (lighter + 0.05) / (darker + 0.05);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    }
  });

  // Additional theme tests
  test('Theme toggle button has proper accessibility attributes', async ({
    page,
  }) => {
    await page.goto('/');

    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Check for aria-label
    await expect(themeToggle).toHaveAttribute('aria-label', /(light|dark) mode/i);

    // Check for aria-pressed
    await expect(themeToggle).toHaveAttribute('aria-pressed');
  });

  test('Theme toggle updates aria-label on toggle', async ({ page }) => {
    await page.goto('/');

    const themeToggle = page.getByTestId('theme-toggle');
    const initialLabel = await themeToggle.getAttribute('aria-label');

    await themeToggle.click();
    await page.waitForTimeout(300);

    const newLabel = await themeToggle.getAttribute('aria-label');
    expect(newLabel).not.toBe(initialLabel);
  });

  test('Homepage respects light mode system preference', async ({ page }) => {
    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });

    await page.goto('/');
    await page.waitForTimeout(500);

    // Check that dark class is NOT applied
    const htmlElement = page.locator('html');
    const hasDarkClass = await htmlElement.evaluate((el) =>
      el.classList.contains('dark')
    );
    expect(hasDarkClass).toBe(false);

    // Check body background is light
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );

    // Light mode background should be bright (high RGB values)
    const rgbMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      expect(r).toBeGreaterThan(200);
      expect(g).toBeGreaterThan(200);
      expect(b).toBeGreaterThan(200);
    }
  });

  test('Theme toggle can cycle between themes', async ({ page }) => {
    await page.goto('/');

    const themeToggle = page.getByTestId('theme-toggle');

    // Get initial state
    const initialDark = await page.evaluate(() =>
      document.documentElement.classList.contains('dark')
    );

    // Toggle once
    await themeToggle.click();
    await page.waitForTimeout(300);

    const afterFirstToggle = await page.evaluate(() =>
      document.documentElement.classList.contains('dark')
    );
    expect(afterFirstToggle).not.toBe(initialDark);

    // Toggle again
    await themeToggle.click();
    await page.waitForTimeout(300);

    const afterSecondToggle = await page.evaluate(() =>
      document.documentElement.classList.contains('dark')
    );
    expect(afterSecondToggle).toBe(initialDark);
  });
});
