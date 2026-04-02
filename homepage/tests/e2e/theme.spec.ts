/**
 * E2E tests for Dark Mode Theme Support
 * Owner: Scenario 11 - Dark Mode Theme Support
 *
 * Test Cases:
 * 1. Theme toggle button/icon is visible on page
 * 2. Page switches to dark color scheme (dark background, light text)
 * 3. Text remains readable with WCAG AA contrast ratios
 * 4. Dark mode preference is persisted (via localStorage)
 * 5. Page switches back to light color scheme
 */
import { test, expect } from '@playwright/test';

test.describe('Dark Mode Theme Support', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  test('TC1: Theme toggle button/icon is visible on page', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify the toggle has an accessible aria-label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/theme|mode/i);
  });

  test('TC2: Page switches to dark color scheme (dark background, light text)', async ({ page }) => {
    // Initial state should be light mode
    let dataTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(dataTheme).not.toBe('dark');

    // Click the theme toggle
    const themeToggle = page.getByTestId('theme-toggle');
    await themeToggle.click();

    // Wait for the theme to change
    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'dark'
    );

    // Verify dark mode is applied
    dataTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(dataTheme).toBe('dark');

    // Verify background color has changed to a dark color
    const backgroundColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Parse RGB values to verify it's a dark color
    const rgbMatch = backgroundColor.match(/\d+/g);
    if (rgbMatch) {
      const [r, g, b] = rgbMatch.map(Number);
      // Dark backgrounds should have low RGB values
      expect(r + g + b).toBeLessThan(200);
    }

    // Verify text color is light
    const textColor = await page.evaluate(() => {
      return getComputedStyle(document.body).color;
    });
    const textRgbMatch = textColor.match(/\d+/g);
    if (textRgbMatch) {
      const [r, g, b] = textRgbMatch.map(Number);
      // Light text should have high RGB values
      expect(r + g + b).toBeGreaterThan(400);
    }
  });

  test('TC3: Text remains readable with WCAG AA contrast ratios', async ({ page }) => {
    // Switch to dark mode
    const themeToggle = page.getByTestId('theme-toggle');
    await themeToggle.click();

    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'dark'
    );

    // Get computed colors
    const colors = await page.evaluate(() => {
      const body = document.body;
      const bgColor = getComputedStyle(body).backgroundColor;
      const textColor = getComputedStyle(body).color;
      return { bgColor, textColor };
    });

    // Parse RGB values
    function parseRgb(color: string): [number, number, number] {
      const match = color.match(/\d+/g);
      if (match) {
        return [Number(match[0]), Number(match[1]), Number(match[2])];
      }
      return [0, 0, 0];
    }

    // Calculate relative luminance
    function getLuminance(r: number, g: number, b: number): number {
      const [rs, gs, bs] = [r, g, b].map((c) => {
        c /= 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    // Calculate contrast ratio
    function getContrastRatio(l1: number, l2: number): number {
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    const bgRgb = parseRgb(colors.bgColor);
    const textRgb = parseRgb(colors.textColor);

    const bgLuminance = getLuminance(...bgRgb);
    const textLuminance = getLuminance(...textRgb);

    const contrastRatio = getContrastRatio(bgLuminance, textLuminance);

    // WCAG AA requires contrast ratio of at least 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('TC4: Dark mode preference is persisted (via localStorage)', async ({ page }) => {
    // Switch to dark mode
    const themeToggle = page.getByTestId('theme-toggle');
    await themeToggle.click();

    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'dark'
    );

    // Verify localStorage has the theme preference
    const storedTheme = await page.evaluate(() =>
      localStorage.getItem('mirdb-theme')
    );
    expect(storedTheme).toBe('dark');

    // Reload the page
    await page.reload();

    // Wait for the page to load
    await page.waitForSelector('[data-testid="theme-toggle"]');

    // Verify dark mode is still applied after reload
    const dataTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(dataTheme).toBe('dark');
  });

  test('TC5: Page switches back to light color scheme', async ({ page }) => {
    // First, switch to dark mode
    const themeToggle = page.getByTestId('theme-toggle');
    await themeToggle.click();

    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'dark'
    );

    // Verify we're in dark mode
    let dataTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(dataTheme).toBe('dark');

    // Toggle back to light mode
    await themeToggle.click();

    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'light'
    );

    // Verify light mode is applied
    dataTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(dataTheme).toBe('light');

    // Verify background color has changed to a light color
    const backgroundColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    const rgbMatch = backgroundColor.match(/\d+/g);
    if (rgbMatch) {
      const [r, g, b] = rgbMatch.map(Number);
      // Light backgrounds should have high RGB values
      expect(r + g + b).toBeGreaterThan(600);
    }

    // Verify localStorage is updated
    const storedTheme = await page.evaluate(() =>
      localStorage.getItem('mirdb-theme')
    );
    expect(storedTheme).toBe('light');
  });

  test('Theme toggle icon changes based on current theme', async ({ page }) => {
    // In light mode, should show sun icon
    const themeToggle = page.getByTestId('theme-toggle');

    // Switch to dark mode
    await themeToggle.click();
    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'dark'
    );

    // In dark mode, aria-label should indicate switching to light mode
    let ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toMatch(/light/i);

    // Switch back to light mode
    await themeToggle.click();
    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'light'
    );

    // In light mode, aria-label should indicate switching to dark mode
    ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toMatch(/dark/i);
  });

  test('Theme toggle is keyboard accessible', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle');

    // Focus the toggle using Tab
    await page.keyboard.press('Tab');

    // Press Enter to toggle
    await themeToggle.focus();
    await page.keyboard.press('Enter');

    // Verify theme changed
    await page.waitForFunction(() =>
      document.documentElement.getAttribute('data-theme') === 'dark'
    );

    const dataTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(dataTheme).toBe('dark');
  });
});
