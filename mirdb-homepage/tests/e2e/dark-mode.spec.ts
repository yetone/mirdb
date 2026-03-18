/**
 * Dark Mode E2E Tests.
 * Owner: Scenario 7 - Dark Mode Toggle
 *
 * Tests:
 * - Theme toggle button presence and accessibility
 * - Default dark mode on initial load
 * - Theme switching between light and dark
 * - Theme persistence after page reload
 * - Color contrast ratio meets WCAG AA standards
 */

import { test, expect } from '@playwright/test';

/**
 * Calculates relative luminance of a color.
 * Formula from WCAG 2.1: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculates contrast ratio between two colors.
 * Returns ratio >= 1 (always lighter/darker relationship)
 */
function getContrastRatio(
  rgb1: { r: number; g: number; b: number },
  rgb2: { r: number; g: number; b: number }
): number {
  const l1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parses an RGB/RGBA color string to RGB values.
 */
function parseRgb(color: string): { r: number; g: number; b: number } {
  const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (match) {
    return {
      r: parseInt(match[1], 10),
      g: parseInt(match[2], 10),
      b: parseInt(match[3], 10),
    };
  }
  // Default to white if parsing fails
  return { r: 255, g: 255, b: 255 };
}

test.describe('Dark Mode Toggle', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test to ensure clean state
    await page.addInitScript(() => {
      window.localStorage.clear();
    });
    await page.goto('/');
  });

  test('TC1: Theme toggle button exists with appropriate aria-label', async ({ page }) => {
    // Query for theme toggle button
    const toggleButton = page.getByTestId('theme-toggle');

    // Verify toggle button is visible
    await expect(toggleButton).toBeVisible();

    // Verify it has an aria-label for accessibility
    const ariaLabel = await toggleButton.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/switch to (light|dark) mode/i);
  });

  test('TC2: Dark mode is active by default (dark background, light text)', async ({ page }) => {
    // Check that html element has 'dark' class
    const htmlElement = page.locator('html');
    await expect(htmlElement).toHaveClass(/dark/);

    // Verify body has dark background color
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) =>
      window.getComputedStyle(el).backgroundColor
    );

    // Dark backgrounds typically have low RGB values
    const bgRgb = parseRgb(backgroundColor);
    const bgLuminance = getLuminance(bgRgb.r, bgRgb.g, bgRgb.b);

    // Dark backgrounds should have luminance < 0.2
    expect(bgLuminance).toBeLessThan(0.2);
  });

  test('TC3: Theme class changes when toggle is clicked (dark to light)', async ({ page }) => {
    const htmlElement = page.locator('html');
    const toggleButton = page.getByTestId('theme-toggle');

    // Verify initially in dark mode
    await expect(htmlElement).toHaveClass(/dark/);

    // Click toggle to switch to light mode
    await toggleButton.click();

    // Verify dark class is removed (light mode active)
    await expect(htmlElement).not.toHaveClass(/dark/);

    // Toggle back to dark
    await toggleButton.click();

    // Verify dark class is added again
    await expect(htmlElement).toHaveClass(/dark/);
  });

  test('TC4: Light theme persists after page reload (localStorage check)', async ({ browser }) => {
    // Use fresh context without addInitScript to test persistence
    const context = await browser.newContext();
    const page = await context.newPage();

    // First visit - clear localStorage and go to page
    await page.goto('/');
    await page.evaluate(() => window.localStorage.clear());
    await page.reload();

    const toggleButton = page.getByTestId('theme-toggle');
    const htmlElement = page.locator('html');

    // Switch to light mode
    await toggleButton.click();
    await expect(htmlElement).not.toHaveClass(/dark/);

    // Verify localStorage was updated
    const storedTheme = await page.evaluate(() =>
      window.localStorage.getItem('mirdb-theme')
    );
    expect(storedTheme).toBe('light');

    // Reload page (without addInitScript clearing localStorage)
    await page.reload();

    // Verify light theme persists
    await expect(htmlElement).not.toHaveClass(/dark/);

    await context.close();
  });

  test('TC5: Text-to-background contrast ratio >= 4.5:1 (WCAG AA) in dark mode', async ({ page }) => {
    // Ensure we're in dark mode
    const htmlElement = page.locator('html');
    await expect(htmlElement).toHaveClass(/dark/);

    // Get computed colors from body
    const colors = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return {
        text: computedStyle.color,
        background: computedStyle.backgroundColor,
      };
    });

    const textRgb = parseRgb(colors.text);
    const bgRgb = parseRgb(colors.background);
    const contrastRatio = getContrastRatio(textRgb, bgRgb);

    // WCAG AA requires contrast ratio >= 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('Text-to-background contrast ratio >= 4.5:1 (WCAG AA) in light mode', async ({ page }) => {
    const toggleButton = page.getByTestId('theme-toggle');

    // Switch to light mode
    await toggleButton.click();

    // Wait for transition
    await page.waitForTimeout(100);

    // Get computed colors from body
    const colors = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return {
        text: computedStyle.color,
        background: computedStyle.backgroundColor,
      };
    });

    const textRgb = parseRgb(colors.text);
    const bgRgb = parseRgb(colors.background);
    const contrastRatio = getContrastRatio(textRgb, bgRgb);

    // WCAG AA requires contrast ratio >= 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('Toggle button shows sun icon in dark mode (to switch to light)', async ({ page }) => {
    // In dark mode, button should offer to switch to light (sun icon)
    const toggleButton = page.getByTestId('theme-toggle');

    // Verify aria-label indicates switching to light mode
    const ariaLabel = await toggleButton.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('light');

    // Verify SVG icon is present
    const svg = toggleButton.locator('svg');
    await expect(svg).toBeVisible();
  });

  test('Toggle button shows moon icon in light mode (to switch to dark)', async ({ page }) => {
    const toggleButton = page.getByTestId('theme-toggle');

    // Switch to light mode
    await toggleButton.click();

    // Verify aria-label indicates switching to dark mode
    const ariaLabel = await toggleButton.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('dark');

    // Verify SVG icon is present
    const svg = toggleButton.locator('svg');
    await expect(svg).toBeVisible();
  });

  test('Dark theme persists after page reload', async ({ page }) => {
    const htmlElement = page.locator('html');

    // Verify initially in dark mode
    await expect(htmlElement).toHaveClass(/dark/);

    // Reload page
    await page.reload();

    // Verify dark mode persists
    await expect(htmlElement).toHaveClass(/dark/);
  });

  test('Theme toggle is keyboard accessible', async ({ page }) => {
    const toggleButton = page.getByTestId('theme-toggle');
    const htmlElement = page.locator('html');

    // Focus the toggle button using keyboard
    await toggleButton.focus();

    // Verify it's focused
    await expect(toggleButton).toBeFocused();

    // Press Enter to toggle
    await page.keyboard.press('Enter');

    // Verify theme changed
    await expect(htmlElement).not.toHaveClass(/dark/);
  });
});
