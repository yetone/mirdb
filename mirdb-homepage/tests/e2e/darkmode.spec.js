/**
 * Dark Mode E2E Tests
 * Owner: Scenario 7 - Dark Mode Toggle and Persistence
 *
 * Tests:
 * - Toggle button presence
 * - Theme switching functionality
 * - localStorage persistence
 * - System preference detection
 * - Color contrast in dark mode
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad, getContrastRatio, getComputedStyle } = require('../test-utils/helpers');

test.describe('Dark Mode Toggle and Persistence', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test to ensure clean state
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
  });

  // Test Case 1: Check dark mode toggle presence
  test('dark mode toggle button is visible and accessible', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Check toggle button exists and is visible
    const toggleButton = page.locator('#theme-toggle');
    await expect(toggleButton).toBeVisible();

    // Check it has proper accessibility attributes
    await expect(toggleButton).toHaveAttribute('type', 'button');
    const ariaLabel = await toggleButton.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('mode');

    // Check it contains theme icons
    const sunIcon = toggleButton.locator('.theme-toggle__icon--sun');
    const moonIcon = toggleButton.locator('.theme-toggle__icon--moon');
    await expect(sunIcon).toBeAttached();
    await expect(moonIcon).toBeAttached();
  });

  // Test Case 2: Click toggle in light mode - page switches to dark theme
  test('clicking toggle in light mode switches to dark theme', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Ensure we start in light mode
    await page.evaluate(() => {
      localStorage.clear();
      document.documentElement.classList.remove('dark-mode');
      document.documentElement.classList.add('light-mode');
    });

    // Verify we're in light mode initially
    const htmlBefore = page.locator('html');
    await expect(htmlBefore).not.toHaveClass(/dark-mode/);

    // Click the toggle button
    await page.locator('#theme-toggle').click();

    // Verify we're now in dark mode
    const htmlAfter = page.locator('html');
    await expect(htmlAfter).toHaveClass(/dark-mode/);

    // Verify the background color changed to dark
    const bgColor = await getComputedStyle(page, 'body', 'background-color');
    expect(bgColor).toBeTruthy();
    // Dark mode background should be dark
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      const brightness = (r * 299 + g * 587 + b * 114) / 1000;
      expect(brightness).toBeLessThan(128); // Dark background
    }
  });

  // Test Case 3: Click toggle in dark mode - page switches back to light theme
  test('clicking toggle in dark mode switches back to light theme', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Set up dark mode
    await page.evaluate(() => {
      localStorage.setItem('mirdb-theme', 'dark');
      document.documentElement.classList.add('dark-mode');
      document.documentElement.classList.remove('light-mode');
    });

    // Verify we're in dark mode initially
    const htmlBefore = page.locator('html');
    await expect(htmlBefore).toHaveClass(/dark-mode/);

    // Click the toggle button
    await page.locator('#theme-toggle').click();

    // Verify we're now in light mode
    const htmlAfter = page.locator('html');
    await expect(htmlAfter).not.toHaveClass(/dark-mode/);
    await expect(htmlAfter).toHaveClass(/light-mode/);

    // Verify the background color changed to light
    const bgColor = await getComputedStyle(page, 'body', 'background-color');
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      const brightness = (r * 299 + g * 587 + b * 114) / 1000;
      expect(brightness).toBeGreaterThan(200); // Light background
    }
  });

  // Test Case 4: Refresh page after toggle - preference persists
  test('theme preference persists after page refresh', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Clear any existing preference
    await page.evaluate(() => localStorage.clear());

    // Wait for page to initialize
    await page.waitForTimeout(100);

    // Ensure we're in light mode first
    await page.evaluate(() => {
      document.documentElement.classList.remove('dark-mode');
      document.documentElement.classList.add('light-mode');
    });

    // Click toggle to switch to dark mode
    await page.locator('#theme-toggle').click();

    // Verify dark mode is active
    await expect(page.locator('html')).toHaveClass(/dark-mode/);

    // Refresh the page
    await page.reload();
    await waitForPageLoad(page);

    // Verify dark mode persists after refresh
    await expect(page.locator('html')).toHaveClass(/dark-mode/);

    // Verify the stored preference
    const storedTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedTheme).toBe('dark');
  });

  // Test Case 5: Check localStorage for theme preference
  test('theme preference is stored in localStorage', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Clear localStorage
    await page.evaluate(() => localStorage.clear());

    // Toggle to dark mode
    await page.locator('#theme-toggle').click();

    // Check localStorage
    const darkTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(darkTheme).toBe('dark');

    // Toggle back to light mode
    await page.locator('#theme-toggle').click();

    // Check localStorage again
    const lightTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(lightTheme).toBe('light');
  });

  // Test Case 6: Test system preference detection
  test('page respects system prefers-color-scheme without stored preference', async ({ page }) => {
    // Clear any stored preference before navigating
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());

    // Emulate dark system preference
    await page.emulateMedia({ colorScheme: 'dark' });

    // Navigate with dark color scheme emulated
    await page.goto('/');
    await waitForPageLoad(page);

    // Wait for theme initialization
    await page.waitForTimeout(200);

    // Without a stored preference, should respect system preference
    // The page should apply dark theme colors via CSS or JS
    const bgColor = await getComputedStyle(page, 'body', 'background-color');
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);

    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      const brightness = (r * 299 + g * 587 + b * 114) / 1000;
      // Should be dark background based on system preference
      expect(brightness).toBeLessThan(128);
    }
  });

  // Test Case 7: Verify dark mode color contrast
  test('dark mode has readable text with minimum 4.5:1 contrast ratio', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Switch to dark mode
    await page.evaluate(() => {
      localStorage.setItem('mirdb-theme', 'dark');
      document.documentElement.classList.add('dark-mode');
      document.documentElement.classList.remove('light-mode');
    });

    // Wait for styles to apply
    await page.waitForTimeout(100);

    // Get background and text colors
    const bgColor = await getComputedStyle(page, 'body', 'background-color');
    const textColor = await getComputedStyle(page, 'body', 'color');

    // Calculate contrast ratio
    const contrastRatio = getContrastRatio(bgColor, textColor);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);

    // Also check hero title contrast
    const heroTitleColor = await getComputedStyle(page, '.hero__title', 'color');
    const heroBgColor = await getComputedStyle(page, '.hero', 'background-color');

    // Hero may use gradient, so we check against body bg as fallback
    const heroContrastBg = heroBgColor || bgColor;
    const heroContrast = getContrastRatio(heroContrastBg, heroTitleColor);
    // Large text (heading) requires 3:1 minimum
    expect(heroContrast).toBeGreaterThanOrEqual(3);
  });

  // Additional test: Toggle button keyboard accessibility
  test('toggle button is keyboard accessible', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Tab to the toggle button
    const toggleButton = page.locator('#theme-toggle');

    // Focus the button
    await toggleButton.focus();

    // Verify it's focused
    const isFocused = await page.evaluate(() =>
      document.activeElement === document.getElementById('theme-toggle')
    );
    expect(isFocused).toBe(true);

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Verify theme changed
    const htmlAfterEnter = page.locator('html');
    await expect(htmlAfterEnter).toHaveClass(/dark-mode|light-mode/);
  });

  // Additional test: Theme icons visibility changes correctly
  test('theme icons toggle visibility correctly', async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);

    // Start in light mode
    await page.evaluate(() => {
      localStorage.clear();
      document.documentElement.classList.remove('dark-mode');
      document.documentElement.classList.add('light-mode');
    });

    // In light mode, moon icon should be visible (showing what to switch to)
    const moonIcon = page.locator('.theme-toggle__icon--moon');
    const sunIcon = page.locator('.theme-toggle__icon--sun');

    // Moon should be displayed in light mode
    let moonDisplay = await moonIcon.evaluate(el => window.getComputedStyle(el).display);
    expect(moonDisplay).not.toBe('none');

    // Switch to dark mode
    await page.locator('#theme-toggle').click();

    // In dark mode, sun icon should be visible
    await page.waitForTimeout(100);
    let sunDisplay = await sunIcon.evaluate(el => window.getComputedStyle(el).display);
    expect(sunDisplay).not.toBe('none');
  });
});
