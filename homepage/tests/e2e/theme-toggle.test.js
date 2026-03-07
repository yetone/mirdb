/**
 * Theme Toggle E2E Tests
 * Owner: Scenario 10 - Dark/Light Theme Toggle
 *
 * Tests:
 * - Theme toggle button exists
 * - Toggle changes colors
 * - Theme persists after reload
 * - Respects system preference
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '..', '..', 'index.html');

test.describe('Theme Toggle E2E Tests (Scenario 10)', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto(`file://${indexPath}`);
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  /**
   * Test Case 1: Theme toggle button exists in DOM
   */
  test('Test Case 1: Theme toggle button exists in DOM', async ({ page }) => {
    // Verify theme toggle button exists
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify it's a button element
    const tagName = await themeToggle.evaluate(el => el.tagName);
    expect(tagName).toBe('BUTTON');

    // Verify button type attribute
    const buttonType = await themeToggle.getAttribute('type');
    expect(buttonType).toBe('button');
  });

  /**
   * Test Case 2: Click theme toggle button - background and text colors change
   */
  test('Test Case 2: Click theme toggle button changes colors', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial background color (light theme)
    const initialBgColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-background').trim();
    });

    // Get initial text color
    const initialTextColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-text').trim();
    });

    // Click theme toggle
    await themeToggle.click();

    // Get new background color after toggle
    const newBgColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-background').trim();
    });

    // Get new text color after toggle
    const newTextColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-text').trim();
    });

    // Colors should be different after toggle
    expect(newBgColor).not.toBe(initialBgColor);
    expect(newTextColor).not.toBe(initialTextColor);
  });

  /**
   * Test Case 3: CSS custom properties update for new theme
   */
  test('Test Case 3: CSS custom properties update for new theme', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');

    // Get light theme CSS variables
    const lightThemeVars = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return {
        background: styles.getPropertyValue('--color-background').trim(),
        text: styles.getPropertyValue('--color-text').trim(),
        primary: styles.getPropertyValue('--color-primary').trim(),
        surface: styles.getPropertyValue('--color-surface').trim()
      };
    });

    // Verify light theme values
    expect(lightThemeVars.background).toBe('#ffffff');
    expect(lightThemeVars.text).toBe('#1a1a2e');

    // Click to switch to dark theme
    await themeToggle.click();

    // Verify data-theme attribute changed
    const dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('dark');

    // Get dark theme CSS variables
    const darkThemeVars = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return {
        background: styles.getPropertyValue('--color-background').trim(),
        text: styles.getPropertyValue('--color-text').trim(),
        primary: styles.getPropertyValue('--color-primary').trim(),
        surface: styles.getPropertyValue('--color-surface').trim()
      };
    });

    // Verify dark theme values
    expect(darkThemeVars.background).toBe('#0f0f23');
    expect(darkThemeVars.text).toBe('#f8f9fa');

    // Click again to switch back to light theme
    await themeToggle.click();

    // Verify data-theme changed back
    const dataThemeAfter = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataThemeAfter).toBe('light');
  });

  /**
   * Test Case 4: Theme preference persists in localStorage after reload
   */
  test('Test Case 4: Theme preference persists in localStorage', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');

    // Toggle to dark theme
    await themeToggle.click();

    // Verify localStorage was updated
    const storedTheme = await page.evaluate(() => {
      return localStorage.getItem('theme');
    });
    expect(storedTheme).toBe('dark');

    // Reload the page
    await page.reload();

    // Verify theme persists after reload
    const dataThemeAfterReload = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataThemeAfterReload).toBe('dark');

    // Verify CSS variables are still dark theme
    const bgColorAfterReload = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-background').trim();
    });
    expect(bgColorAfterReload).toBe('#0f0f23');

    // Toggle back to light theme
    await page.locator('.theme-toggle').click();

    // Verify localStorage updated
    const storedThemeLight = await page.evaluate(() => {
      return localStorage.getItem('theme');
    });
    expect(storedThemeLight).toBe('light');

    // Reload and verify light theme persists
    await page.reload();

    const dataThemeLightReload = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataThemeLightReload).toBe('light');
  });

  /**
   * Test Case 5: Toggle has aria-label and keyboard accessibility
   */
  test('Test Case 5: Theme toggle accessibility', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify aria-label exists
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('theme');

    // Test keyboard accessibility - focus the button
    await themeToggle.focus();

    // Verify the button is focused
    const isFocused = await themeToggle.evaluate(el => document.activeElement === el);
    expect(isFocused).toBe(true);

    // Test keyboard activation (Enter key)
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme') || 'light';
    });

    await page.keyboard.press('Enter');

    const themeAfterEnter = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Theme should have toggled
    expect(themeAfterEnter).not.toBe(initialTheme);

    // Test keyboard activation (Space key)
    await page.keyboard.press('Space');

    const themeAfterSpace = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Theme should have toggled back
    expect(themeAfterSpace).toBe(initialTheme);
  });

  /**
   * Additional test: Theme toggle icon changes
   */
  test('Theme toggle has icon', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Check for SVG icon inside the toggle
    const icon = themeToggle.locator('svg');
    await expect(icon).toBeVisible();
  });

  /**
   * Additional test: System preference is respected on initial load
   */
  test('System color scheme preference is respected when no localStorage', async ({ page, context }) => {
    // Clear localStorage
    await page.evaluate(() => localStorage.clear());

    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.reload();

    // Check that dark theme is applied
    const themeWithDarkPreference = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(themeWithDarkPreference).toBe('dark');

    // Clear localStorage again
    await page.evaluate(() => localStorage.clear());

    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload();

    // Check that light theme is applied
    const themeWithLightPreference = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(themeWithLightPreference).toBe('light');
  });

  /**
   * Additional test: localStorage preference overrides system preference
   */
  test('localStorage preference overrides system preference', async ({ page }) => {
    // Set light theme in localStorage
    await page.evaluate(() => {
      localStorage.setItem('theme', 'light');
    });

    // Emulate dark system preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.reload();

    // Theme should still be light (from localStorage)
    const theme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(theme).toBe('light');
  });
});
