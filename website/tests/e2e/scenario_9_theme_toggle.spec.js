// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Scenario 9: Dark/Light Theme Toggle
 * Tests for verifying the dark/light theme toggle functions correctly
 * and persists user preference.
 */

test.describe('Dark/Light Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test to ensure clean state
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.goto('/');
  });

  /**
   * Test Case 1: Theme toggle control is visible on the page
   * Input: Check theme toggle presence
   * Expected: Theme toggle control is visible on the page
   */
  test('TC1: Theme toggle control is visible on the page', async ({ page }) => {
    // Use first() since there are desktop and mobile toggle buttons
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    await expect(themeToggle).toBeVisible();

    // Verify it's within the header navigation
    const header = page.locator('header');
    await expect(header.locator('[data-testid="theme-toggle"]').first()).toBeVisible();

    // Verify it has appropriate aria attributes for accessibility
    await expect(themeToggle).toHaveAttribute('aria-label', /theme/i);
  });

  /**
   * Test Case 2: Click theme toggle in light mode
   * Input: Click theme toggle in light mode
   * Expected: Page switches to dark theme (dark background, light text)
   */
  test('TC2: Click toggle in light mode switches to dark theme', async ({ page }) => {
    // Ensure we're in light mode first
    await page.evaluate(() => {
      localStorage.setItem('theme', 'light');
      document.documentElement.classList.remove('dark');
    });
    await page.reload();

    // Verify starting in light mode
    const html = page.locator('html');
    await expect(html).not.toHaveClass(/dark/);

    // Click the theme toggle (use first() for desktop version)
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    await themeToggle.click();

    // Verify page switched to dark mode
    await expect(html).toHaveClass(/dark/);

    // Verify visual changes - dark background
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Dark mode should have dark background (gray-900 = rgb(17, 24, 39))
    expect(bgColor).not.toBe('rgb(255, 255, 255)');
  });

  /**
   * Test Case 3: Click theme toggle in dark mode
   * Input: Click theme toggle in dark mode
   * Expected: Page switches to light theme (light background, dark text)
   */
  test('TC3: Click toggle in dark mode switches to light theme', async ({ page }) => {
    // Set dark mode
    await page.evaluate(() => {
      localStorage.setItem('theme', 'dark');
      document.documentElement.classList.add('dark');
    });
    await page.reload();

    // Verify starting in dark mode
    const html = page.locator('html');
    await expect(html).toHaveClass(/dark/);

    // Click the theme toggle (use first() for desktop version)
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    await themeToggle.click();

    // Verify page switched to light mode
    await expect(html).not.toHaveClass(/dark/);

    // Verify localStorage was updated to light
    const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(storedTheme).toBe('light');

    // Verify the body has the light theme CSS variable applied
    // The CSS variable --bg-primary should be white (#ffffff) in light mode
    const bgVarValue = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--bg-primary').trim();
    });
    expect(bgVarValue).toBe('#ffffff');
  });

  /**
   * Test Case 4: Set system to dark mode and reload
   * Input: Set system to dark mode and reload
   * Expected: Page auto-detects system preference and uses dark theme
   */
  test('TC4: Auto-detects system dark mode preference on first load', async ({ page }) => {
    // Clear any stored preference
    await page.evaluate(() => localStorage.removeItem('theme'));

    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.reload();

    // Wait for page to apply the theme
    await page.waitForTimeout(100);

    // Verify page detected system preference and applied dark mode
    const html = page.locator('html');
    await expect(html).toHaveClass(/dark/);
  });

  /**
   * Test Case 5: Toggle theme, reload page
   * Input: Toggle theme, reload page
   * Expected: Theme preference persists after page reload
   */
  test('TC5: Theme preference persists after page reload', async ({ page }) => {
    // Start fresh
    await page.evaluate(() => localStorage.clear());
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload();

    // Verify starting in light mode
    const html = page.locator('html');
    await expect(html).not.toHaveClass(/dark/);

    // Toggle to dark mode (use first() for desktop version)
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    await themeToggle.click();

    // Verify dark mode is active
    await expect(html).toHaveClass(/dark/);

    // Verify localStorage was updated
    const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(storedTheme).toBe('dark');

    // Reload the page
    await page.reload();

    // Verify dark mode persisted after reload
    await expect(html).toHaveClass(/dark/);

    // Toggle back to light mode (re-locate after page reload)
    const themeToggleAfterReload = page.locator('[data-testid="theme-toggle"]').first();
    await themeToggleAfterReload.click();
    await expect(html).not.toHaveClass(/dark/);

    // Verify localStorage updated
    const storedThemeLight = await page.evaluate(() => localStorage.getItem('theme'));
    expect(storedThemeLight).toBe('light');

    // Reload and verify light mode persisted
    await page.reload();
    await expect(html).not.toHaveClass(/dark/);
  });

  /**
   * Test Case 6: Check contrast in both themes
   * Input: Check contrast in both themes
   * Expected: Text has sufficient contrast in both themes
   */
  test('TC6: Text has sufficient contrast in both themes', async ({ page }) => {
    // Helper function to calculate relative luminance
    const getContrastRatio = async (fgColor, bgColor) => {
      return page.evaluate(([fg, bg]) => {
        const parseRgb = (color) => {
          const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
          if (match) {
            return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
          }
          return { r: 0, g: 0, b: 0 };
        };

        const getLuminance = (color) => {
          const { r, g, b } = parseRgb(color);
          const [rs, gs, bs] = [r, g, b].map(c => {
            c = c / 255;
            return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
          });
          return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
        };

        const l1 = getLuminance(fg);
        const l2 = getLuminance(bg);
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }, [fgColor, bgColor]);
    };

    // Test light mode contrast
    await page.evaluate(() => {
      localStorage.setItem('theme', 'light');
      document.documentElement.classList.remove('dark');
    });
    await page.reload();

    // Get text and background colors in light mode
    const lightModeColors = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      const section = document.querySelector('section');
      if (h1 && section) {
        const textColor = window.getComputedStyle(h1).color;
        const bgColor = window.getComputedStyle(section).backgroundColor;
        return { textColor, bgColor };
      }
      return { textColor: 'rgb(0, 0, 0)', bgColor: 'rgb(255, 255, 255)' };
    });

    const lightContrast = await getContrastRatio(lightModeColors.textColor, lightModeColors.bgColor);
    // WCAG AA requires 4.5:1 for normal text
    expect(lightContrast).toBeGreaterThanOrEqual(4.5);

    // Test dark mode contrast
    await page.evaluate(() => {
      localStorage.setItem('theme', 'dark');
      document.documentElement.classList.add('dark');
    });
    await page.reload();

    // Get text and background colors in dark mode
    const darkModeColors = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      const section = document.querySelector('section');
      if (h1 && section) {
        const textColor = window.getComputedStyle(h1).color;
        const bgColor = window.getComputedStyle(section).backgroundColor;
        return { textColor, bgColor };
      }
      return { textColor: 'rgb(255, 255, 255)', bgColor: 'rgb(0, 0, 0)' };
    });

    const darkContrast = await getContrastRatio(darkModeColors.textColor, darkModeColors.bgColor);
    // WCAG AA requires 4.5:1 for normal text
    expect(darkContrast).toBeGreaterThanOrEqual(4.5);
  });

  /**
   * Additional test: Theme toggle icons change appropriately
   */
  test('Theme toggle shows appropriate icons for current mode', async ({ page }) => {
    // Test light mode - should show moon icon (to switch to dark)
    await page.evaluate(() => {
      localStorage.setItem('theme', 'light');
      document.documentElement.classList.remove('dark');
    });
    await page.reload();

    // Use the desktop toggle (first one)
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    const moonIcon = themeToggle.locator('[data-testid="moon-icon"]');
    const sunIcon = themeToggle.locator('[data-testid="sun-icon"]');

    // In light mode, moon icon should be visible (click to go dark)
    await expect(moonIcon).toBeVisible();
    await expect(sunIcon).not.toBeVisible();

    // Toggle to dark mode
    await themeToggle.click();

    // In dark mode, sun icon should be visible (click to go light)
    await expect(sunIcon).toBeVisible();
    await expect(moonIcon).not.toBeVisible();
  });

  /**
   * Additional test: User preference overrides system preference
   */
  test('User explicit preference overrides system preference', async ({ page }) => {
    // Set system to dark mode
    await page.emulateMedia({ colorScheme: 'dark' });

    // But user prefers light mode in localStorage
    await page.evaluate(() => localStorage.setItem('theme', 'light'));
    await page.reload();

    // User preference should win - page should be light
    const html = page.locator('html');
    await expect(html).not.toHaveClass(/dark/);

    // Verify the opposite: system light but user prefers dark
    await page.emulateMedia({ colorScheme: 'light' });
    await page.evaluate(() => localStorage.setItem('theme', 'dark'));
    await page.reload();

    await expect(html).toHaveClass(/dark/);
  });
});
