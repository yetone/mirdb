import { test, expect } from '@playwright/test';

/**
 * Dark and Light Mode Theme Tests
 * Owner: Scenario 9 - Dark and Light Mode Theme
 *
 * Tests cover:
 * - Theme toggle button existence and accessibility
 * - Light mode appearance and colors
 * - Dark mode appearance and colors
 * - Theme switching functionality
 * - Theme persistence in localStorage
 * - WCAG AA contrast compliance
 */

// Helper function to calculate relative luminance
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

// Helper function to calculate contrast ratio
function getContrastRatio(rgb1: string, rgb2: string): number {
  const parseRgb = (rgb: string): [number, number, number] => {
    const match = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (match) {
      return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
    }
    return [0, 0, 0];
  };

  const [r1, g1, b1] = parseRgb(rgb1);
  const [r2, g2, b2] = parseRgb(rgb2);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Dark and Light Mode Theme', () => {
  test.beforeEach(async ({ page, context }) => {
    // Clear localStorage before each test
    await context.clearCookies();
    await page.goto('/');
    // Clear localStorage after navigation
    await page.evaluate(() => {
      localStorage.clear();
    });
    // Reload to apply clean state
    await page.reload();
  });

  test('TC1: Theme toggle button exists in header area', async ({ page }) => {
    // Query header for theme toggle button
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await expect(themeToggle).toBeVisible();

    // Verify it's within the header
    const toggleInHeader = header.locator('[data-testid="theme-toggle"]');
    await expect(toggleInHeader).toBeVisible();

    // Verify button has accessible label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toMatch(/switch to (dark|light) mode/);
  });

  test('TC2: Page loads in light mode or respects system preference', async ({ page }) => {
    // Check initial theme state
    const html = page.locator('html');

    // Get the data-theme attribute
    const dataTheme = await html.getAttribute('data-theme');

    // Should either be 'light' explicitly or respect system (which defaults to light)
    // The page should have a readable theme set
    expect(dataTheme).toMatch(/^(light|dark)$/);

    // Get background color
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Background should be a valid color
    expect(bgColor).toBeTruthy();
    expect(bgColor).not.toBe('');
  });

  test('TC3: Click theme toggle button switches between light and dark mode', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Get initial theme
    const initialTheme = await html.getAttribute('data-theme');

    // Click toggle
    await themeToggle.click();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get new theme
    const newTheme = await html.getAttribute('data-theme');

    // Theme should have changed
    expect(newTheme).not.toBe(initialTheme);

    // Verify it's the opposite theme
    if (initialTheme === 'light') {
      expect(newTheme).toBe('dark');
    } else {
      expect(newTheme).toBe('light');
    }

    // Click again to toggle back
    await themeToggle.click();
    await page.waitForTimeout(300);

    const finalTheme = await html.getAttribute('data-theme');
    expect(finalTheme).toBe(initialTheme);
  });

  test('TC4: Dark mode uses dark background color', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in dark mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Verify dark mode background
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Parse RGB values
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Dark background should have low RGB values (dark colors)
      // #1a1a2e = rgb(26, 26, 46)
      expect(r).toBeLessThan(100);
      expect(g).toBeLessThan(100);
      expect(b).toBeLessThan(100);
    }
  });

  test('TC5: Dark mode text uses light color for readability', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in dark mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Get text color
    const textColor = await page.evaluate(() => {
      return getComputedStyle(document.body).color;
    });

    // Parse RGB values
    const rgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Light text should have high RGB values (light colors)
      // #f0f0f5 = rgb(240, 240, 245)
      expect(r).toBeGreaterThan(150);
      expect(g).toBeGreaterThan(150);
      expect(b).toBeGreaterThan(150);
    }
  });

  test('TC6: Light mode uses light background color', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in light mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'light') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Verify light mode background
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Parse RGB values
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Light background should have high RGB values (light colors)
      // #ffffff = rgb(255, 255, 255)
      expect(r).toBeGreaterThan(200);
      expect(g).toBeGreaterThan(200);
      expect(b).toBeGreaterThan(200);
    }
  });

  test('TC7: Text contrast ratio in dark mode meets WCAG AA standard (4.5:1 minimum)', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in dark mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Get background and text colors
    const colors = await page.evaluate(() => {
      const bodyStyles = getComputedStyle(document.body);
      return {
        bg: bodyStyles.backgroundColor,
        text: bodyStyles.color
      };
    });

    const contrastRatio = getContrastRatio(colors.text, colors.bg);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('TC8: Text contrast ratio in light mode meets WCAG AA standard (4.5:1 minimum)', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in light mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'light') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Get background and text colors
    const colors = await page.evaluate(() => {
      const bodyStyles = getComputedStyle(document.body);
      return {
        bg: bodyStyles.backgroundColor,
        text: bodyStyles.color
      };
    });

    const contrastRatio = getContrastRatio(colors.text, colors.bg);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('TC9: Theme preference persists after page reload', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Get initial theme
    const initialTheme = await html.getAttribute('data-theme');

    // Toggle to opposite theme
    await themeToggle.click();
    await page.waitForTimeout(300);

    const toggledTheme = await html.getAttribute('data-theme');
    expect(toggledTheme).not.toBe(initialTheme);

    // Verify localStorage has the theme saved
    const savedTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(savedTheme).toBe(toggledTheme);

    // Reload the page
    await page.reload();
    await page.waitForTimeout(300);

    // Verify theme persists
    const persistedTheme = await html.getAttribute('data-theme');
    expect(persistedTheme).toBe(toggledTheme);
  });

  test('Theme toggle button is keyboard accessible', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Get initial theme
    const initialTheme = await html.getAttribute('data-theme');

    // Focus the toggle button using keyboard
    await themeToggle.focus();

    // Verify button is focused
    await expect(themeToggle).toBeFocused();

    // Press Enter to toggle
    await page.keyboard.press('Enter');
    await page.waitForTimeout(300);

    // Verify theme changed
    const newTheme = await html.getAttribute('data-theme');
    expect(newTheme).not.toBe(initialTheme);
  });

  test('Theme toggle button has proper focus styles', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Focus the button
    await themeToggle.focus();

    // Get the outline style
    const outlineStyle = await themeToggle.evaluate((el) => {
      const styles = getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle
      };
    });

    // Should have visible focus indicator
    // Either outline or box-shadow should be present
    const hasVisibleFocus =
      (outlineStyle.outlineWidth !== '0px' && outlineStyle.outlineStyle !== 'none') ||
      outlineStyle.outline !== 'none';

    expect(hasVisibleFocus).toBe(true);
  });

  test('Theme toggle shows correct icon for current theme', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');
    const sunIcon = page.locator('.theme-toggle__sun');
    const moonIcon = page.locator('.theme-toggle__moon');

    // In light mode, sun should be visible
    let currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'light') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Sun icon visible in light mode
    await expect(sunIcon).toBeVisible();
    const moonDisplay = await moonIcon.evaluate((el) => getComputedStyle(el).display);
    expect(moonDisplay).toBe('none');

    // Toggle to dark mode
    await themeToggle.click();
    await page.waitForTimeout(300);

    // Moon icon visible in dark mode
    await expect(moonIcon).toBeVisible();
    const sunDisplay = await sunIcon.evaluate((el) => getComputedStyle(el).display);
    expect(sunDisplay).toBe('none');
  });

  test('All major sections have appropriate colors in dark mode', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in dark mode
    const currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'dark') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Check header background
    const headerBg = await page.locator('header').evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(headerBg).not.toBe('rgb(255, 255, 255)');

    // Check hero section
    const heroBg = await page.locator('#hero').evaluate((el) =>
      getComputedStyle(el).background
    );
    expect(heroBg).toBeTruthy();

    // Check features section
    const featuresBg = await page.locator('#features').evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(featuresBg).not.toBe('rgb(255, 255, 255)');

    // Check footer
    const footerBg = await page.locator('footer').evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(footerBg).toBeTruthy();
  });

  test('aria-label updates when theme changes', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Ensure we're in light mode
    let currentTheme = await html.getAttribute('data-theme');
    if (currentTheme !== 'light') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // In light mode, should say "Switch to dark mode"
    let ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('dark');

    // Toggle to dark mode
    await themeToggle.click();
    await page.waitForTimeout(300);

    // In dark mode, should say "Switch to light mode"
    ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('light');
  });
});
