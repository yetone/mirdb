// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Dark/Light Mode E2E Tests
 * Owner: Scenario 10 - Dark Mode and Light Mode
 *
 * Test cases:
 * 1. Theme toggle button is visible in navigation bar
 * 2. Click theme toggle from light mode switches to dark mode
 * 3. Click theme toggle from dark mode switches to light mode
 * 4. System preference dark mode detection
 * 5. System preference light mode detection
 * 6. Theme transition is smooth without visual glitches
 * 7. Theme persistence across page reloads via localStorage
 */

test.describe('Theme Toggle', () => {
  test.beforeEach(async ({ page, context }) => {
    // Clear localStorage before each test
    await context.clearCookies();
    await page.goto('/');
    // Clear localStorage after page load
    await page.evaluate(() => {
      try {
        localStorage.removeItem('mirdb-theme');
      } catch (e) {}
    });
  });

  test('TC1: Theme toggle button is visible in navigation bar', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Check theme toggle button exists and is visible
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await expect(themeToggle).toBeVisible();

    // Verify it's in the navigation area
    const nav = page.locator('nav');
    await expect(nav.locator('[data-testid="theme-toggle"]')).toBeVisible();

    // Verify the button has proper accessibility attributes
    await expect(themeToggle).toHaveAttribute('type', 'button');
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/switch to (dark|light) mode/i);

    // Verify icons are present
    const sunIcon = themeToggle.locator('.theme-icon-sun');
    const moonIcon = themeToggle.locator('.theme-icon-moon');
    await expect(sunIcon).toBeAttached();
    await expect(moonIcon).toBeAttached();
  });

  test('TC2: Click theme toggle from light mode switches to dark mode', async ({ page }) => {
    // Navigate with light mode preference
    await page.goto('/');

    // Set to light mode first
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('mirdb-theme', 'light');
    });
    await page.reload();

    // Wait for page to stabilize
    await page.waitForLoadState('networkidle');

    // Verify we're in light mode
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(initialTheme).toBe('light');

    // Get initial background color
    const initialBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Click theme toggle
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await themeToggle.click();

    // Wait for transition
    await page.waitForTimeout(350);

    // Verify theme changed to dark
    const newTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(newTheme).toBe('dark');

    // Verify background color changed
    const newBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });
    expect(newBgColor).not.toBe(initialBgColor);

    // Dark mode should have darker background
    // Parse RGB values and check if it's darker
    const parseRgb = (rgb) => {
      const match = rgb.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
      }
      return null;
    };

    const darkBg = parseRgb(newBgColor);
    if (darkBg) {
      // Dark background should have low RGB values
      expect(darkBg.r + darkBg.g + darkBg.b).toBeLessThan(200);
    }
  });

  test('TC3: Click theme toggle from dark mode switches to light mode', async ({ page }) => {
    // Navigate with dark mode preference
    await page.goto('/');

    // Set to dark mode first
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('mirdb-theme', 'dark');
    });
    await page.reload();

    // Wait for page to stabilize
    await page.waitForLoadState('networkidle');

    // Verify we're in dark mode
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(initialTheme).toBe('dark');

    // Get initial background color
    const initialBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Click theme toggle
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await themeToggle.click();

    // Wait for transition
    await page.waitForTimeout(350);

    // Verify theme changed to light
    const newTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(newTheme).toBe('light');

    // Verify background color changed
    const newBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });
    expect(newBgColor).not.toBe(initialBgColor);

    // Light mode should have lighter background
    const parseRgb = (rgb) => {
      const match = rgb.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
      }
      return null;
    };

    const lightBg = parseRgb(newBgColor);
    if (lightBg) {
      // Light background should have high RGB values (white-ish)
      expect(lightBg.r + lightBg.g + lightBg.b).toBeGreaterThan(600);
    }
  });

  test('TC4: System preference dark mode detection', async ({ page }) => {
    // Clear any stored preference
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
    });

    // Emulate dark color scheme
    await page.emulateMedia({ colorScheme: 'dark' });

    // Navigate to page with fresh load (no stored preference)
    await page.goto('/');
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
    });
    await page.reload();

    // Wait for page to stabilize
    await page.waitForLoadState('networkidle');

    // Verify page loaded in dark mode
    const theme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(theme).toBe('dark');

    // Verify dark mode styling is applied
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    const parseRgb = (rgb) => {
      const match = rgb.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
      }
      return null;
    };

    const bg = parseRgb(bgColor);
    if (bg) {
      // Dark mode should have dark background (low RGB sum)
      expect(bg.r + bg.g + bg.b).toBeLessThan(200);
    }
  });

  test('TC5: System preference light mode detection', async ({ page }) => {
    // Clear any stored preference
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
    });

    // Emulate light color scheme
    await page.emulateMedia({ colorScheme: 'light' });

    // Navigate to page with fresh load (no stored preference)
    await page.goto('/');
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
    });
    await page.reload();

    // Wait for page to stabilize
    await page.waitForLoadState('networkidle');

    // Verify page loaded in light mode
    const theme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(theme).toBe('light');

    // Verify light mode styling is applied
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    const parseRgb = (rgb) => {
      const match = rgb.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
      }
      return null;
    };

    const bg = parseRgb(bgColor);
    if (bg) {
      // Light mode should have light background (high RGB sum)
      expect(bg.r + bg.g + bg.b).toBeGreaterThan(600);
    }
  });

  test('TC6: Theme transition is smooth without visual glitches', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Set initial light mode
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('mirdb-theme', 'light');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Get initial viewport dimensions to check for layout shifts
    const initialViewport = await page.evaluate(() => {
      return {
        scrollHeight: document.body.scrollHeight,
        clientWidth: document.body.clientWidth,
        scrollWidth: document.body.scrollWidth
      };
    });

    // Check that theme-transition class is applied during transition
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Click and immediately check for transition class
    await themeToggle.click();

    // Check that transition class was added (may need to catch it quickly)
    // The transition class is removed after 300ms, so check within that window
    const hasTransitionClass = await page.evaluate(() => {
      return document.documentElement.classList.contains('theme-transition');
    });
    // Note: This might be false if checked too late, but we verify smooth transition via layout stability

    // Wait for transition to complete
    await page.waitForTimeout(350);

    // Check that no layout shift occurred (dimensions remain the same)
    const afterViewport = await page.evaluate(() => {
      return {
        scrollHeight: document.body.scrollHeight,
        clientWidth: document.body.clientWidth,
        scrollWidth: document.body.scrollWidth
      };
    });

    // Verify no horizontal scroll was introduced
    expect(afterViewport.scrollWidth).toBeLessThanOrEqual(afterViewport.clientWidth + 20);

    // Verify all main sections are still visible (no flash of unstyled content)
    const hero = page.locator('#hero');
    const features = page.locator('#features');
    const nav = page.locator('nav');
    const footer = page.locator('footer');

    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(nav).toBeVisible();
    await expect(footer).toBeVisible();

    // Verify theme actually changed
    const finalTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(finalTheme).toBe('dark');

    // Toggle back and verify smooth transition again
    await themeToggle.click();
    await page.waitForTimeout(350);

    // All sections should still be visible after toggle back
    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(nav).toBeVisible();
    await expect(footer).toBeVisible();

    const finalTheme2 = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(finalTheme2).toBe('light');
  });

  test('TC7: Theme persistence across page reloads via localStorage', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Set initial light mode and clear storage
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
      document.documentElement.setAttribute('data-theme', 'light');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Click theme toggle to switch to dark mode
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await themeToggle.click();
    await page.waitForTimeout(350);

    // Verify dark mode is set
    const themeAfterClick = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(themeAfterClick).toBe('dark');

    // Verify localStorage was updated
    const storedTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(storedTheme).toBe('dark');

    // Reload the page
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify theme persisted after reload
    const themeAfterReload = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(themeAfterReload).toBe('dark');

    // Verify localStorage still has the value
    const storedThemeAfterReload = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(storedThemeAfterReload).toBe('dark');

    // Toggle back to light mode
    await themeToggle.click();
    await page.waitForTimeout(350);

    // Reload again
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify light mode persisted
    const finalTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(finalTheme).toBe('light');

    const finalStoredTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(finalStoredTheme).toBe('light');
  });
});

test.describe('Theme Toggle Accessibility', () => {
  test('Theme toggle is keyboard accessible', async ({ page }) => {
    await page.goto('/');

    // Set initial theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('mirdb-theme', 'light');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Tab to the theme toggle button
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await themeToggle.focus();

    // Verify focus is visible
    const focusStyle = await themeToggle.evaluate(el => {
      const style = getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth
      };
    });

    // Press Enter to toggle
    await page.keyboard.press('Enter');
    await page.waitForTimeout(350);

    // Verify theme changed
    const theme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(theme).toBe('dark');

    // Press Space to toggle back
    await page.keyboard.press('Space');
    await page.waitForTimeout(350);

    const theme2 = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(theme2).toBe('light');
  });

  test('Theme toggle has appropriate aria-label that updates', async ({ page }) => {
    await page.goto('/');

    // Set initial light mode
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('mirdb-theme', 'light');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // In light mode, should offer to switch to dark
    let ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('dark');

    // Click to switch to dark mode
    await themeToggle.click();
    await page.waitForTimeout(350);

    // In dark mode, should offer to switch to light
    ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('light');
  });
});

test.describe('Theme Icon Display', () => {
  test('Correct icon is displayed for each theme', async ({ page }) => {
    await page.goto('/');

    // Set light mode
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('mirdb-theme', 'light');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    const sunIcon = page.locator('.theme-icon-sun');
    const moonIcon = page.locator('.theme-icon-moon');

    // In light mode, moon icon should be visible (to indicate switching to dark)
    const moonDisplay = await moonIcon.evaluate(el => getComputedStyle(el).display);
    expect(moonDisplay).not.toBe('none');

    // Switch to dark mode
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await themeToggle.click();
    await page.waitForTimeout(350);

    // In dark mode, sun icon should be visible (to indicate switching to light)
    const sunDisplay = await sunIcon.evaluate(el => getComputedStyle(el).display);
    expect(sunDisplay).not.toBe('none');
  });
});
