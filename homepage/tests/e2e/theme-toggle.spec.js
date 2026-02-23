/**
 * Theme Toggle E2E Tests
 * Owner: Scenario 8 - Theme Toggle
 *
 * Tests:
 * - Theme toggle button exists in header
 * - Initial theme state is indicated
 * - Click theme toggle when in light mode changes to dark
 * - Click theme toggle when in dark mode changes to light
 * - localStorage contains theme preference after change
 * - Theme persists after page reload
 */

const { test, expect } = require('@playwright/test');

test.describe('Theme Toggle Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('http://localhost:8080');
    await page.evaluate(() => localStorage.clear());
    // Reload to get fresh state
    await page.reload();
    await page.waitForLoadState('domcontentloaded');
  });

  test('Theme toggle button exists in header', async ({ page }) => {
    // Test Case 1: Query for theme toggle element
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify it's a button
    await expect(themeToggle).toHaveAttribute('type', 'button');

    // Verify it has accessible aria-label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/switch to (dark|light) theme/i);
  });

  test('Initial theme state is indicated by data-theme attribute', async ({ page }) => {
    // Test Case 2: Get initial theme state
    const htmlElement = page.locator('html');

    // Should have data-theme attribute
    const dataTheme = await htmlElement.getAttribute('data-theme');
    expect(dataTheme).toBeTruthy();
    expect(['light', 'dark']).toContain(dataTheme);
  });

  test('Click theme toggle when in light mode changes to dark mode', async ({ page }) => {
    // Test Case 3: Click theme toggle when in light mode

    // First ensure we're in light mode
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
    });
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Set to light mode explicitly
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('light');
      }
    });

    // Verify starting in light mode
    let dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('light');

    // Click the theme toggle
    await page.click('.theme-toggle');

    // Wait for theme change
    await page.waitForTimeout(100);

    // Verify changed to dark mode
    dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('dark');
  });

  test('Click theme toggle when in dark mode changes to light mode', async ({ page }) => {
    // Test Case 4: Click theme toggle when in dark mode

    // Set to dark mode first
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('dark');
      }
    });

    // Verify starting in dark mode
    let dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('dark');

    // Click the theme toggle
    await page.click('.theme-toggle');

    // Wait for theme change
    await page.waitForTimeout(100);

    // Verify changed to light mode
    dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('light');
  });

  test('localStorage contains theme preference after theme change', async ({ page }) => {
    // Test Case 5: Check localStorage after theme change

    // Start fresh
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Click the theme toggle to change theme
    await page.click('.theme-toggle');
    await page.waitForTimeout(100);

    // Check localStorage
    const storedTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });

    expect(storedTheme).toBeTruthy();
    expect(['light', 'dark']).toContain(storedTheme);
  });

  test('Theme persists after page reload (dark mode)', async ({ page }) => {
    // Test Case 6: Reload page after setting dark mode

    // Set dark mode
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('dark');
      }
    });

    // Verify dark mode is set
    let dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('dark');

    // Reload the page
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify theme persisted
    dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('dark');
  });

  test('Theme persists after page reload (light mode)', async ({ page }) => {
    // Variant of Test Case 6 for light mode

    // Set light mode
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('light');
      }
    });

    // Verify light mode is set
    let dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('light');

    // Reload the page
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify theme persisted
    dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('light');
  });

  test('Theme toggle button has correct icon for current theme', async ({ page }) => {
    // Verify icons toggle correctly based on theme

    // Set light mode - should show moon icon
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('light');
      }
    });
    await page.waitForTimeout(100);

    let moonIconVisible = await page.locator('.theme-toggle__icon--moon').isVisible();
    let sunIconVisible = await page.locator('.theme-toggle__icon--sun').isVisible();

    expect(moonIconVisible).toBe(true);
    expect(sunIconVisible).toBe(false);

    // Set dark mode - should show sun icon
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('dark');
      }
    });
    await page.waitForTimeout(100);

    moonIconVisible = await page.locator('.theme-toggle__icon--moon').isVisible();
    sunIconVisible = await page.locator('.theme-toggle__icon--sun').isVisible();

    expect(moonIconVisible).toBe(false);
    expect(sunIconVisible).toBe(true);
  });

  test('Theme toggle updates aria-label on change', async ({ page }) => {
    // Verify accessibility - aria-label updates

    // Set light mode
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('light');
      }
    });
    await page.waitForTimeout(100);

    let ariaLabel = await page.locator('.theme-toggle').getAttribute('aria-label');
    expect(ariaLabel).toMatch(/switch to dark theme/i);

    // Set dark mode
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('dark');
      }
    });
    await page.waitForTimeout(100);

    ariaLabel = await page.locator('.theme-toggle').getAttribute('aria-label');
    expect(ariaLabel).toMatch(/switch to light theme/i);
  });

  test('Background color changes with theme', async ({ page }) => {
    // Verify visual change by checking computed styles

    // Set light mode
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('light');
      }
    });
    await page.waitForTimeout(100);

    const lightBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Set dark mode
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('dark');
      }
    });
    await page.waitForTimeout(100);

    const darkBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Colors should be different
    expect(lightBgColor).not.toBe(darkBgColor);
  });
});

test.describe('Theme Toggle - System Preference Detection', () => {
  test('Initializes to dark mode when system prefers dark (no localStorage)', async ({ page }) => {
    // Test Case 7: initTheme() with system prefers-color-scheme: dark

    // Clear localStorage
    await page.goto('http://localhost:8080');
    await page.evaluate(() => localStorage.clear());

    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });

    // Reload page to trigger initTheme
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Should be dark mode
    const dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('dark');
  });

  test('Initializes to light mode when system prefers light (no localStorage)', async ({ page }) => {
    // Test Case 8: initTheme() with system prefers-color-scheme: light

    // Clear localStorage
    await page.goto('http://localhost:8080');
    await page.evaluate(() => localStorage.clear());

    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });

    // Reload page to trigger initTheme
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Should be light mode
    const dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('light');
  });

  test('Stored preference overrides system preference', async ({ page }) => {
    // Verify localStorage takes precedence over system preference

    // Set localStorage to light
    await page.goto('http://localhost:8080');
    await page.evaluate(() => localStorage.setItem('mirdb-theme', 'light'));

    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });

    // Reload page
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Should be light mode (localStorage wins)
    const dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('light');
  });
});

test.describe('Theme Toggle - toggleTheme() Function', () => {
  test('toggleTheme() returns new theme and updates DOM + localStorage', async ({ page }) => {
    // Test Case 9: toggleTheme() function call

    await page.goto('http://localhost:8080');
    await page.waitForLoadState('domcontentloaded');

    // Set initial state to light
    await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        MirDBTheme.setTheme('light');
      }
    });

    // Call toggleTheme and check return value
    const result = await page.evaluate(() => {
      if (typeof MirDBTheme !== 'undefined') {
        return MirDBTheme.toggleTheme();
      }
      return null;
    });

    expect(result).toBe('dark');

    // Verify DOM updated
    const dataTheme = await page.locator('html').getAttribute('data-theme');
    expect(dataTheme).toBe('dark');

    // Verify localStorage updated
    const storedTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(storedTheme).toBe('dark');
  });
});
