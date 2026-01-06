// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Dark Mode Support Tests (NFR-6)
 * Tests verify the optional dark mode functionality including:
 * - Theme toggle button existence and accessibility
 * - Theme switching between light and dark modes
 * - Dark mode color contrast compliance (4.5:1 ratio)
 * - System preference detection (prefers-color-scheme)
 * - Theme persistence across page reloads
 */

test.describe('Dark Mode Support (NFR-6)', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test to ensure clean state
    await page.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });
    await page.goto('/');
  });

  /**
   * Test Case 1: Theme Toggle Button Exists
   * Verify dark mode toggle button exists and is accessible
   */
  test('TC1: Theme toggle button exists and is accessible', async ({ page }) => {
    // Check that theme toggle button exists
    const themeToggle = page.locator('[data-testid="theme-toggle"]');
    await expect(themeToggle, 'Theme toggle button should exist').toBeVisible();

    // Check accessibility attributes
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel, 'Theme toggle should have aria-label').toBeTruthy();
    expect(ariaLabel).toContain('dark mode');

    // Check it has a title for tooltip
    const title = await themeToggle.getAttribute('title');
    expect(title, 'Theme toggle should have title attribute').toBeTruthy();

    // Verify it's a button element
    const tagName = await themeToggle.evaluate(el => el.tagName.toLowerCase());
    expect(tagName, 'Theme toggle should be a button').toBe('button');

    // Check it's keyboard focusable
    await themeToggle.focus();
    const isFocused = await themeToggle.evaluate(el => el === document.activeElement);
    expect(isFocused, 'Theme toggle should be focusable').toBeTruthy();
  });

  /**
   * Test Case 2: Theme Switching
   * Verify clicking theme toggle switches between light and dark color schemes
   */
  test('TC2: Page switches between light and dark color schemes on toggle', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Get initial background color (should be light by default without system preference)
    const getBackgroundColor = async () => {
      return await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });
    };

    const getTextColor = async () => {
      return await page.evaluate(() => {
        return getComputedStyle(document.body).color;
      });
    };

    const initialBgColor = await getBackgroundColor();
    const initialTextColor = await getTextColor();

    // Click theme toggle
    await themeToggle.click();

    // Wait for theme to change
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') !== null;
    });

    // Get new colors
    const newBgColor = await getBackgroundColor();
    const newTextColor = await getTextColor();

    // Colors should have changed
    expect(newBgColor).not.toBe(initialBgColor);
    expect(newTextColor).not.toBe(initialTextColor);

    // Verify data-theme attribute is set
    const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(['light', 'dark']).toContain(theme);

    // Toggle again to switch back
    await themeToggle.click();

    const toggledBackBgColor = await getBackgroundColor();
    const toggledBackTextColor = await getTextColor();

    // Verify colors toggle back
    expect(toggledBackBgColor).toBe(initialBgColor);
    expect(toggledBackTextColor).toBe(initialTextColor);
  });

  /**
   * Test Case 3: Dark Mode Contrast Compliance
   * Verify dark mode maintains 4.5:1 contrast ratio for WCAG AA compliance
   */
  test('TC3: Dark mode maintains 4.5:1 contrast ratio', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Switch to dark mode
    await themeToggle.click();

    // Wait for dark mode to be applied
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'dark';
    });

    // Helper function to calculate relative luminance
    const getLuminance = (r, g, b) => {
      const [rs, gs, bs] = [r, g, b].map(c => {
        const sRGB = c / 255;
        return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    };

    // Helper function to calculate contrast ratio
    const getContrastRatio = (l1, l2) => {
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    };

    // Helper function to parse RGB color string
    const parseRgb = (color) => {
      const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (match) {
        return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
      }
      return null;
    };

    // Check contrast for key text elements in dark mode
    const contrastResults = await page.evaluate(() => {
      const results = [];

      const textElements = [
        { selector: '.hero-title', name: 'Hero title' },
        { selector: '.hero-tagline', name: 'Hero tagline' },
        { selector: '.section-description', name: 'Section description' },
        { selector: 'h2', name: 'Section headings' },
        { selector: '.nav-brand', name: 'Navigation brand' }
      ];

      for (const { selector, name } of textElements) {
        const element = document.querySelector(selector);
        if (element) {
          const computedStyle = window.getComputedStyle(element);
          const color = computedStyle.color;
          const backgroundColor = computedStyle.backgroundColor;

          // Get parent background if element has transparent background
          let bgColor = backgroundColor;
          if (backgroundColor === 'rgba(0, 0, 0, 0)' || backgroundColor === 'transparent') {
            let parent = element.parentElement;
            while (parent) {
              const parentStyle = window.getComputedStyle(parent);
              if (parentStyle.backgroundColor !== 'rgba(0, 0, 0, 0)' && parentStyle.backgroundColor !== 'transparent') {
                bgColor = parentStyle.backgroundColor;
                break;
              }
              parent = parent.parentElement;
            }
            // Default to dark background for dark mode
            if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
              bgColor = 'rgb(15, 23, 42)'; // --color-background in dark mode
            }
          }

          results.push({
            name,
            selector,
            textColor: color,
            backgroundColor: bgColor
          });
        }
      }
      return results;
    });

    // Verify contrast for each element
    for (const result of contrastResults) {
      const textRgb = parseRgb(result.textColor);
      const bgRgb = parseRgb(result.backgroundColor);

      if (textRgb && bgRgb) {
        const textLuminance = getLuminance(textRgb.r, textRgb.g, textRgb.b);
        const bgLuminance = getLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG 2.1 AA requires 4.5:1 for normal text
        expect(
          contrastRatio,
          `${result.name} in dark mode should have contrast ratio >= 4.5:1 (actual: ${contrastRatio.toFixed(2)}:1)`
        ).toBeGreaterThanOrEqual(4.5);
      }
    }
  });

  /**
   * Test Case 4: System Preference Detection
   * Verify page respects prefers-color-scheme system preference
   */
  test('TC4: Page respects system dark mode preference (prefers-color-scheme)', async ({ browser }) => {
    // Create a context with dark color scheme preference
    const darkContext = await browser.newContext({
      colorScheme: 'dark'
    });

    const darkPage = await darkContext.newPage();

    // Clear any stored theme preference
    await darkPage.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await darkPage.goto('/');

    // Wait for page to fully load
    await darkPage.waitForLoadState('domcontentloaded');

    // Check that dark mode styles are applied based on system preference
    const backgroundColor = await darkPage.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Dark mode background should be dark (rgb(15, 23, 42) = #0f172a)
    const bgRgb = backgroundColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (bgRgb) {
      const r = parseInt(bgRgb[1]);
      const g = parseInt(bgRgb[2]);
      const b = parseInt(bgRgb[3]);

      // In dark mode, background should be significantly darker
      expect(r + g + b, 'Background should be dark when system prefers dark mode').toBeLessThan(150);
    }

    await darkContext.close();

    // Test with light color scheme preference
    const lightContext = await browser.newContext({
      colorScheme: 'light'
    });

    const lightPage = await lightContext.newPage();

    await lightPage.addInitScript(() => {
      localStorage.removeItem('mirdb-theme');
    });

    await lightPage.goto('/');
    await lightPage.waitForLoadState('domcontentloaded');

    const lightBackgroundColor = await lightPage.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Light mode background should be light (rgb(255, 255, 255) = #ffffff)
    const lightBgRgb = lightBackgroundColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (lightBgRgb) {
      const r = parseInt(lightBgRgb[1]);
      const g = parseInt(lightBgRgb[2]);
      const b = parseInt(lightBgRgb[3]);

      // In light mode, background should be significantly lighter
      expect(r + g + b, 'Background should be light when system prefers light mode').toBeGreaterThan(600);
    }

    await lightContext.close();
  });

  /**
   * Test Case 5: Theme Persistence
   * Verify theme preference is persisted across page reloads
   */
  test('TC5: Theme preference is persisted across page reloads', async ({ browser }) => {
    // Create a fresh context without the init script that clears localStorage
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Toggle to dark mode
    await themeToggle.click();

    // Wait for theme to be set
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'dark';
    });

    // Verify localStorage has the theme stored
    const storedThemeBefore = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
    expect(storedThemeBefore, 'Theme should be stored in localStorage').toBe('dark');

    // Reload the page (without the init script clearing storage)
    await page.reload();

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Verify theme is still dark after reload
    const themeAfterReload = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(themeAfterReload, 'Theme should persist as dark after reload').toBe('dark');

    // Verify background is still dark
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    const bgRgb = bgColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (bgRgb) {
      const r = parseInt(bgRgb[1]);
      const g = parseInt(bgRgb[2]);
      const b = parseInt(bgRgb[3]);
      expect(r + g + b, 'Background should remain dark after reload').toBeLessThan(150);
    }

    // Toggle back to light and verify persistence
    const themeToggleAfterReload = page.locator('[data-testid="theme-toggle"]');
    await themeToggleAfterReload.click();

    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'light';
    });

    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    const themeAfterSecondReload = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    expect(themeAfterSecondReload, 'Light theme should persist after reload').toBe('light');

    await context.close();
  });

  /**
   * Additional Test: Theme toggle icons change correctly
   */
  test('Theme toggle icons update when theme changes', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // In light mode, moon icon should be visible (to switch to dark)
    const moonIconVisible = await page.evaluate(() => {
      const moonIcon = document.querySelector('.icon-moon');
      if (moonIcon) {
        const style = getComputedStyle(moonIcon);
        return style.display !== 'none';
      }
      return false;
    });

    expect(moonIconVisible, 'Moon icon should be visible in light mode').toBeTruthy();

    // Toggle to dark mode
    await themeToggle.click();

    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'dark';
    });

    // In dark mode, sun icon should be visible (to switch to light)
    const sunIconVisible = await page.evaluate(() => {
      const sunIcon = document.querySelector('.icon-sun');
      if (sunIcon) {
        const style = getComputedStyle(sunIcon);
        return style.display !== 'none';
      }
      return false;
    });

    expect(sunIconVisible, 'Sun icon should be visible in dark mode').toBeTruthy();
  });

  /**
   * Additional Test: Theme toggle keyboard accessibility
   */
  test('Theme toggle is keyboard accessible', async ({ page }) => {
    // Tab to theme toggle
    let foundToggle = false;

    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const active = document.activeElement;
        return active?.getAttribute('data-testid');
      });

      if (focusedElement === 'theme-toggle') {
        foundToggle = true;

        // Press Enter or Space to activate
        await page.keyboard.press('Enter');

        // Verify theme changed
        const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
        expect(['light', 'dark']).toContain(theme);

        break;
      }
    }

    expect(foundToggle, 'Theme toggle should be reachable via keyboard').toBeTruthy();
  });

  /**
   * Additional Test: Dark mode CSS variables are correctly applied
   */
  test('Dark mode CSS variables are correctly applied', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Toggle to dark mode
    await themeToggle.click();

    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'dark';
    });

    // Check that CSS variables are updated
    const cssVariables = await page.evaluate(() => {
      const root = document.documentElement;
      const style = getComputedStyle(root);

      return {
        colorBackground: style.getPropertyValue('--color-background').trim(),
        colorText: style.getPropertyValue('--color-text').trim(),
        colorPrimary: style.getPropertyValue('--color-primary').trim()
      };
    });

    // Dark mode should have dark background
    expect(cssVariables.colorBackground).toBe('#0f172a');
    // Dark mode should have light text
    expect(cssVariables.colorText).toBe('#f1f5f9');
    // Dark mode should have appropriate primary color
    expect(cssVariables.colorPrimary).toBe('#60a5fa');
  });

  /**
   * Additional Test: Theme change dispatches custom event
   */
  test('Theme change dispatches custom event', async ({ page }) => {
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // Set up event listener
    const eventPromise = page.evaluate(() => {
      return new Promise((resolve) => {
        document.addEventListener('theme-changed', (e) => {
          resolve(e.detail);
        }, { once: true });
      });
    });

    // Toggle theme
    await themeToggle.click();

    // Wait for event
    const eventDetail = await eventPromise;
    expect(eventDetail).toBeDefined();
    expect(eventDetail.theme).toBeDefined();
    expect(['light', 'dark']).toContain(eventDetail.theme);
  });
});
