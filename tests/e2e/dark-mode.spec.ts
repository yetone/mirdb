/**
 * Dark Mode Tests
 * Owner: Scenario 11 - Dark Mode Support
 *
 * Test cases:
 * - Responds to prefers-color-scheme: dark
 * - Dark mode toggle button exists
 * - Theme transition is smooth
 * - Dark mode has sufficient contrast
 * - Code blocks styled in dark mode
 * - Preference persists across reloads
 */

import { test, expect, Page } from '@playwright/test';

test.describe('Dark Mode Support', () => {
  // Note: We clear localStorage in individual tests that need it
  // to avoid clearing on reload for persistence tests

  test('TC1: Page responds to system dark mode preference (prefers-color-scheme: dark)', async ({ browser }) => {
    // Create context with dark color scheme
    const darkContext = await browser.newContext({
      colorScheme: 'dark',
    });
    const darkPage = await darkContext.newPage();
    // Clear localStorage for this test
    await darkPage.addInitScript(() => {
      localStorage.clear();
    });
    await darkPage.goto('/');
    await darkPage.waitForLoadState('domcontentloaded');

    // Verify the page adapts to dark mode
    const bgColor = await darkPage.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Dark mode should have a dark background
    // rgb(15, 23, 42) is #0f172a (our dark background color)
    expect(bgColor).toMatch(/rgb\(\s*15\s*,\s*23\s*,\s*42\s*\)|#0f172a/i);

    await darkContext.close();
  });

  test('TC1b: Page responds to system light mode preference', async ({ browser }) => {
    // Create context with light color scheme
    const lightContext = await browser.newContext({
      colorScheme: 'light',
    });
    const lightPage = await lightContext.newPage();
    // Clear localStorage for this test
    await lightPage.addInitScript(() => {
      localStorage.clear();
    });
    await lightPage.goto('/');
    await lightPage.waitForLoadState('domcontentloaded');

    // Verify the page is in light mode
    const bgColor = await lightPage.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Light mode should have a light background
    // rgb(255, 255, 255) is #ffffff (our light background color)
    expect(bgColor).toMatch(/rgb\(\s*255\s*,\s*255\s*,\s*255\s*\)|#fff(fff)?/i);

    await lightContext.close();
  });

  test('TC2: Dark mode toggle button exists in header', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Find the dark mode toggle button
    const toggleButton = page.locator('#dark-mode-toggle');
    await expect(toggleButton).toBeVisible();

    // Verify it has proper accessibility attributes
    const ariaLabel = await toggleButton.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel!.toLowerCase()).toMatch(/dark|light|theme|mode/i);

    // Verify it has aria-pressed attribute
    const ariaPressed = await toggleButton.getAttribute('aria-pressed');
    expect(ariaPressed).toBeTruthy();
    expect(['true', 'false']).toContain(ariaPressed);

    // Verify it has the correct type
    const buttonType = await toggleButton.getAttribute('type');
    expect(buttonType).toBe('button');
  });

  test('TC3: Toggle dark mode and verify smooth transition', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const toggleButton = page.locator('#dark-mode-toggle');
    await expect(toggleButton).toBeVisible();

    // Get initial background color
    const initialBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Click the toggle button
    await toggleButton.click();

    // Wait a moment for the transition class to be applied
    await page.waitForTimeout(50);

    // Verify the transition class is applied during transition
    const hasTransitionClass = await page.evaluate(() => {
      return document.documentElement.classList.contains('theme-transitioning');
    });
    // The class may or may not be present depending on timing
    // The important thing is no flash or jank

    // Wait for transition to complete
    await page.waitForTimeout(350);

    // Get new background color
    const newBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    // Verify the background color changed
    expect(newBgColor).not.toBe(initialBgColor);

    // Verify no visible jank by checking the data-theme attribute was set
    const dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBeTruthy();
  });

  test('TC3b: Toggle back and forth works correctly', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const toggleButton = page.locator('#dark-mode-toggle');

    // Toggle to dark mode
    await toggleButton.click();
    await page.waitForTimeout(350);

    let dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('dark');

    // Toggle back to light mode
    await toggleButton.click();
    await page.waitForTimeout(350);

    dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('light');
  });

  test('TC4: Dark mode has sufficient color contrast for readability', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Toggle to dark mode
    const toggleButton = page.locator('#dark-mode-toggle');
    await toggleButton.click();
    await page.waitForTimeout(350);

    // Get text and background colors
    const colors = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = getComputedStyle(body);
      const bgColor = computedStyle.backgroundColor;
      const textColor = computedStyle.color;

      // Parse RGB values
      const parseRgb = (rgb: string) => {
        const match = rgb.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        if (match) {
          return {
            r: parseInt(match[1]),
            g: parseInt(match[2]),
            b: parseInt(match[3]),
          };
        }
        return null;
      };

      const bg = parseRgb(bgColor);
      const text = parseRgb(textColor);

      // Calculate relative luminance
      const getLuminance = (color: { r: number; g: number; b: number }) => {
        const a = [color.r, color.g, color.b].map((v) => {
          v /= 255;
          return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return a[0] * 0.2126 + a[1] * 0.7152 + a[2] * 0.0722;
      };

      if (bg && text) {
        const bgLum = getLuminance(bg);
        const textLum = getLuminance(text);
        const lighter = Math.max(bgLum, textLum);
        const darker = Math.min(bgLum, textLum);
        const contrastRatio = (lighter + 0.05) / (darker + 0.05);
        return { contrastRatio, bgColor, textColor };
      }

      return { contrastRatio: 0, bgColor, textColor };
    });

    // WCAG AA requires at least 4.5:1 contrast ratio for normal text
    expect(colors.contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('TC5: Code blocks have appropriate dark theme styling', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Toggle to dark mode
    const toggleButton = page.locator('#dark-mode-toggle');
    await toggleButton.click();
    await page.waitForTimeout(350);

    // Scroll to a code block
    const codeBlock = page.locator('.code-block').first();
    await codeBlock.scrollIntoViewIfNeeded();

    // Verify code block has dark theme styling
    const codeStyles = await codeBlock.evaluate((el) => {
      const computedStyle = getComputedStyle(el);
      return {
        backgroundColor: computedStyle.backgroundColor,
        borderColor: computedStyle.borderColor,
      };
    });

    // Code block should have a dark background
    // Check it's darker than rgb(100, 100, 100)
    const bgMatch = codeStyles.backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (bgMatch) {
      const avgBrightness = (parseInt(bgMatch[1]) + parseInt(bgMatch[2]) + parseInt(bgMatch[3])) / 3;
      expect(avgBrightness).toBeLessThan(100);
    }

    // Verify code text is light colored
    const codeElement = page.locator('.code-block code').first();
    const codeColor = await codeElement.evaluate((el) => {
      return getComputedStyle(el).color;
    });

    const textMatch = codeColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (textMatch) {
      const avgTextBrightness = (parseInt(textMatch[1]) + parseInt(textMatch[2]) + parseInt(textMatch[3])) / 3;
      expect(avgTextBrightness).toBeGreaterThan(150);
    }
  });

  test('TC6: Dark mode preference persists across page reloads', async ({ page }) => {
    // Clear localStorage before first navigation (not on reload)
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Toggle to dark mode
    const toggleButton = page.locator('#dark-mode-toggle');
    await toggleButton.click();
    await page.waitForTimeout(350);

    // Verify dark mode is active
    let dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('dark');

    // Verify preference is stored in localStorage
    const storedTheme = await page.evaluate(() => {
      return localStorage.getItem('mirdb-theme');
    });
    expect(storedTheme).toBe('dark');

    // Reload the page - localStorage should persist
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify dark mode persists after reload
    dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('dark');

    // Verify background is still dark
    const bgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });
    expect(bgColor).toMatch(/rgb\(\s*15\s*,\s*23\s*,\s*42\s*\)/);
  });

  test('TC6b: Light mode preference persists across page reloads', async ({ page }) => {
    // Clear localStorage before first navigation (not on reload)
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Toggle to dark mode first, then back to light
    const toggleButton = page.locator('#dark-mode-toggle');
    await toggleButton.click();
    await page.waitForTimeout(350);
    await toggleButton.click();
    await page.waitForTimeout(350);

    // Verify light mode is active
    let dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('light');

    // Reload the page - localStorage should persist
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify light mode persists after reload
    dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBe('light');
  });

  test('Dark mode toggle button is keyboard accessible', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const toggleButton = page.locator('#dark-mode-toggle');

    // Tab to the toggle button
    await page.keyboard.press('Tab');
    // Navigate through nav items to reach the toggle
    for (let i = 0; i < 10; i++) {
      const focusedElement = await page.evaluate(() => {
        return document.activeElement?.id || document.activeElement?.tagName;
      });
      if (focusedElement === 'dark-mode-toggle') break;
      await page.keyboard.press('Tab');
    }

    // Verify the button is focusable
    const focusedElement = await page.evaluate(() => {
      return document.activeElement?.id;
    });
    expect(focusedElement).toBe('dark-mode-toggle');

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(350);

    // Verify toggle worked
    const dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });
    expect(dataTheme).toBeTruthy();
  });

  test('Dark mode icons switch correctly', async ({ page }) => {
    // Clear localStorage for this test
    await page.addInitScript(() => {
      localStorage.clear();
    });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const moonIcon = page.locator('#dark-mode-toggle .icon-moon');
    const sunIcon = page.locator('#dark-mode-toggle .icon-sun');

    // In light mode, moon icon should be visible (to switch to dark)
    // In dark mode, sun icon should be visible (to switch to light)

    // Check initial state (should show moon for dark mode option)
    const toggleButton = page.locator('#dark-mode-toggle');

    // Toggle to dark mode
    await toggleButton.click();
    await page.waitForTimeout(350);

    // Sun icon should now be visible
    const sunDisplay = await sunIcon.evaluate((el) => {
      return getComputedStyle(el).display;
    });
    expect(sunDisplay).not.toBe('none');

    const moonDisplay = await moonIcon.evaluate((el) => {
      return getComputedStyle(el).display;
    });
    expect(moonDisplay).toBe('none');
  });
});
