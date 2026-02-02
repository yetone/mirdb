/**
 * Dark Mode E2E Tests
 * Owner: Scenario 14 - Dark Mode Support
 *
 * Tests:
 * - System preference detection
 * - Theme toggle functionality
 * - Dark mode color scheme
 * - Contrast in both modes
 * - Code highlighting in dark mode
 */

import { test, expect } from '@playwright/test';

test.describe('Dark Mode Support', () => {
  test.describe('System Preference Detection', () => {
    test('should display dark mode automatically when prefers-color-scheme is dark', async ({ page }) => {
      // Emulate dark color scheme
      await page.emulateMedia({ colorScheme: 'dark' });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify the page applies dark mode styles
      const body = page.locator('body');
      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Dark mode background should be dark (#1a1a2e = rgb(26, 26, 46))
      expect(backgroundColor).toMatch(/rgb\(26,\s*26,\s*46\)|#1a1a2e/i);
    });

    test('should display light mode automatically when prefers-color-scheme is light', async ({ page }) => {
      // Emulate light color scheme
      await page.emulateMedia({ colorScheme: 'light' });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify the page applies light mode styles
      const body = page.locator('body');
      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Light mode background should be white/light (#ffffff = rgb(255, 255, 255))
      expect(backgroundColor).toMatch(/rgb\(255,\s*255,\s*255\)|#ffffff|white/i);
    });
  });

  test.describe('Theme Toggle Functionality', () => {
    test('should toggle theme when theme toggle button is clicked', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Find and click the theme toggle button
      const themeToggle = page.locator('[data-theme-toggle], .theme-toggle, #theme-toggle, button[aria-label*="theme"]');
      await expect(themeToggle).toBeVisible();

      // Get initial background color (should be light)
      const body = page.locator('body');
      const initialBgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Click to toggle to dark mode
      await themeToggle.click();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get new background color (should be dark)
      const newBgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Background should have changed
      expect(initialBgColor).not.toEqual(newBgColor);

      // Verify dark theme is applied
      const htmlElement = page.locator('html');
      const dataTheme = await htmlElement.getAttribute('data-theme');
      expect(dataTheme).toBe('dark');
    });

    test('should toggle back to light mode on second click', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const themeToggle = page.locator('[data-theme-toggle], .theme-toggle, #theme-toggle, button[aria-label*="theme"]');

      // Click once to go to dark mode
      await themeToggle.click();
      await page.waitForTimeout(300);

      // Click again to go back to light mode
      await themeToggle.click();
      await page.waitForTimeout(300);

      // Verify light theme is applied
      const htmlElement = page.locator('html');
      const dataTheme = await htmlElement.getAttribute('data-theme');
      expect(dataTheme).toBe('light');
    });

    test('should persist theme preference across page reloads', async ({ page, context }) => {
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const themeToggle = page.locator('[data-theme-toggle], .theme-toggle, #theme-toggle, button[aria-label*="theme"]');

      // Switch to dark mode
      await themeToggle.click();
      await page.waitForTimeout(300);

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');

      // Reload the page
      await page.reload();
      await page.waitForLoadState('domcontentloaded');

      // Verify dark theme is still applied after reload
      const htmlElement = page.locator('html');
      const dataTheme = await htmlElement.getAttribute('data-theme');
      expect(dataTheme).toBe('dark');
    });
  });

  test.describe('Dark Mode Visual Styles', () => {
    test('should have appropriate dark background color', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const body = page.locator('body');
      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Should be dark background (#1a1a2e = rgb(26, 26, 46))
      expect(backgroundColor).toMatch(/rgb\(26,\s*26,\s*46\)/i);
    });

    test('should have light text color in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check a visible text element
      const heading = page.locator('h1, h2, h3').first();
      await expect(heading).toBeVisible();

      const textColor = await heading.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Text should be light colored (rgb values > 200 for each channel)
      const rgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        expect(r).toBeGreaterThan(180);
        expect(g).toBeGreaterThan(180);
        expect(b).toBeGreaterThan(180);
      }
    });

    test('should have appropriate surface color for cards in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check a card or surface element
      const surfaceElement = page.locator('.feature-card, .spec-card, .status-card, [class*="card"]').first();

      if (await surfaceElement.isVisible()) {
        const bgColor = await surfaceElement.evaluate((el) => {
          return window.getComputedStyle(el).backgroundColor;
        });

        // Surface should be slightly lighter than background but still dark (#252542 = rgb(37, 37, 66))
        const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        if (rgbMatch) {
          const [, r, g, b] = rgbMatch.map(Number);
          // Should be dark (low values) but not pure black
          expect(r).toBeLessThan(100);
          expect(g).toBeLessThan(100);
          expect(b).toBeLessThan(150);
        }
      }
    });
  });

  test.describe('Code Syntax Highlighting in Dark Mode', () => {
    test('should have readable code blocks in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Navigate to code examples section
      const codeSection = page.locator('#code-examples');
      if (await codeSection.isVisible()) {
        await codeSection.scrollIntoViewIfNeeded();

        // Check that code blocks have appropriate background
        const codeBlock = page.locator('.code-block, pre code, pre').first();
        if (await codeBlock.isVisible()) {
          const bgColor = await codeBlock.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
          });

          // Code background should be dark
          const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
          if (rgbMatch) {
            const [, r, g, b] = rgbMatch.map(Number);
            // Should be dark background
            expect(r).toBeLessThan(100);
            expect(g).toBeLessThan(100);
            expect(b).toBeLessThan(100);
          }
        }
      }
    });

    test('should have visible syntax highlighting colors in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check for syntax highlighting classes
      const codeKeyword = page.locator('.code-keyword').first();

      if (await codeKeyword.isVisible()) {
        const keywordColor = await codeKeyword.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });

        // Keyword should have distinct color (not white/gray)
        // Expecting something like purple/pink for keywords (#cba6f7)
        expect(keywordColor).not.toMatch(/rgb\(255,\s*255,\s*255\)/i);
      }
    });
  });

  test.describe('Theme Toggle Accessibility', () => {
    test('should have accessible theme toggle button', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const themeToggle = page.locator('[data-theme-toggle], .theme-toggle, #theme-toggle, button[aria-label*="theme"]');
      await expect(themeToggle).toBeVisible();

      // Should have aria-label
      const ariaLabel = await themeToggle.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toMatch(/theme|mode|dark|light/i);
    });

    test('should be keyboard accessible', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const themeToggle = page.locator('[data-theme-toggle], .theme-toggle, #theme-toggle, button[aria-label*="theme"]');

      // Focus on the toggle button
      await themeToggle.focus();

      // Verify it can be activated with Enter key
      await page.keyboard.press('Enter');
      await page.waitForTimeout(300);

      const htmlElement = page.locator('html');
      const dataTheme = await htmlElement.getAttribute('data-theme');
      expect(dataTheme).toBe('dark');
    });
  });
});
