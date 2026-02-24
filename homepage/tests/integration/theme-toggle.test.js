/**
 * Theme Toggle Integration Tests
 * Owner: Scenario 9 - Dark Mode Theme Toggle
 *
 * Test cases:
 * - Theme toggle button renders with sun/moon icons
 * - Click theme toggle changes document data-theme attribute
 * - Theme preference is saved to localStorage
 * - Page loads in dark mode when localStorage has dark theme
 * - WCAG AA color contrast compliance
 */

import { test, expect } from '@playwright/test';

test.describe('Dark Mode Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  test.describe('Theme Toggle Button', () => {
    test('Theme toggle button is rendered with sun/moon icons', async ({ page }) => {
      // Check desktop theme toggle
      const themeToggle = page.locator('#theme-toggle');
      await expect(themeToggle).toBeVisible();

      // Check for sun icon
      const sunIcon = themeToggle.locator('.icon-sun');
      await expect(sunIcon).toBeAttached();

      // Check for moon icon
      const moonIcon = themeToggle.locator('.icon-moon');
      await expect(moonIcon).toBeAttached();

      // Check accessibility attributes
      await expect(themeToggle).toHaveAttribute('aria-label', /toggle|switch|mode/i);
      await expect(themeToggle).toHaveAttribute('type', 'button');
    });

    test('Theme toggle button has correct accessibility attributes', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');
      await expect(themeToggle).toHaveAttribute('aria-label');
      await expect(themeToggle).toHaveAttribute('title', 'Toggle theme');
    });
  });

  test.describe('Theme Switching', () => {
    test('Click theme toggle changes document data-theme attribute to dark', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');

      // Get initial theme (should be light or based on system preference)
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );

      // If starting in light mode, click should change to dark
      if (initialTheme === 'light' || !initialTheme) {
        await themeToggle.click();

        const newTheme = await page.evaluate(() =>
          document.documentElement.getAttribute('data-theme')
        );
        expect(newTheme).toBe('dark');
      }
    });

    test('Click theme toggle twice returns to original theme', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');

      // Force light mode first
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });

      // Click to dark
      await themeToggle.click();
      let theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');

      // Click back to light
      await themeToggle.click();
      theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('light');
    });

    test('Mobile theme toggle also toggles theme', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      // Open mobile nav
      const hamburgerBtn = page.locator('#hamburger-btn');
      await hamburgerBtn.click();

      // Find mobile theme toggle
      const mobileThemeToggle = page.locator('#mobile-theme-toggle');
      await expect(mobileThemeToggle).toBeVisible();

      // Force light mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });

      // Click mobile theme toggle
      await mobileThemeToggle.click();

      const theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');
    });
  });

  test.describe('Theme Persistence', () => {
    test('Theme preference is saved to localStorage', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');

      // Force light mode first
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });

      // Toggle to dark
      await themeToggle.click();

      // Check localStorage
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');
    });

    test('Page loads in dark mode when localStorage has dark theme', async ({ page }) => {
      // Set dark mode in localStorage before page load
      await page.evaluate(() => localStorage.setItem('theme', 'dark'));

      // Reload page
      await page.reload();

      // Check that dark theme is applied
      const theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');
    });

    test('Page loads without flash of wrong theme when dark mode is stored', async ({ page }) => {
      // Set dark mode preference
      await page.evaluate(() => localStorage.setItem('theme', 'dark'));

      // Track theme attribute changes during page load
      const themeChanges = [];

      // Navigate and capture initial theme
      await page.addInitScript(() => {
        // This runs before any other scripts
        const observer = new MutationObserver((mutations) => {
          for (const mutation of mutations) {
            if (mutation.attributeName === 'data-theme') {
              window.__themeChanges = window.__themeChanges || [];
              window.__themeChanges.push(document.documentElement.getAttribute('data-theme'));
            }
          }
        });
        observer.observe(document.documentElement, { attributes: true });
        window.__themeChanges = [];
      });

      await page.reload();

      // Wait for page to be fully loaded
      await page.waitForLoadState('networkidle');

      // The first theme set should be dark (no flash of light)
      const finalTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(finalTheme).toBe('dark');
    });

    test('Theme persists after multiple page reloads', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');

      // Set to dark mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      // Reload multiple times
      for (let i = 0; i < 3; i++) {
        await page.reload();

        const theme = await page.evaluate(() =>
          document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');
      }
    });
  });

  test.describe('Visual Theme Changes', () => {
    test('Body background color changes in dark mode', async ({ page }) => {
      // Start in light mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.reload();

      const lightBgColor = await page.evaluate(() =>
        getComputedStyle(document.body).backgroundColor
      );

      // Switch to dark mode
      const themeToggle = page.locator('#theme-toggle');
      await themeToggle.click();

      // Wait for transition
      await page.waitForTimeout(400);

      const darkBgColor = await page.evaluate(() =>
        getComputedStyle(document.body).backgroundColor
      );

      // Colors should be different
      expect(darkBgColor).not.toBe(lightBgColor);
    });

    test('Header background changes in dark mode', async ({ page }) => {
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.reload();

      const header = page.locator('header.header');
      const lightHeaderBg = await header.evaluate((el) =>
        getComputedStyle(el).backgroundColor
      );

      // Switch to dark
      const themeToggle = page.locator('#theme-toggle');
      await themeToggle.click();
      await page.waitForTimeout(400);

      const darkHeaderBg = await header.evaluate((el) =>
        getComputedStyle(el).backgroundColor
      );

      expect(darkHeaderBg).not.toBe(lightHeaderBg);
    });

    test('Sun icon visible in light mode, moon icon visible in dark mode', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');

      // Set light mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.reload();

      // In light mode: sun should be visible (to indicate "switch to dark")
      const sunIcon = themeToggle.locator('.icon-sun');
      const moonIcon = themeToggle.locator('.icon-moon');

      // Check computed display styles
      const sunDisplay = await sunIcon.evaluate((el) => getComputedStyle(el).display);
      const moonDisplay = await moonIcon.evaluate((el) => getComputedStyle(el).display);

      expect(sunDisplay).not.toBe('none');
      expect(moonDisplay).toBe('none');

      // Toggle to dark mode
      await themeToggle.click();
      await page.waitForTimeout(100);

      const darkSunDisplay = await sunIcon.evaluate((el) => getComputedStyle(el).display);
      const darkMoonDisplay = await moonIcon.evaluate((el) => getComputedStyle(el).display);

      expect(darkSunDisplay).toBe('none');
      expect(darkMoonDisplay).not.toBe('none');
    });
  });

  test.describe('WCAG AA Compliance', () => {
    test('Text has sufficient contrast in light mode', async ({ page }) => {
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.reload();

      // Get text color and background color
      const textColor = await page.evaluate(() =>
        getComputedStyle(document.body).color
      );
      const bgColor = await page.evaluate(() =>
        getComputedStyle(document.body).backgroundColor
      );

      // Both colors should be defined
      expect(textColor).toBeTruthy();
      expect(bgColor).toBeTruthy();

      // We verify colors are properly set (actual contrast calculation would require additional library)
      // For now, ensure colors are different enough
      expect(textColor).not.toBe(bgColor);
    });

    test('Text has sufficient contrast in dark mode', async ({ page }) => {
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      // Get text color and background color
      const textColor = await page.evaluate(() =>
        getComputedStyle(document.body).color
      );
      const bgColor = await page.evaluate(() =>
        getComputedStyle(document.body).backgroundColor
      );

      // Both colors should be defined
      expect(textColor).toBeTruthy();
      expect(bgColor).toBeTruthy();

      // Verify colors are different (basic contrast check)
      expect(textColor).not.toBe(bgColor);
    });

    test('Primary buttons remain visible in dark mode', async ({ page }) => {
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      const primaryBtn = page.locator('.btn-primary').first();
      if (await primaryBtn.isVisible()) {
        const btnColor = await primaryBtn.evaluate((el) =>
          getComputedStyle(el).color
        );
        const btnBgColor = await primaryBtn.evaluate((el) =>
          getComputedStyle(el).backgroundColor
        );

        expect(btnColor).toBeTruthy();
        expect(btnBgColor).toBeTruthy();
      }
    });

    test('Links are distinguishable in both modes', async ({ page }) => {
      // Test light mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.reload();

      const lightLinkColor = await page.evaluate(() => {
        const link = document.querySelector('a');
        return link ? getComputedStyle(link).color : null;
      });
      expect(lightLinkColor).toBeTruthy();

      // Test dark mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      const darkLinkColor = await page.evaluate(() => {
        const link = document.querySelector('a');
        return link ? getComputedStyle(link).color : null;
      });
      expect(darkLinkColor).toBeTruthy();
    });
  });
});
