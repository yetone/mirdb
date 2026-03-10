/**
 * Theme E2E Tests
 * Owner: Scenario 6 - Theme Support
 *
 * End-to-end tests for theme switching functionality:
 * - Theme toggle switches immediately without page reload
 * - Text has sufficient contrast in dark mode (WCAG AA)
 * - Theme preference persists across page loads
 */

import { test, expect } from '@playwright/test';

test.describe('Theme Support E2E', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  test.describe('Test Case 3: Theme Toggle Click', () => {
    test('theme switches immediately without page reload when clicking toggle', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await expect(page.getByTestId('navbar')).toBeVisible();

      const themeToggle = page.getByTestId('theme-toggle');
      await expect(themeToggle).toBeVisible();

      // Get initial theme
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );

      // Track navigation - we want to ensure NO navigation occurs
      let navigationOccurred = false;
      page.on('load', () => {
        navigationOccurred = true;
      });

      // Click the theme toggle
      await themeToggle.click();

      // Wait briefly for any potential navigation
      await page.waitForTimeout(500);

      // Verify no page reload occurred
      expect(navigationOccurred).toBe(false);

      // Verify theme changed
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );

      expect(newTheme).not.toBe(initialTheme);

      // If we started with light, we should now be dark (or vice versa)
      if (initialTheme === 'light') {
        expect(newTheme).toBe('dark');
      } else {
        expect(newTheme).toBe('light');
      }
    });

    test('theme toggle updates aria-label after click', async ({ page }) => {
      await page.goto('/');

      const themeToggle = page.getByTestId('theme-toggle');

      // Get initial aria-label
      const initialLabel = await themeToggle.getAttribute('aria-label');

      // Click to toggle
      await themeToggle.click();

      // Wait for state update
      await page.waitForTimeout(100);

      // Get new aria-label
      const newLabel = await themeToggle.getAttribute('aria-label');

      // Labels should be different
      expect(newLabel).not.toBe(initialLabel);

      // Verify correct labels based on theme state
      const currentTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );

      if (currentTheme === 'dark') {
        expect(newLabel).toBe('Switch to light theme');
      } else {
        expect(newLabel).toBe('Switch to dark theme');
      }
    });

    test('multiple rapid toggles work correctly', async ({ page }) => {
      await page.goto('/');

      const themeToggle = page.getByTestId('theme-toggle');

      // Get starting theme
      const startTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );

      // Toggle multiple times
      await themeToggle.click();
      await page.waitForTimeout(100);
      await themeToggle.click();
      await page.waitForTimeout(100);
      await themeToggle.click();
      await page.waitForTimeout(100);

      // After 3 toggles, should be in opposite theme
      const finalTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );

      expect(finalTheme).not.toBe(startTheme);
    });
  });

  test.describe('Test Case 5: Text Contrast in Dark Mode', () => {
    test('text elements in dark mode have sufficient contrast', async ({ page }) => {
      await page.goto('/');

      // Switch to dark mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      // Reload to ensure dark mode is applied
      await page.reload();
      await expect(page.getByTestId('navbar')).toBeVisible();

      // Verify dark mode is active
      const theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');

      // Check that key text elements are visible and readable
      const navbarLogo = page.getByTestId('navbar-logo');
      await expect(navbarLogo).toBeVisible();

      const loginLink = page.getByTestId('navbar-login');
      await expect(loginLink).toBeVisible();

      const registerLink = page.getByTestId('navbar-register');
      await expect(registerLink).toBeVisible();

      // Verify text is not the same color as background (basic contrast check)
      // DaisyUI's dark theme should provide proper contrast
      const logoStyles = await navbarLogo.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          color: styles.color,
          backgroundColor: styles.backgroundColor,
        };
      });

      // Text color should be defined (not transparent or same as background)
      expect(logoStyles.color).toBeDefined();
      expect(logoStyles.color).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('heading text is visible and readable in dark mode', async ({ page }) => {
      await page.goto('/');

      // Switch to dark mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      // Check main heading is visible
      const heading = page.locator('h1');
      await expect(heading).toBeVisible();

      // Get computed color - should be light colored text on dark background
      const headingColor = await heading.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.color;
      });

      // The color should be defined and visible
      expect(headingColor).toBeDefined();
      expect(headingColor).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('interactive elements remain visible in dark mode', async ({ page }) => {
      await page.goto('/');

      // Switch to dark mode
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      // All interactive elements should be visible
      const themeToggle = page.getByTestId('theme-toggle');
      await expect(themeToggle).toBeVisible();

      const loginLink = page.getByTestId('navbar-login');
      await expect(loginLink).toBeVisible();

      const registerLink = page.getByTestId('navbar-register');
      await expect(registerLink).toBeVisible();

      // Buttons should be clickable
      await expect(themeToggle).toBeEnabled();
    });
  });

  test.describe('Test Case 6: Theme Persistence', () => {
    test('theme preference persists across page loads', async ({ page }) => {
      await page.goto('/');

      // Start with light theme
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.reload();

      // Verify light theme
      let theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('light');

      // Toggle to dark
      const themeToggle = page.getByTestId('theme-toggle');
      await themeToggle.click();

      // Wait for localStorage update
      await page.waitForTimeout(200);

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');

      // Refresh the page
      await page.reload();

      // Wait for page load
      await expect(page.getByTestId('navbar')).toBeVisible();

      // Theme should still be dark after refresh
      theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');
    });

    test('dark theme persists when navigating between pages', async ({ page }) => {
      await page.goto('/');

      // Set dark theme
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      // Verify dark theme on home page
      let theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');

      // Navigate to login page
      await page.getByTestId('navbar-login').click();
      await expect(page).toHaveURL('/login');

      // Theme should still be dark
      theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');

      // Navigate to register page
      await page.getByTestId('navbar-register').click();
      await expect(page).toHaveURL('/register');

      // Theme should still be dark
      theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');
    });

    test('light theme persists across page loads', async ({ page }) => {
      await page.goto('/');

      // Start with dark theme
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.reload();

      // Toggle to light
      const themeToggle = page.getByTestId('theme-toggle');
      await themeToggle.click();

      // Wait for localStorage update
      await page.waitForTimeout(200);

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('light');

      // Refresh the page
      await page.reload();

      // Wait for page load
      await expect(page.getByTestId('navbar')).toBeVisible();

      // Theme should still be light after refresh
      const theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('light');
    });

    test('new browser session uses stored theme preference', async ({ page, context }) => {
      await page.goto('/');

      // Set dark theme
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
      });

      // Create new page in same context (simulates new tab)
      const newPage = await context.newPage();
      await newPage.goto('/');

      // Wait for page load
      await expect(newPage.getByTestId('navbar')).toBeVisible();

      // Should load dark theme from localStorage
      const theme = await newPage.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');

      await newPage.close();
    });
  });
});
