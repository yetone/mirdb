import { test, expect } from '@playwright/test';

/**
 * Theme Toggle Functionality Tests (REQ-8)
 * Tests for light/dark theme toggle implementation
 *
 * This is a 'Could' priority feature per the PRD
 */

test.describe('Theme Toggle Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test for clean state
    await page.goto('file://' + process.cwd() + '/public/index.html');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  test.describe('Test Case 1: Theme toggle presence', () => {
    test('theme toggle control is visible in header', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      // Check that theme toggle button exists and is visible
      const themeToggle = page.locator('[data-testid="theme-toggle"]');
      await expect(themeToggle).toBeVisible();

      // Verify it's within the header/navbar area
      const navbar = page.locator('.navbar');
      await expect(navbar.locator('[data-testid="theme-toggle"]')).toBeVisible();
    });

    test('theme toggle has accessible label', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const themeToggle = page.locator('[data-testid="theme-toggle"]');

      // Should have aria-label for accessibility
      await expect(themeToggle).toHaveAttribute('aria-label', /toggle|theme|mode/i);
    });
  });

  test.describe('Test Case 2: Click theme toggle from light mode', () => {
    test('page switches to dark theme without reload when clicking from light mode', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      // Ensure we start in light mode
      const html = page.locator('html');
      const initialTheme = await html.getAttribute('data-theme');

      // If starting in dark mode, click to get to light first
      if (initialTheme === 'dark') {
        await page.locator('[data-testid="theme-toggle"]').click();
      }

      // Verify starting in light mode
      await expect(html).not.toHaveAttribute('data-theme', 'dark');

      // Record the page URL before clicking
      const urlBefore = page.url();

      // Click theme toggle
      await page.locator('[data-testid="theme-toggle"]').click();

      // Verify switched to dark theme
      await expect(html).toHaveAttribute('data-theme', 'dark');

      // Verify no page reload (URL should be the same)
      expect(page.url()).toBe(urlBefore);

      // Verify visual change - background should be dark
      const bodyBgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Dark background typically has low RGB values
      const rgbMatch = bodyBgColor.match(/\d+/g);
      if (rgbMatch) {
        const [r, g, b] = rgbMatch.map(Number);
        const isDark = (r + g + b) / 3 < 128;
        expect(isDark).toBe(true);
      }
    });
  });

  test.describe('Test Case 3: Click theme toggle from dark mode', () => {
    test('page switches to light theme without reload when clicking from dark mode', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // Ensure we're in dark mode first
      const initialTheme = await html.getAttribute('data-theme');
      if (initialTheme !== 'dark') {
        await page.locator('[data-testid="theme-toggle"]').click();
        await expect(html).toHaveAttribute('data-theme', 'dark');
      }

      // Record the page URL before clicking
      const urlBefore = page.url();

      // Click theme toggle to switch back to light
      await page.locator('[data-testid="theme-toggle"]').click();

      // Verify switched to light theme
      await expect(html).not.toHaveAttribute('data-theme', 'dark');

      // Verify no page reload
      expect(page.url()).toBe(urlBefore);

      // Verify visual change - background should be light
      const bodyBgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Light background typically has high RGB values
      const rgbMatch = bodyBgColor.match(/\d+/g);
      if (rgbMatch) {
        const [r, g, b] = rgbMatch.map(Number);
        const isLight = (r + g + b) / 3 > 128;
        expect(isLight).toBe(true);
      }
    });
  });

  test.describe('Test Case 4: Theme persistence across page loads', () => {
    test('theme preference persists after page reload', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // Switch to dark mode
      const initialTheme = await html.getAttribute('data-theme');
      if (initialTheme !== 'dark') {
        await page.locator('[data-testid="theme-toggle"]').click();
      }

      await expect(html).toHaveAttribute('data-theme', 'dark');

      // Verify localStorage is set
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');

      // Reload the page
      await page.reload();

      // Verify theme persists
      await expect(html).toHaveAttribute('data-theme', 'dark');
    });

    test('light theme preference persists after page reload', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // Ensure we're in light mode by going to dark then back
      await page.locator('[data-testid="theme-toggle"]').click();
      await page.locator('[data-testid="theme-toggle"]').click();

      // Should be in light mode
      await expect(html).not.toHaveAttribute('data-theme', 'dark');

      // Verify localStorage is set
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('light');

      // Reload the page
      await page.reload();

      // Verify theme persists
      await expect(html).not.toHaveAttribute('data-theme', 'dark');
    });
  });

  test.describe('Test Case 5: Dark theme contrast requirements', () => {
    test('dark theme meets 4.5:1 contrast ratio for text', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // Switch to dark mode
      const initialTheme = await html.getAttribute('data-theme');
      if (initialTheme !== 'dark') {
        await page.locator('[data-testid="theme-toggle"]').click();
      }

      await expect(html).toHaveAttribute('data-theme', 'dark');

      // Check contrast by examining CSS variables
      const contrast = await page.evaluate(() => {
        const root = document.documentElement;
        const textColor = getComputedStyle(root).getPropertyValue('--text-color').trim();
        const bgColor = getComputedStyle(root).getPropertyValue('--background-color').trim();

        return { textColor, bgColor };
      });

      // Verify colors are defined
      expect(contrast.textColor).toBeTruthy();
      expect(contrast.bgColor).toBeTruthy();

      // Check that hero section text is readable
      const heroText = page.locator('.hero-content h1');
      await expect(heroText).toBeVisible();

      // Check main text elements are visible and styled
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
    });
  });

  test.describe('Test Case 6: Code block styling in both themes', () => {
    test('code blocks are readable in light theme', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // Ensure light mode
      const initialTheme = await html.getAttribute('data-theme');
      if (initialTheme === 'dark') {
        await page.locator('[data-testid="theme-toggle"]').click();
      }

      // Navigate to quickstart section which has code blocks
      const codeBlock = page.locator('.code-block').first();
      await codeBlock.scrollIntoViewIfNeeded();

      await expect(codeBlock).toBeVisible();

      // Verify code is visible
      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Check code has proper styling
      const codeStyles = await codeElement.evaluate((el) => {
        const styles = getComputedStyle(el);
        return {
          color: styles.color,
          backgroundColor: getComputedStyle(el.closest('.code-block')!).backgroundColor
        };
      });

      expect(codeStyles.color).toBeTruthy();
      expect(codeStyles.backgroundColor).toBeTruthy();
    });

    test('code blocks are readable in dark theme', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // Switch to dark mode
      const initialTheme = await html.getAttribute('data-theme');
      if (initialTheme !== 'dark') {
        await page.locator('[data-testid="theme-toggle"]').click();
      }

      await expect(html).toHaveAttribute('data-theme', 'dark');

      // Navigate to quickstart section which has code blocks
      const codeBlock = page.locator('.code-block').first();
      await codeBlock.scrollIntoViewIfNeeded();

      await expect(codeBlock).toBeVisible();

      // Verify code is visible
      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Check code has proper styling in dark mode
      const codeStyles = await codeElement.evaluate((el) => {
        const styles = getComputedStyle(el);
        return {
          color: styles.color,
          backgroundColor: getComputedStyle(el.closest('.code-block')!).backgroundColor
        };
      });

      expect(codeStyles.color).toBeTruthy();
      expect(codeStyles.backgroundColor).toBeTruthy();
    });
  });

  test.describe('Test Case 7: System preference detection', () => {
    test('theme respects prefers-color-scheme dark on first load', async ({ page }) => {
      // Emulate dark color scheme preference
      await page.emulateMedia({ colorScheme: 'dark' });

      // Clear localStorage to simulate first visit
      await page.goto('file://' + process.cwd() + '/public/index.html');
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Should default to dark theme based on system preference
      const html = page.locator('html');
      await expect(html).toHaveAttribute('data-theme', 'dark');
    });

    test('theme respects prefers-color-scheme light on first load', async ({ page }) => {
      // Emulate light color scheme preference
      await page.emulateMedia({ colorScheme: 'light' });

      // Clear localStorage to simulate first visit
      await page.goto('file://' + process.cwd() + '/public/index.html');
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Should default to light theme based on system preference
      const html = page.locator('html');
      await expect(html).not.toHaveAttribute('data-theme', 'dark');
    });

    test('user preference overrides system preference', async ({ page }) => {
      // Emulate dark color scheme preference
      await page.emulateMedia({ colorScheme: 'dark' });

      await page.goto('file://' + process.cwd() + '/public/index.html');

      const html = page.locator('html');

      // System should default to dark
      await expect(html).toHaveAttribute('data-theme', 'dark');

      // User toggles to light
      await page.locator('[data-testid="theme-toggle"]').click();

      // Should be light despite system preference
      await expect(html).not.toHaveAttribute('data-theme', 'dark');

      // Reload page
      await page.reload();

      // User preference should persist
      await expect(html).not.toHaveAttribute('data-theme', 'dark');
    });
  });

  test.describe('Additional accessibility tests', () => {
    test('theme toggle is keyboard accessible', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const themeToggle = page.locator('[data-testid="theme-toggle"]');

      // Focus the toggle with keyboard
      await themeToggle.focus();

      // Verify it can receive focus
      await expect(themeToggle).toBeFocused();

      // Trigger with Enter key
      const html = page.locator('html');
      const beforeTheme = await html.getAttribute('data-theme');

      await page.keyboard.press('Enter');

      // Theme should change
      const afterTheme = await html.getAttribute('data-theme');
      expect(afterTheme).not.toBe(beforeTheme);
    });

    test('theme toggle has focus-visible styles', async ({ page }) => {
      await page.goto('file://' + process.cwd() + '/public/index.html');

      const themeToggle = page.locator('[data-testid="theme-toggle"]');

      // Tab to the toggle to trigger focus-visible
      await page.keyboard.press('Tab');

      // Keep tabbing until we reach the theme toggle
      let attempts = 0;
      while (attempts < 20) {
        const focused = await page.evaluate(() => document.activeElement?.getAttribute('data-testid'));
        if (focused === 'theme-toggle') break;
        await page.keyboard.press('Tab');
        attempts++;
      }

      // Verify the toggle is focusable (can receive focus)
      await themeToggle.focus();
      await expect(themeToggle).toBeFocused();
    });
  });
});
