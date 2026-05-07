import { test, expect } from '@playwright/test';

test.describe('Dark Mode Support', () => {
  test.describe('Test Case 1: System dark mode preference', () => {
    test('page automatically renders in dark theme when prefers-color-scheme: dark is set', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        colorScheme: 'dark',
      });
      const page = await context.newPage();
      await page.goto('/');

      // Check that the dark class is on the html element
      const hasDarkClass = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(hasDarkClass).toBe(true);

      // Verify dark theme CSS variables are active
      const bgColor = await page.evaluate(() =>
        getComputedStyle(document.documentElement).getPropertyValue('--background')
      );
      expect(bgColor.trim()).toBe('#0f172a');

      await context.close();
    });

    test('page renders in light theme when prefers-color-scheme: light is set', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        colorScheme: 'light',
      });
      const page = await context.newPage();
      await page.goto('/');

      const hasDarkClass = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(hasDarkClass).toBe(false);

      await context.close();
    });
  });

  test.describe('Test Case 2: Theme toggle functionality', () => {
    test('clicking theme toggle switches between light and dark themes', async ({
      page,
    }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const toggle = page.getByTestId('theme-toggle');
      await expect(toggle).toBeVisible();

      // Get initial state
      const initialHasDark = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );

      // Click toggle
      await toggle.click();

      // Verify theme switched
      const afterToggleHasDark = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(afterToggleHasDark).toBe(!initialHasDark);

      // Click again to toggle back
      await toggle.click();

      const afterSecondToggle = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(afterSecondToggle).toBe(initialHasDark);
    });
  });

  test.describe('Test Case 3: Theme persistence', () => {
    test('selected theme is saved and restored on page reload', async ({
      page,
    }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const toggle = page.getByTestId('theme-toggle');

      // Determine initial theme
      const initialDark = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );

      // Toggle to opposite theme if needed
      if (!initialDark) {
        await toggle.click();
      }

      // Verify dark is active
      await page.waitForFunction(() =>
        document.documentElement.classList.contains('dark')
      );

      // Reload page
      await page.reload();
      await page.waitForLoadState('networkidle');

      // Verify dark theme persists after reload
      const persistedDark = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(persistedDark).toBe(true);

      // Verify localStorage has the saved theme
      const savedTheme = await page.evaluate(() =>
        localStorage.getItem('mirdb-theme')
      );
      expect(savedTheme).toBe('dark');
    });
  });

  test.describe('Test Case 4: Dark theme contrast', () => {
    test('text has sufficient contrast against dark backgrounds', async ({
      page,
    }) => {
      // Set dark theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('mirdb-theme', 'dark');
        document.documentElement.classList.add('dark');
      });
      await page.reload();
      await page.waitForLoadState('networkidle');

      // Check hero heading contrast
      const heroHeading = page.locator('h1').first();
      await expect(heroHeading).toBeVisible();

      const headingColor = await heroHeading.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.color;
      });

      // In dark mode, text should be light colored
      // rgb(248, 250, 252) = #f8fafc
      expect(headingColor).toBe('rgb(248, 250, 252)');

      // Check body background is dark
      const bodyBg = await page.evaluate(() => {
        const style = window.getComputedStyle(document.body);
        return style.backgroundColor;
      });

      // rgb(15, 23, 42) = #0f172a
      expect(bodyBg).toBe('rgb(15, 23, 42)');
    });

    test('code blocks are readable in dark theme', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('mirdb-theme', 'dark');
        document.documentElement.classList.add('dark');
      });
      await page.reload();
      await page.waitForLoadState('networkidle');

      const codeBlocks = page.locator('pre, code');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < Math.min(count, 3); i++) {
        const el = codeBlocks.nth(i);
        const color = await el.evaluate((e) => {
          const style = window.getComputedStyle(e);
          return style.color;
        });
        // Code should be visible (not same as background)
        expect(color).not.toBe('rgba(0, 0, 0, 0)');
      }
    });
  });

  test.describe('Test Case 5: No flash on load', () => {
    test('page does not flash light theme before switching to dark', async ({
      browser,
    }) => {
      const context = await browser.newContext({
        colorScheme: 'dark',
      });
      const page = await context.newPage();

      // Navigate and check immediately (before networkidle)
      await page.goto('/');

      // Check that dark class is present as early as possible
      const hasDarkClass = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(hasDarkClass).toBe(true);

      // Verify the background is dark immediately
      const bgColor = await page.evaluate(() => {
        const style = window.getComputedStyle(document.body);
        return style.backgroundColor;
      });
      expect(bgColor).toBe('rgb(15, 23, 42)');

      await context.close();
    });

    test('persisted dark theme is applied before first paint', async ({
      page,
    }) => {
      // First set dark theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('mirdb-theme', 'dark');
      });

      // Reload and immediately check
      await page.reload();

      const hasDarkClass = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(hasDarkClass).toBe(true);
    });
  });
});
