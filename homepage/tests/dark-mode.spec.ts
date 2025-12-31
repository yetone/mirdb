import { test, expect } from '@playwright/test';

test.describe('Dark Mode Support', () => {
  test.describe('Theme Toggle Presence', () => {
    test('should have a theme toggle button visible', async ({ page }) => {
      await page.goto('/');

      // Look for theme toggle button
      const themeToggle = page.locator('[data-testid="theme-toggle"]');
      await expect(themeToggle).toBeVisible();
      await expect(themeToggle).toBeEnabled();
    });

    test('theme toggle should be accessible', async ({ page }) => {
      await page.goto('/');

      const themeToggle = page.locator('[data-testid="theme-toggle"]');
      await expect(themeToggle).toHaveAttribute('aria-label', /toggle|theme|dark|light/i);
    });
  });

  test.describe('Dark Mode Toggle', () => {
    test('should switch to dark mode when toggle is clicked', async ({ page }) => {
      await page.goto('/');

      // Get initial background color (light mode)
      const initialBgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Click theme toggle
      await page.click('[data-testid="theme-toggle"]');

      // Verify dark mode is applied
      const darkModeBgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // In dark mode, background should be dark (low RGB values)
      expect(darkModeBgColor).not.toBe(initialBgColor);

      // Check that document has dark class or data attribute
      const hasDarkClass = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });
      expect(hasDarkClass).toBe(true);
    });

    test('should have dark background color in dark mode', async ({ page }) => {
      await page.goto('/');

      // Click theme toggle to enable dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Verify body background is dark
      const bgColor = await page.evaluate(() => {
        const rgb = getComputedStyle(document.body).backgroundColor;
        const match = rgb.match(/\d+/g);
        if (match) {
          return {
            r: parseInt(match[0]),
            g: parseInt(match[1]),
            b: parseInt(match[2])
          };
        }
        return null;
      });

      // Dark background should have low RGB values (< 100)
      expect(bgColor).not.toBeNull();
      if (bgColor) {
        const avgBrightness = (bgColor.r + bgColor.g + bgColor.b) / 3;
        expect(avgBrightness).toBeLessThan(100);
      }
    });

    test('should have light text color in dark mode', async ({ page }) => {
      await page.goto('/');

      // Click theme toggle to enable dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Verify text color is light
      const textColor = await page.evaluate(() => {
        const rgb = getComputedStyle(document.body).color;
        const match = rgb.match(/\d+/g);
        if (match) {
          return {
            r: parseInt(match[0]),
            g: parseInt(match[1]),
            b: parseInt(match[2])
          };
        }
        return null;
      });

      // Light text should have high RGB values (> 150)
      expect(textColor).not.toBeNull();
      if (textColor) {
        const avgBrightness = (textColor.r + textColor.g + textColor.b) / 3;
        expect(avgBrightness).toBeGreaterThan(150);
      }
    });
  });

  test.describe('Light Mode Toggle', () => {
    test('should switch back to light mode when toggle is clicked again', async ({ page }) => {
      await page.goto('/');

      // Click to enable dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Click again to return to light mode
      await page.click('[data-testid="theme-toggle"]');

      // Verify light mode is applied
      const hasLightMode = await page.evaluate(() => {
        return !document.documentElement.classList.contains('dark') &&
               document.documentElement.getAttribute('data-theme') !== 'dark' &&
               !document.body.classList.contains('dark-mode');
      });
      expect(hasLightMode).toBe(true);
    });

    test('should have light background color in light mode', async ({ page }) => {
      await page.goto('/');

      // Ensure we're in light mode (toggle twice if needed)
      const isDarkMode = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });

      if (isDarkMode) {
        await page.click('[data-testid="theme-toggle"]');
      }

      // Verify body background is light
      const bgColor = await page.evaluate(() => {
        const rgb = getComputedStyle(document.body).backgroundColor;
        const match = rgb.match(/\d+/g);
        if (match) {
          return {
            r: parseInt(match[0]),
            g: parseInt(match[1]),
            b: parseInt(match[2])
          };
        }
        return null;
      });

      // Light background should have high RGB values (> 200)
      expect(bgColor).not.toBeNull();
      if (bgColor) {
        const avgBrightness = (bgColor.r + bgColor.g + bgColor.b) / 3;
        expect(avgBrightness).toBeGreaterThan(200);
      }
    });

    test('should have dark text color in light mode', async ({ page }) => {
      await page.goto('/');

      // Ensure we're in light mode
      const isDarkMode = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });

      if (isDarkMode) {
        await page.click('[data-testid="theme-toggle"]');
      }

      // Verify text color is dark
      const textColor = await page.evaluate(() => {
        const rgb = getComputedStyle(document.body).color;
        const match = rgb.match(/\d+/g);
        if (match) {
          return {
            r: parseInt(match[0]),
            g: parseInt(match[1]),
            b: parseInt(match[2])
          };
        }
        return null;
      });

      // Dark text should have low RGB values (< 100)
      expect(textColor).not.toBeNull();
      if (textColor) {
        const avgBrightness = (textColor.r + textColor.g + textColor.b) / 3;
        expect(avgBrightness).toBeLessThan(100);
      }
    });
  });

  test.describe('Code Blocks Readability', () => {
    test('code blocks should be readable in dark mode', async ({ page }) => {
      await page.goto('/');

      // Enable dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Navigate to quick-start section with code
      const codeBlock = page.locator('.code-block code').first();
      await expect(codeBlock).toBeVisible();

      // Check code is readable (has sufficient contrast)
      const codeColors = await page.evaluate(() => {
        const code = document.querySelector('.code-block code');
        if (!code) return null;
        const style = getComputedStyle(code);
        return {
          color: style.color,
          backgroundColor: style.backgroundColor
        };
      });

      expect(codeColors).not.toBeNull();
      // Code should have text color defined
      expect(codeColors?.color).toBeTruthy();
    });

    test('code blocks should be readable in light mode', async ({ page }) => {
      await page.goto('/');

      // Ensure light mode
      const isDarkMode = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark';
      });

      if (isDarkMode) {
        await page.click('[data-testid="theme-toggle"]');
      }

      // Navigate to quick-start section with code
      const codeBlock = page.locator('.code-block code').first();
      await expect(codeBlock).toBeVisible();

      // Check code is readable (has sufficient contrast)
      const codeColors = await page.evaluate(() => {
        const code = document.querySelector('.code-block code');
        if (!code) return null;
        const style = getComputedStyle(code);
        return {
          color: style.color,
          backgroundColor: style.backgroundColor
        };
      });

      expect(codeColors).not.toBeNull();
      // Code should have text color defined
      expect(codeColors?.color).toBeTruthy();
    });

    test('syntax highlighting should be visible in both modes', async ({ page }) => {
      await page.goto('/');

      // Check in light mode first
      const codeBlockLight = page.locator('.code-block').first();
      await expect(codeBlockLight).toBeVisible();

      // Toggle to dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Code block should still be visible
      const codeBlockDark = page.locator('.code-block').first();
      await expect(codeBlockDark).toBeVisible();
    });
  });

  test.describe('System Preference Detection', () => {
    test('should respect prefers-color-scheme: dark', async ({ page }) => {
      // Emulate dark color scheme preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Wait for page to apply theme based on system preference
      await page.waitForTimeout(100);

      // Check that dark mode is applied automatically
      const isDarkMode = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });

      expect(isDarkMode).toBe(true);
    });

    test('should respect prefers-color-scheme: light', async ({ page }) => {
      // Emulate light color scheme preference
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      // Wait for page to apply theme based on system preference
      await page.waitForTimeout(100);

      // Check that light mode is applied
      const isLightMode = await page.evaluate(() => {
        return !document.documentElement.classList.contains('dark') &&
               document.documentElement.getAttribute('data-theme') !== 'dark' &&
               !document.body.classList.contains('dark-mode');
      });

      expect(isLightMode).toBe(true);
    });
  });

  test.describe('Theme Persistence', () => {
    test('should persist dark mode preference after page refresh', async ({ page }) => {
      await page.goto('/');

      // Enable dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Verify dark mode is active
      let isDarkMode = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });
      expect(isDarkMode).toBe(true);

      // Refresh the page
      await page.reload();

      // Wait for page to load and apply saved theme
      await page.waitForTimeout(100);

      // Verify dark mode is still active
      isDarkMode = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });
      expect(isDarkMode).toBe(true);
    });

    test('should persist light mode preference after page refresh', async ({ page }) => {
      await page.goto('/');

      // Ensure light mode (toggle twice if in dark mode)
      const startInDark = await page.evaluate(() => {
        return document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.body.classList.contains('dark-mode');
      });

      if (startInDark) {
        await page.click('[data-testid="theme-toggle"]');
      }

      // Verify light mode is active
      let isLightMode = await page.evaluate(() => {
        return !document.documentElement.classList.contains('dark') &&
               document.documentElement.getAttribute('data-theme') !== 'dark' &&
               !document.body.classList.contains('dark-mode');
      });
      expect(isLightMode).toBe(true);

      // Refresh the page
      await page.reload();

      // Wait for page to load and apply saved theme
      await page.waitForTimeout(100);

      // Verify light mode is still active
      isLightMode = await page.evaluate(() => {
        return !document.documentElement.classList.contains('dark') &&
               document.documentElement.getAttribute('data-theme') !== 'dark' &&
               !document.body.classList.contains('dark-mode');
      });
      expect(isLightMode).toBe(true);
    });

    test('should store theme preference in localStorage', async ({ page }) => {
      await page.goto('/');

      // Enable dark mode
      await page.click('[data-testid="theme-toggle"]');

      // Check localStorage
      const storedTheme = await page.evaluate(() => {
        return localStorage.getItem('theme') || localStorage.getItem('color-theme') || localStorage.getItem('theme-preference');
      });

      expect(storedTheme).toBeTruthy();
    });
  });
});
