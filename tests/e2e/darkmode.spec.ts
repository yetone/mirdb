/**
 * Dark Mode E2E Tests
 * Owner: Scenario 15 - Dark Mode Theme
 *
 * Test coverage:
 * - Dark mode CSS variables
 * - Dark mode toggle (if implemented)
 * - prefers-color-scheme support
 * - Dark mode contrast ratios
 */

import { test, expect } from '@playwright/test';

test.describe('Dark Mode Theme', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Dark Mode CSS Variables', () => {
    test('should have dark mode CSS variables defined in :root', async ({ page }) => {
      // Check that dark mode CSS variables are defined
      const darkPrimary = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--dark-color-primary').trim();
      });
      expect(darkPrimary).toBeTruthy();
      expect(darkPrimary).toMatch(/^#[0-9a-fA-F]{6}$/);

      const darkBackground = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--dark-color-background').trim();
      });
      expect(darkBackground).toBeTruthy();
      expect(darkBackground).toMatch(/^#[0-9a-fA-F]{6}$/);

      const darkText = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--dark-color-text').trim();
      });
      expect(darkText).toBeTruthy();
      expect(darkText).toMatch(/^#[0-9a-fA-F]{6}$/);

      const darkBorder = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--dark-color-border').trim();
      });
      expect(darkBorder).toBeTruthy();
      expect(darkBorder).toMatch(/^#[0-9a-fA-F]{6}$/);
    });

    test('should have all required dark mode color variables', async ({ page }) => {
      const requiredVariables = [
        '--dark-color-primary',
        '--dark-color-primary-dark',
        '--dark-color-text',
        '--dark-color-text-light',
        '--dark-color-background',
        '--dark-color-background-alt',
        '--dark-color-border'
      ];

      for (const varName of requiredVariables) {
        const value = await page.evaluate((name) => {
          return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
        }, varName);
        expect(value, `CSS variable ${varName} should be defined`).toBeTruthy();
      }
    });
  });

  test.describe('Test Case 2: Dark Mode Toggle', () => {
    test('should have dark mode toggle button in the UI', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');
      await expect(toggle).toBeVisible();
    });

    test('toggle should have accessible label', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');
      await expect(toggle).toHaveAttribute('aria-label', 'Toggle dark mode');
    });

    test('toggle should switch theme when clicked', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');

      // Get initial theme
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      // Click toggle
      await toggle.click();

      // Theme should change
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      expect(newTheme).not.toBe(initialTheme);
      expect(['dark', 'light']).toContain(newTheme);
    });

    test('toggle should persist preference in localStorage', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');

      // Click toggle to set dark mode
      await toggle.click();

      // Check localStorage
      const storedTheme = await page.evaluate(() => {
        return localStorage.getItem('mirdb-theme');
      });

      expect(storedTheme).toBeTruthy();
      expect(['dark', 'light']).toContain(storedTheme);
    });

    test('should restore theme from localStorage on reload', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');

      // Set dark mode
      await page.evaluate(() => {
        localStorage.setItem('mirdb-theme', 'dark');
      });

      // Reload page
      await page.reload();

      // Check theme is applied
      const theme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      expect(theme).toBe('dark');
    });

    test('toggle should have moon icon in light mode', async ({ page }) => {
      // Ensure light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });

      const moonIcon = page.locator('[data-testid="dark-mode-toggle"] .icon-moon');
      const sunIcon = page.locator('[data-testid="dark-mode-toggle"] .icon-sun');

      // Moon icon should be visible in light mode
      await expect(moonIcon).toBeVisible();
      // Sun icon should be hidden in light mode
      await expect(sunIcon).not.toBeVisible();
    });

    test('toggle should have sun icon in dark mode', async ({ page }) => {
      // Set dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      const moonIcon = page.locator('[data-testid="dark-mode-toggle"] .icon-moon');
      const sunIcon = page.locator('[data-testid="dark-mode-toggle"] .icon-sun');

      // Sun icon should be visible in dark mode
      await expect(sunIcon).toBeVisible();
      // Moon icon should be hidden in dark mode
      await expect(moonIcon).not.toBeVisible();
    });
  });

  test.describe('Test Case 3: prefers-color-scheme Media Query Support', () => {
    test('should respond to system dark mode preference', async ({ page, context }) => {
      // Emulate dark color scheme
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.reload();

      // Clear any stored preference
      await page.evaluate(() => {
        localStorage.removeItem('mirdb-theme');
        document.documentElement.removeAttribute('data-theme');
      });

      // Wait for styles to apply
      await page.waitForTimeout(100);

      // Body should have dark background color
      const bgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Dark background should be dark (RGB values should be low)
      const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // In dark mode, background RGB values should be relatively low
        expect(r + g + b).toBeLessThan(200);
      }
    });

    test('should respond to system light mode preference', async ({ page }) => {
      // Emulate light color scheme
      await page.emulateMedia({ colorScheme: 'light' });
      await page.reload();

      // Clear any stored preference
      await page.evaluate(() => {
        localStorage.removeItem('mirdb-theme');
        document.documentElement.removeAttribute('data-theme');
      });

      // Wait for styles to apply
      await page.waitForTimeout(100);

      // Body should have light background color
      const bgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Light background RGB values should be high
      const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // In light mode, background RGB values should be high (near white)
        expect(r + g + b).toBeGreaterThan(600);
      }
    });

    test('manual theme preference should override system preference', async ({ page }) => {
      // Emulate system dark mode
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.reload();

      // Set manual light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });

      // Wait for styles to apply
      await page.waitForTimeout(100);

      // Body should have light background despite system dark mode
      const bgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Light mode should have high RGB values
        expect(r + g + b).toBeGreaterThan(600);
      }
    });

    test('CSS should include prefers-color-scheme media query', async ({ page }) => {
      // Check if CSS stylesheet includes prefers-color-scheme
      const hasMediaQuery = await page.evaluate(() => {
        const styleSheets = document.styleSheets;
        for (let i = 0; i < styleSheets.length; i++) {
          try {
            const rules = styleSheets[i].cssRules;
            for (let j = 0; j < rules.length; j++) {
              if (rules[j] instanceof CSSMediaRule) {
                const mediaRule = rules[j] as CSSMediaRule;
                if (mediaRule.conditionText && mediaRule.conditionText.includes('prefers-color-scheme')) {
                  return true;
                }
              }
            }
          } catch (e) {
            // Cross-origin stylesheet, skip
          }
        }
        return false;
      });

      expect(hasMediaQuery).toBe(true);
    });
  });

  test.describe('Test Case 4: Dark Mode Contrast Ratios (WCAG AA)', () => {
    /**
     * Calculate relative luminance of a color
     * @see https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
     */
    function getLuminance(r: number, g: number, b: number): number {
      const [rs, gs, bs] = [r, g, b].map(c => {
        const sRGB = c / 255;
        return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    /**
     * Calculate contrast ratio between two colors
     * @see https://www.w3.org/TR/WCAG21/#dfn-contrast-ratio
     */
    function getContrastRatio(l1: number, l2: number): number {
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    /**
     * Parse RGB color string to array of numbers
     */
    function parseRGB(rgbString: string): [number, number, number] | null {
      const match = rgbString.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
      }
      return null;
    }

    test('body text should have sufficient contrast against background in dark mode', async ({ page }) => {
      // Enable dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      const colors = await page.evaluate(() => {
        const body = document.body;
        const computedStyle = getComputedStyle(body);
        return {
          textColor: computedStyle.color,
          backgroundColor: computedStyle.backgroundColor
        };
      });

      const textRGB = parseRGB(colors.textColor);
      const bgRGB = parseRGB(colors.backgroundColor);

      if (textRGB && bgRGB) {
        const textLuminance = getLuminance(...textRGB);
        const bgLuminance = getLuminance(...bgRGB);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG AA requires 4.5:1 for normal text
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('hero title should have sufficient contrast in dark mode', async ({ page }) => {
      // Enable dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      const colors = await page.evaluate(() => {
        const title = document.querySelector('.hero-title');
        const hero = document.querySelector('.hero');
        if (title && hero) {
          return {
            textColor: getComputedStyle(title).color,
            backgroundColor: getComputedStyle(hero).backgroundColor
          };
        }
        return null;
      });

      if (colors) {
        const textRGB = parseRGB(colors.textColor);
        const bgRGB = parseRGB(colors.backgroundColor);

        if (textRGB && bgRGB) {
          const textLuminance = getLuminance(...textRGB);
          const bgLuminance = getLuminance(...bgRGB);
          const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

          // Large text (hero title) requires 3:1 contrast for WCAG AA
          expect(contrastRatio).toBeGreaterThanOrEqual(3);
        }
      }
    });

    test('links should have sufficient contrast in dark mode', async ({ page }) => {
      // Enable dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      const colors = await page.evaluate(() => {
        const link = document.querySelector('.nav-link');
        const header = document.querySelector('.header');
        if (link && header) {
          return {
            linkColor: getComputedStyle(link).color,
            backgroundColor: getComputedStyle(header).backgroundColor
          };
        }
        return null;
      });

      if (colors) {
        const linkRGB = parseRGB(colors.linkColor);
        const bgRGB = parseRGB(colors.backgroundColor);

        if (linkRGB && bgRGB) {
          const linkLuminance = getLuminance(...linkRGB);
          const bgLuminance = getLuminance(...bgRGB);
          const contrastRatio = getContrastRatio(linkLuminance, bgLuminance);

          // Links should meet WCAG AA contrast (4.5:1)
          expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    test('feature card text should have sufficient contrast in dark mode', async ({ page }) => {
      // Enable dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      const colors = await page.evaluate(() => {
        const card = document.querySelector('.feature-card');
        const cardText = document.querySelector('.feature-card p');
        if (card && cardText) {
          return {
            textColor: getComputedStyle(cardText).color,
            backgroundColor: getComputedStyle(card).backgroundColor
          };
        }
        return null;
      });

      if (colors) {
        const textRGB = parseRGB(colors.textColor);
        const bgRGB = parseRGB(colors.backgroundColor);

        if (textRGB && bgRGB) {
          const textLuminance = getLuminance(...textRGB);
          const bgLuminance = getLuminance(...bgRGB);
          const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

          // Feature card text should meet WCAG AA contrast (4.5:1)
          expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    test('CTA button text should have sufficient contrast in dark mode', async ({ page }) => {
      // Enable dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      const colors = await page.evaluate(() => {
        const cta = document.querySelector('.hero-cta');
        if (cta) {
          return {
            textColor: getComputedStyle(cta).color,
            backgroundColor: getComputedStyle(cta).backgroundColor
          };
        }
        return null;
      });

      if (colors) {
        const textRGB = parseRGB(colors.textColor);
        const bgRGB = parseRGB(colors.backgroundColor);

        if (textRGB && bgRGB) {
          const textLuminance = getLuminance(...textRGB);
          const bgLuminance = getLuminance(...bgRGB);
          const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

          // CTA button text should meet WCAG AA contrast (4.5:1)
          expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
        }
      }
    });
  });

  test.describe('Additional Dark Mode Tests', () => {
    test('toggle button should have visible focus indicator', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');

      // Tab to the toggle button
      await toggle.focus();

      // Check for focus outline
      const outline = await toggle.evaluate((el) => {
        const style = getComputedStyle(el);
        return style.outlineStyle !== 'none' || style.outline !== 'none';
      });

      expect(outline).toBe(true);
    });

    test('dark mode should affect all major sections', async ({ page }) => {
      // Enable dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      // Check that major sections have dark backgrounds
      const sections = ['.hero', '.features', '.quick-start', '.status', '.footer'];

      for (const selector of sections) {
        const bgColor = await page.evaluate((sel) => {
          const el = document.querySelector(sel);
          return el ? getComputedStyle(el).backgroundColor : null;
        }, selector);

        if (bgColor) {
          const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
          if (rgbMatch) {
            const [, r, g, b] = rgbMatch.map(Number);
            // Dark mode sections should have relatively dark backgrounds
            expect(r + g + b, `${selector} should have dark background`).toBeLessThan(300);
          }
        }
      }
    });

    test('keyboard navigation should work for toggle', async ({ page }) => {
      const toggle = page.locator('[data-testid="dark-mode-toggle"]');

      // Focus the toggle
      await toggle.focus();

      // Get initial theme
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      // Press Enter to toggle
      await page.keyboard.press('Enter');

      // Theme should change
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      expect(newTheme).not.toBe(initialTheme);
    });
  });
});
