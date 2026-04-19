/**
 * Dark Mode E2E Tests
 * Owner: Scenario 10 - Dark Mode Theme Support
 *
 * Tests for NFR-3: Must support dark mode following system settings.
 * Verifies that the homepage adapts colors based on prefers-color-scheme
 * and maintains WCAG 2.1 AA contrast ratios.
 */

import { test, expect } from '@playwright/test';

// Base URL for tests - use environment variable or default to localhost:3000
const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';

/**
 * Helper to calculate relative luminance (WCAG 2.1)
 * @param {number} r - Red (0-255)
 * @param {number} g - Green (0-255)
 * @param {number} b - Blue (0-255)
 * @returns {number} Relative luminance
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Helper to calculate contrast ratio between two colors
 * @param {string} color1 - RGB color string like "rgb(255, 255, 255)"
 * @param {string} color2 - RGB color string
 * @returns {number} Contrast ratio
 */
function getContrastRatio(color1, color2) {
  const parseRGB = (str) => {
    const match = str.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (!match) return [0, 0, 0];
    return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
  };

  const [r1, g1, b1] = parseRGB(color1);
  const [r2, g2, b2] = parseRGB(color2);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Dark Mode Theme Support', () => {

  test.describe('Test Case 1: Dark mode detection', () => {
    test('page background changes to dark color when system prefers dark mode', async ({ page }) => {
      // Emulate dark color scheme
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Get body background color
      const backgroundColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Parse RGB values
      const rgbMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(rgbMatch).toBeTruthy();

      const [, r, g, b] = rgbMatch.map(Number);

      // Dark background should have low luminance (dark colors)
      const luminance = getLuminance(r, g, b);
      expect(luminance).toBeLessThan(0.2);
    });

    test('text changes to light color in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Get text color from body
      const textColor = await page.evaluate(() => {
        return getComputedStyle(document.body).color;
      });

      // Parse RGB values
      const rgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(rgbMatch).toBeTruthy();

      const [, r, g, b] = rgbMatch.map(Number);

      // Light text should have high luminance
      const luminance = getLuminance(r, g, b);
      expect(luminance).toBeGreaterThan(0.5);
    });
  });

  test.describe('Test Case 2: Light mode (default)', () => {
    test('page uses light background with dark text by default', async ({ page }) => {
      // Emulate light color scheme (default)
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto(BASE_URL);

      // Get body background color
      const backgroundColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Get text color
      const textColor = await page.evaluate(() => {
        return getComputedStyle(document.body).color;
      });

      // Parse background RGB
      const bgMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(bgMatch).toBeTruthy();
      const [, bgR, bgG, bgB] = bgMatch.map(Number);

      // Parse text RGB
      const textMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(textMatch).toBeTruthy();
      const [, textR, textG, textB] = textMatch.map(Number);

      // Light background should have high luminance
      const bgLuminance = getLuminance(bgR, bgG, bgB);
      expect(bgLuminance).toBeGreaterThan(0.8);

      // Dark text should have low luminance
      const textLuminance = getLuminance(textR, textG, textB);
      expect(textLuminance).toBeLessThan(0.2);
    });
  });

  test.describe('Test Case 3: Dark mode code block styling', () => {
    test('code blocks maintain readability in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Find code blocks
      const codeBlock = page.locator('.code-block__content, .quickstart__command, pre code').first();
      await expect(codeBlock).toBeVisible();

      // Get code block styles
      const styles = await codeBlock.evaluate((el) => {
        const computed = getComputedStyle(el);
        return {
          backgroundColor: computed.backgroundColor,
          color: computed.color
        };
      });

      // Calculate contrast ratio
      const contrast = getContrastRatio(styles.color, styles.backgroundColor);

      // Should meet WCAG AA requirement (4.5:1 for normal text)
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    });

    test('code blocks have appropriate dark background', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Check code block background
      const codeBlock = page.locator('.code-block__content, pre').first();
      await expect(codeBlock).toBeVisible();

      const backgroundColor = await codeBlock.evaluate((el) => {
        return getComputedStyle(el).backgroundColor;
      });

      // Parse RGB values
      const rgbMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(rgbMatch).toBeTruthy();

      const [, r, g, b] = rgbMatch.map(Number);
      const luminance = getLuminance(r, g, b);

      // Code blocks should have dark background in dark mode
      expect(luminance).toBeLessThan(0.3);
    });
  });

  test.describe('Test Case 4: Dark mode contrast ratios', () => {
    test('hero title maintains 4.5:1 contrast ratio in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();

      const styles = await heroTitle.evaluate((el) => {
        const computed = getComputedStyle(el);
        // Get background from closest ancestor with a background
        let bgColor = 'rgb(255, 255, 255)';
        let current = el;
        while (current && current !== document.body) {
          const bg = getComputedStyle(current).backgroundColor;
          if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
            bgColor = bg;
            break;
          }
          current = current.parentElement;
        }
        if (bgColor === 'rgb(255, 255, 255)' || bgColor === 'rgba(0, 0, 0, 0)') {
          bgColor = getComputedStyle(document.body).backgroundColor;
        }
        return {
          color: computed.color,
          backgroundColor: bgColor
        };
      });

      const contrast = getContrastRatio(styles.color, styles.backgroundColor);
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    });

    test('navigation links maintain proper contrast in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Check header links
      const navLink = page.locator('.header__link').first();
      await expect(navLink).toBeVisible();

      const styles = await navLink.evaluate((el) => {
        const computed = getComputedStyle(el);
        // Get background from header
        const header = el.closest('.header');
        const bgColor = header ? getComputedStyle(header).backgroundColor : getComputedStyle(document.body).backgroundColor;
        return {
          color: computed.color,
          backgroundColor: bgColor
        };
      });

      const contrast = getContrastRatio(styles.color, styles.backgroundColor);
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    });

    test('feature card text maintains proper contrast in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      const featureDesc = page.locator('.feature-card__description').first();
      await expect(featureDesc).toBeVisible();

      const styles = await featureDesc.evaluate((el) => {
        const computed = getComputedStyle(el);
        // Get background from card
        const card = el.closest('.feature-card');
        let bgColor = 'rgb(30, 41, 59)'; // default dark surface
        if (card) {
          const cardBg = getComputedStyle(card).backgroundColor;
          if (cardBg && cardBg !== 'rgba(0, 0, 0, 0)' && cardBg !== 'transparent') {
            bgColor = cardBg;
          }
        }
        return {
          color: computed.color,
          backgroundColor: bgColor
        };
      });

      const contrast = getContrastRatio(styles.color, styles.backgroundColor);
      // WCAG AA requires 4.5:1 for normal text
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    });

    test('buttons maintain proper contrast in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Check primary button
      const primaryBtn = page.locator('.btn-primary, .btn.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      const styles = await primaryBtn.evaluate((el) => {
        const computed = getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor
        };
      });

      const contrast = getContrastRatio(styles.color, styles.backgroundColor);
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    });
  });

  test.describe('Test Case 5: CSS custom properties for theming', () => {
    test('CSS variables update based on color-scheme', async ({ page }) => {
      // Test light mode variables
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto(BASE_URL);

      const lightVars = await page.evaluate(() => {
        const root = document.documentElement;
        const computed = getComputedStyle(root);
        return {
          background: computed.getPropertyValue('--color-background').trim(),
          text: computed.getPropertyValue('--color-text').trim(),
          surface: computed.getPropertyValue('--color-surface').trim()
        };
      });

      // Verify light mode has light background color
      expect(lightVars.background).toMatch(/^#[fF]/); // Starts with high hex value (light)

      // Switch to dark mode
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      const darkVars = await page.evaluate(() => {
        const root = document.documentElement;
        const computed = getComputedStyle(root);
        return {
          background: computed.getPropertyValue('--color-background').trim(),
          text: computed.getPropertyValue('--color-text').trim(),
          surface: computed.getPropertyValue('--color-surface').trim()
        };
      });

      // Verify dark mode has dark background color
      expect(darkVars.background).toMatch(/^#[0-3]/); // Starts with low hex value (dark)

      // Verify variables are different between modes
      expect(lightVars.background).not.toBe(darkVars.background);
      expect(lightVars.text).not.toBe(darkVars.text);
    });

    test('theme data attribute is set correctly', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto(BASE_URL);

      // Wait for JS to initialize
      await page.waitForFunction(() => document.documentElement.dataset.theme !== undefined);

      const theme = await page.evaluate(() => document.documentElement.dataset.theme);
      expect(theme).toBe('dark');

      // Switch to light mode
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto(BASE_URL);

      await page.waitForFunction(() => document.documentElement.dataset.theme !== undefined);

      const lightTheme = await page.evaluate(() => document.documentElement.dataset.theme);
      expect(lightTheme).toBe('light');
    });
  });

});
