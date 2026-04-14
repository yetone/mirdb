/**
 * Dark Mode E2E Tests
 * Owner: Scenario 13 - Dark Mode Support
 *
 * Tests for:
 * - Light mode preference detection
 * - Dark mode preference detection
 * - Code block styling in both modes
 * - Text contrast in dark mode
 */
import { test, expect } from '@playwright/test';
import { SELECTORS } from '../fixtures/test-data';

test.describe('Dark Mode Support', () => {
  test.describe('TC1: Light Mode Preference', () => {
    test('Page displays in light color scheme when system prefers light mode', async ({ page }) => {
      // Emulate light mode preference
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      // Check body background color is light (white or near-white)
      const bodyBgColor = await page.evaluate(() => {
        return window.getComputedStyle(document.body).backgroundColor;
      });

      // Parse RGB values - light mode should have high RGB values (near 255)
      const rgbMatch = bodyBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(rgbMatch).toBeTruthy();
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Light background should have high values (above 200)
        expect(r).toBeGreaterThan(200);
        expect(g).toBeGreaterThan(200);
        expect(b).toBeGreaterThan(200);
      }

      // Check text color is dark
      const textColor = await page.evaluate(() => {
        const hero = document.querySelector('.hero-title');
        if (hero) {
          return window.getComputedStyle(hero).color;
        }
        return null;
      });

      expect(textColor).toBeTruthy();
      const textRgbMatch = textColor?.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (textRgbMatch) {
        const [, r, g, b] = textRgbMatch.map(Number);
        // Dark text should have low values (below 100)
        expect(r).toBeLessThan(100);
        expect(g).toBeLessThan(100);
        expect(b).toBeLessThan(100);
      }
    });
  });

  test.describe('TC2: Dark Mode Preference', () => {
    test('Page displays in dark color scheme with appropriate contrast when system prefers dark mode', async ({ page }) => {
      // Emulate dark mode preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check body background color is dark
      const bodyBgColor = await page.evaluate(() => {
        return window.getComputedStyle(document.body).backgroundColor;
      });

      // Parse RGB values - dark mode should have low RGB values (near 0)
      const rgbMatch = bodyBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      expect(rgbMatch).toBeTruthy();
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Dark background should have low values (below 50)
        expect(r).toBeLessThan(50);
        expect(g).toBeLessThan(50);
        expect(b).toBeLessThan(50);
      }

      // Check that data-theme attribute is set to dark
      const dataTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });
      expect(dataTheme).toBe('dark');
    });

    test('Text colors have appropriate contrast in dark mode', async ({ page }) => {
      // Emulate dark mode preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check text color is light for readability
      const textColor = await page.evaluate(() => {
        const hero = document.querySelector('.hero-title');
        if (hero) {
          return window.getComputedStyle(hero).color;
        }
        return null;
      });

      expect(textColor).toBeTruthy();
      const textRgbMatch = textColor?.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (textRgbMatch) {
        const [, r, g, b] = textRgbMatch.map(Number);
        // Light text in dark mode should have high values (above 180)
        expect(r).toBeGreaterThan(180);
        expect(g).toBeGreaterThan(180);
        expect(b).toBeGreaterThan(180);
      }
    });
  });

  test.describe('TC3: Code Blocks in Dark Mode', () => {
    test('Code blocks are readable with appropriate syntax highlighting colors', async ({ page }) => {
      // Emulate dark mode preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Navigate to quick-start section where code blocks exist
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Check that code blocks exist
      const codeBlocks = page.locator(SELECTORS.codeBlock);
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Check the first code block styling
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();

      // Get code content element
      const codeContent = firstCodeBlock.locator('.code-content');
      await expect(codeContent).toBeVisible();

      // Check that code has readable text color
      const codeTextColor = await codeContent.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Text should be visible (not too dark against dark background)
      const codeRgbMatch = codeTextColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (codeRgbMatch) {
        const [, r, g, b] = codeRgbMatch.map(Number);
        // Code text should be light enough to read (above 150)
        expect(Math.max(r, g, b)).toBeGreaterThan(150);
      }
    });

    test('Code blocks have appropriate background in dark mode', async ({ page }) => {
      // Emulate dark mode preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Navigate to quick-start section
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      // Get code block background
      const codeBlock = page.locator(SELECTORS.codeBlock).first();
      const codeContent = codeBlock.locator('.code-content');

      const bgColor = await codeContent.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Code block background should be dark
      const bgRgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (bgRgbMatch) {
        const [, r, g, b] = bgRgbMatch.map(Number);
        // Dark background should have low values
        expect(r).toBeLessThan(80);
        expect(g).toBeLessThan(80);
        expect(b).toBeLessThan(80);
      }
    });
  });

  test.describe('TC4: Contrast in Dark Mode', () => {
    test('Text maintains readable contrast in dark mode', async ({ page }) => {
      // Emulate dark mode preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Test various text elements for contrast
      const elementsToCheck = [
        { selector: '.hero-title', description: 'Hero title' },
        { selector: '.hero-tagline', description: 'Hero tagline' },
        { selector: '.features-title', description: 'Features title' },
        { selector: '.feature-title', description: 'Feature card title' },
        { selector: '.feature-description', description: 'Feature description' },
      ];

      for (const { selector, description } of elementsToCheck) {
        const element = page.locator(selector).first();

        // Skip if element doesn't exist
        const elementExists = await element.count() > 0;
        if (!elementExists) continue;

        await element.scrollIntoViewIfNeeded();

        // Get computed styles
        const styles = await element.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            color: computed.color,
            backgroundColor: computed.backgroundColor,
          };
        });

        // Parse text color
        const textRgbMatch = styles.color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        if (textRgbMatch) {
          const [, r, g, b] = textRgbMatch.map(Number);
          // Calculate luminance (simplified)
          const textLuminance = (r + g + b) / 3;

          // In dark mode, text should be light (high luminance)
          expect(textLuminance).toBeGreaterThan(100);
        }
      }
    });

    test('Links have visible contrast in dark mode', async ({ page }) => {
      // Emulate dark mode preference
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check primary CTA button
      const ctaPrimary = page.locator('.cta-primary').first();
      await expect(ctaPrimary).toBeVisible();

      const linkColor = await ctaPrimary.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Link should be visible
      const linkRgbMatch = linkColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (linkRgbMatch) {
        const [, r, g, b] = linkRgbMatch.map(Number);
        // Links should have visible color (not too dark)
        expect(Math.max(r, g, b)).toBeGreaterThan(100);
      }
    });
  });

  test.describe('Theme Transition', () => {
    test('Theme transitions smoothly when system preference changes', async ({ page }) => {
      // Start with light mode
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      // Verify light mode is active
      let dataTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });
      expect(dataTheme).toBe('light');

      // Switch to dark mode
      await page.emulateMedia({ colorScheme: 'dark' });

      // Wait a moment for the JS to process the change
      await page.waitForTimeout(100);

      // Verify dark mode is now active
      dataTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });
      expect(dataTheme).toBe('dark');
    });
  });
});
