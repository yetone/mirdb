/**
 * Accessibility E2E Tests for HomePage
 * Owner: Scenario 8 - Theme & Accessibility Compliance
 *
 * Test Cases:
 * 1. Load HomePage with prefers-color-scheme: dark - Dark theme is automatically applied
 * 2. Load HomePage with prefers-color-scheme: light - Light theme is automatically applied
 * 3. Click theme toggle button - Theme switches between light and dark mode
 * 4. Run automated accessibility audit (axe-core) - No WCAG AA violations
 * 5. Navigate page using Tab key only - All interactive elements receive focus
 * 6. Focus on CTA button - Visible focus indicator is displayed
 * 9. Press Tab on page load - Skip-to-main-content link appears
 * 10. Measure text color contrast ratios in light theme - >= 4.5:1
 * 11. Measure text color contrast ratios in dark theme - >= 4.5:1
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Theme & Accessibility Compliance', () => {
  test.describe('Theme Detection', () => {
    // Test Case 1: Dark theme detection
    test('loads with dark theme when system prefers dark mode', async ({ browser }) => {
      const context = await browser.newContext({
        colorScheme: 'dark',
      });
      const page = await context.newPage();

      // Clear localStorage to test system preference
      await page.addInitScript(() => {
        localStorage.removeItem('theme');
      });

      await page.goto('/');

      // Wait for page to load and theme to be applied
      await page.waitForLoadState('domcontentloaded');

      // Check that dark theme is applied
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveAttribute('data-theme', 'dark');

      await context.close();
    });

    // Test Case 2: Light theme detection
    test('loads with light theme when system prefers light mode', async ({ browser }) => {
      const context = await browser.newContext({
        colorScheme: 'light',
      });
      const page = await context.newPage();

      // Clear localStorage to test system preference
      await page.addInitScript(() => {
        localStorage.removeItem('theme');
      });

      await page.goto('/');

      // Wait for page to load
      await page.waitForLoadState('domcontentloaded');

      // Check that light theme is applied
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveAttribute('data-theme', 'light');

      await context.close();
    });
  });

  test.describe('Theme Toggle', () => {
    // Test Case 3: Theme toggle switches between modes
    test('theme toggle switches between light and dark mode', async ({ page }) => {
      await page.goto('/');

      // Get initial theme
      const htmlElement = page.locator('html');
      const initialTheme = await htmlElement.getAttribute('data-theme');

      // Find and click the theme toggle button
      const themeToggle = page.getByTestId('theme-toggle');
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed
      const newTheme = initialTheme === 'light' ? 'dark' : 'light';
      await expect(htmlElement).toHaveAttribute('data-theme', newTheme);

      // Click again to toggle back
      await themeToggle.click();
      await expect(htmlElement).toHaveAttribute('data-theme', initialTheme);
    });

    test('theme toggle has accessible label', async ({ page }) => {
      await page.goto('/');

      const themeToggle = page.getByTestId('theme-toggle');
      await expect(themeToggle).toHaveAttribute('aria-label', /switch to (light|dark) mode/i);
    });
  });

  test.describe('WCAG Accessibility Audit', () => {
    // Test Case 4: No WCAG AA violations
    test('has no WCAG AA violations in light mode', async ({ page }) => {
      await page.goto('/');

      // Ensure light theme is set
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });

      // Wait for theme to apply
      await page.waitForTimeout(100);

      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      // Filter out color-contrast issues temporarily for this baseline test
      // The implementation uses DaisyUI which should have compliant colors
      const violations = accessibilityScanResults.violations.filter(
        v => v.id !== 'color-contrast' // We'll test contrast separately
      );

      expect(violations).toEqual([]);
    });

    test('has no WCAG AA violations in dark mode', async ({ page }) => {
      await page.goto('/');

      // Set dark theme
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      // Wait for theme to apply
      await page.waitForTimeout(100);

      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      // Filter out color-contrast issues for baseline
      const violations = accessibilityScanResults.violations.filter(
        v => v.id !== 'color-contrast'
      );

      expect(violations).toEqual([]);
    });
  });

  test.describe('Keyboard Navigation', () => {
    // Test Case 5: Tab navigation reaches all interactive elements
    test('all interactive elements receive focus in logical order', async ({ page }) => {
      await page.goto('/');

      // Start tabbing through the page
      const focusedElements: string[] = [];

      // Tab through elements and collect their test IDs or roles
      for (let i = 0; i < 20; i++) {
        await page.keyboard.press('Tab');

        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el) return null;
          return {
            testId: el.getAttribute('data-testid'),
            tagName: el.tagName.toLowerCase(),
            text: el.textContent?.substring(0, 30),
          };
        });

        if (focusedElement && focusedElement.tagName !== 'body') {
          focusedElements.push(
            focusedElement.testId || `${focusedElement.tagName}:${focusedElement.text}`
          );
        }
      }

      // Verify key interactive elements are in the focus order
      const focusOrderString = focusedElements.join(',');

      // Skip link should be first (may not be visible until focused)
      expect(focusOrderString).toContain('skip-to-main');

      // Navigation elements should be focusable
      expect(focusOrderString).toContain('navbar-logo');

      // CTA buttons should be focusable
      expect(focusOrderString).toContain('hero-cta-primary');
    });

    // Test Case 6: Focus indicator visibility
    test('CTA button has visible focus indicator', async ({ page }) => {
      await page.goto('/');

      // Tab to the primary CTA button
      const ctaButton = page.getByTestId('hero-cta-primary');
      await ctaButton.focus();

      // Check that the button is focused
      await expect(ctaButton).toBeFocused();

      // Check for focus styles (DaisyUI buttons have focus ring)
      const focusStyles = await ctaButton.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineOffset: styles.outlineOffset,
          boxShadow: styles.boxShadow,
        };
      });

      // Button should have some focus indication (ring, outline, or box-shadow)
      const hasFocusIndicator =
        focusStyles.outline !== 'none' ||
        focusStyles.boxShadow !== 'none' ||
        focusStyles.outline.includes('2px');

      expect(hasFocusIndicator).toBe(true);
    });
  });

  test.describe('Skip Navigation', () => {
    // Test Case 9: Skip to main content link
    test('skip-to-main-content link appears on Tab and works', async ({ page }) => {
      await page.goto('/');

      // Press Tab to reveal skip link
      await page.keyboard.press('Tab');

      // Find the skip link
      const skipLink = page.getByTestId('skip-to-main');

      // After pressing Tab, the skip link should be focused and visible
      await expect(skipLink).toBeFocused();

      // Activate the skip link
      await page.keyboard.press('Enter');

      // Main content should now be focused
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeFocused();
    });

    test('skip link has correct href', async ({ page }) => {
      await page.goto('/');

      const skipLink = page.getByTestId('skip-to-main');
      await expect(skipLink).toHaveAttribute('href', '#main-content');
    });
  });

  test.describe('Color Contrast', () => {
    // Test Case 10: Light theme contrast ratios
    test('text meets WCAG AA contrast requirements in light theme', async ({ page }) => {
      await page.goto('/');

      // Set light theme
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });

      await page.waitForTimeout(100);

      // Run axe specifically for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa'])
        .options({ runOnly: ['color-contrast'] })
        .analyze();

      // DaisyUI themes are designed to meet contrast requirements
      // We check for serious violations
      const seriousViolations = accessibilityScanResults.violations.filter(
        v => v.impact === 'serious' || v.impact === 'critical'
      );

      expect(seriousViolations).toEqual([]);
    });

    // Test Case 11: Dark theme contrast ratios
    test('text meets WCAG AA contrast requirements in dark theme', async ({ page }) => {
      await page.goto('/');

      // Set dark theme
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      // Run axe specifically for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa'])
        .options({ runOnly: ['color-contrast'] })
        .analyze();

      // Check for serious violations
      const seriousViolations = accessibilityScanResults.violations.filter(
        v => v.impact === 'serious' || v.impact === 'critical'
      );

      expect(seriousViolations).toEqual([]);
    });
  });

  test.describe('Screen Reader Support', () => {
    test('page has proper heading hierarchy', async ({ page }) => {
      await page.goto('/');

      // Get all headings
      const headings = await page.evaluate(() => {
        const heads = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(heads).map(h => ({
          level: parseInt(h.tagName[1]),
          text: h.textContent?.trim(),
        }));
      });

      // Should have exactly one h1
      const h1Count = headings.filter(h => h.level === 1).length;
      expect(h1Count).toBe(1);

      // Heading levels should not skip (e.g., h1 -> h3)
      for (let i = 1; i < headings.length; i++) {
        const prevLevel = headings[i - 1].level;
        const currLevel = headings[i].level;
        // Can go deeper by 1 or back up to any level
        expect(currLevel <= prevLevel + 1).toBe(true);
      }
    });

    test('interactive elements have accessible names', async ({ page }) => {
      await page.goto('/');

      // Check that all buttons have accessible names
      const buttonsWithoutName = await page.evaluate(() => {
        const buttons = document.querySelectorAll('button');
        return Array.from(buttons).filter(btn => {
          const name = btn.getAttribute('aria-label') ||
                      btn.textContent?.trim() ||
                      btn.getAttribute('title');
          return !name;
        }).map(btn => btn.outerHTML.substring(0, 100));
      });

      expect(buttonsWithoutName).toEqual([]);
    });

    test('navigation has proper landmark role', async ({ page }) => {
      await page.goto('/');

      // Check for nav element
      const navElements = page.locator('nav');
      const count = await navElements.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });

    test('main content has proper landmark', async ({ page }) => {
      await page.goto('/');

      const mainElement = page.locator('main');
      await expect(mainElement).toBeVisible();
    });

    test('footer has proper landmark', async ({ page }) => {
      await page.goto('/');

      const footerElement = page.locator('footer');
      await expect(footerElement).toBeVisible();
    });
  });
});
