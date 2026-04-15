/**
 * Accessibility E2E Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA compliance including:
 * - axe-core accessibility audit
 * - Color contrast verification
 * - Keyboard navigation
 * - Focus indicators
 * - Skip navigation link
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Run axe accessibility audit
  test('should have no critical or serious accessibility violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    expect(criticalAndSerious).toEqual([]);
  });

  // Test Case 2: Check color contrast ratios
  test('should have proper color contrast ratios', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    expect(accessibilityScanResults.violations).toEqual([]);
  });

  // Test Case 3: Tab through all interactive elements
  test('should allow keyboard navigation through all interactive elements', async ({ page }) => {
    // Get all focusable elements
    const focusableElements = await page.locator(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    ).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Start from the skip link (first focusable element)
    await page.keyboard.press('Tab');

    // Track focused elements
    const focusedElements: string[] = [];

    for (let i = 0; i < focusableElements.length; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el?.tagName?.toLowerCase(),
          text: el?.textContent?.trim().substring(0, 50),
          ariaLabel: el?.getAttribute('aria-label'),
          href: el?.getAttribute('href'),
        };
      });

      focusedElements.push(
        `${activeElement.tagName}: ${activeElement.ariaLabel || activeElement.text || activeElement.href}`
      );

      await page.keyboard.press('Tab');
    }

    // Verify we can tab through elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify logical order - skip nav should be first
    expect(focusedElements[0]).toContain('Skip to main content');
  });

  // Test Case 4: Check focus visibility on each element
  test('should have visible focus indicators on all focusable elements', async ({ page }) => {
    // Test elements with focus styles
    const elementsToCheck = [
      'a.header__logo',
      'a.header__nav-link',
      'button#theme-toggle',
      'a.hero__cta',
      'button.code-block__copy',
    ];

    for (const selector of elementsToCheck) {
      const element = page.locator(selector).first();
      await element.focus();

      // Wait for focus to be applied
      await page.waitForTimeout(100);

      const focusStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
        };
      });

      // Verify there's a visible outline when focused
      const hasVisibleOutline =
        focusStyles.outlineStyle !== 'none' &&
        focusStyles.outlineWidth !== '0px' &&
        parseInt(focusStyles.outlineWidth) > 0;

      expect(hasVisibleOutline).toBe(true);
    }

    // Test skip-nav separately - it should become visible when focused
    const skipNav = page.locator('.skip-nav');
    await skipNav.focus();
    await page.waitForTimeout(100);

    // Verify the skip-nav has proper focus styles (outline)
    const skipNavFocusStyles = await skipNav.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });

    expect(skipNavFocusStyles.outlineStyle).not.toBe('none');
    expect(parseInt(skipNavFocusStyles.outlineWidth)).toBeGreaterThan(0);
  });

  // Test Case 7: Check for skip navigation link
  test('should have working skip navigation link', async ({ page }) => {
    // Find the skip navigation link
    const skipNav = page.locator('.skip-nav');
    await expect(skipNav).toBeAttached();

    // Verify skip nav text content
    await expect(skipNav).toHaveText('Skip to main content');

    // Verify skip nav links to main content
    await expect(skipNav).toHaveAttribute('href', '#main-content');

    // Tab to focus the skip link
    await page.keyboard.press('Tab');
    await expect(skipNav).toBeFocused();

    // Verify main content exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeAttached();

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Verify focus moved to main content or URL hash changed
    const url = page.url();
    expect(url).toContain('#main-content');
  });

  // Additional accessibility tests
  test('should have proper ARIA landmarks', async ({ page }) => {
    // Check for main landmark
    const main = page.locator('main');
    await expect(main).toBeAttached();

    // Check for header
    const header = page.locator('header');
    await expect(header).toBeAttached();

    // Check for navigation
    const nav = page.locator('nav[aria-label]');
    await expect(nav).toBeAttached();
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

    // Check for footer
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();
  });

  test('should not have any accessibility violations on light theme', async ({ page }) => {
    // Ensure light theme is active
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
    });

    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    expect(criticalAndSerious).toEqual([]);
  });

  test('should not have any accessibility violations on dark theme', async ({ page }) => {
    // Switch to dark theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
    });

    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    expect(criticalAndSerious).toEqual([]);
  });
});
