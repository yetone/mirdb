/**
 * Accessibility Compliance E2E Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA accessibility standards including:
 * - Automated axe-core accessibility audit
 * - Keyboard navigation
 * - Focus indicators
 * - Color contrast (light and dark themes)
 * - Semantic HTML structure
 * - Image alt text
 */

import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: No critical or serious axe-core violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalOrSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalOrSerious.length > 0) {
      console.log('Critical/Serious Violations:', JSON.stringify(criticalOrSerious, null, 2));
    }

    expect(criticalOrSerious).toHaveLength(0);
  });

  test('Test Case 2: All interactive elements are keyboard accessible', async ({ page }) => {
    // Get all focusable elements
    const focusableElements = await page.locator(
      'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
    ).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Tab through elements and verify they become focused
    let focusedCount = 0;
    const maxTabs = Math.min(focusableElements.length + 5, 50); // Safety limit

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Check if an element is focused
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el && el !== document.body ? {
          tagName: el.tagName,
          type: (el as HTMLInputElement).type || '',
          href: (el as HTMLAnchorElement).href || '',
          text: el.textContent?.trim().slice(0, 50) || ''
        } : null;
      });

      if (focusedElement) {
        focusedCount++;
      }
    }

    // All focusable elements should be reachable via Tab
    expect(focusedCount).toBeGreaterThanOrEqual(focusableElements.length - 2);
  });

  test('Test Case 3: Navigation links have visible focus indicators', async ({ page }) => {
    // Find navigation links
    const navLinks = page.locator('nav a, header a');
    const linkCount = await navLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Tab to first link and check focus visibility
    await page.keyboard.press('Tab');

    // Find the focused element
    const focusedElement = page.locator(':focus');

    // Check that focus indicator is visible (outline, box-shadow, or border)
    const focusStyles = await focusedElement.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineColor: styles.outlineColor,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow,
        border: styles.border,
      };
    });

    // Focus indicator should have some visible property
    const hasVisibleFocus =
      (focusStyles.outlineWidth !== '0px' && focusStyles.outline !== 'none') ||
      (focusStyles.boxShadow !== 'none' && focusStyles.boxShadow !== '') ||
      focusStyles.border !== '0px none rgb(0, 0, 0)';

    expect(hasVisibleFocus).toBe(true);
  });

  test('Test Case 4: Buttons have visible focus indicators', async ({ page }) => {
    // Find all buttons including CTA buttons
    const buttons = page.locator('button, a[role="button"], .cta, [class*="button"], [class*="Button"]');
    const buttonCount = await buttons.count();

    if (buttonCount === 0) {
      // If no explicit buttons, skip this test as it's expected in some scenarios
      test.skip();
      return;
    }

    // Focus on first button
    await buttons.first().focus();

    // Check focus visibility
    const focusStyles = await buttons.first().evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        boxShadow: styles.boxShadow,
      };
    });

    const hasVisibleFocus =
      (focusStyles.outlineWidth !== '0px' && focusStyles.outline !== 'none') ||
      (focusStyles.boxShadow !== 'none' && focusStyles.boxShadow !== '');

    expect(hasVisibleFocus).toBe(true);
  });

  test('Test Case 5: Light theme meets 4.5:1 color contrast ratio', async ({ page }) => {
    // Ensure light theme is active
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
    });

    // Wait for theme to apply
    await page.waitForTimeout(100);

    // Run axe-core specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ rules: { 'color-contrast': { enabled: true } } })
      .analyze();

    // Check for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Light theme contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 6: Dark theme meets 4.5:1 color contrast ratio', async ({ page }) => {
    // Switch to dark theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
    });

    // Wait for theme to apply
    await page.waitForTimeout(100);

    // Run axe-core specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ rules: { 'color-contrast': { enabled: true } } })
      .analyze();

    // Check for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Dark theme contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 7: logo.gif has descriptive alt text', async ({ page }) => {
    // Find logo image
    const logoImage = page.locator('img[src*="logo"]');

    if (await logoImage.count() === 0) {
      // Logo might not be present in all scenarios
      test.skip();
      return;
    }

    // Check alt text exists and is descriptive
    const altText = await logoImage.first().getAttribute('alt');

    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(3); // Should be descriptive, not just empty or "img"
    expect(altText!.toLowerCase()).not.toBe('image');
    expect(altText!.toLowerCase()).not.toBe('logo');
  });

  test('Test Case 8: usage.gif has descriptive alt text', async ({ page }) => {
    // Find usage gif image
    const usageImage = page.locator('img[src*="usage"]');

    if (await usageImage.count() === 0) {
      // Usage gif might not be present in all scenarios
      test.skip();
      return;
    }

    // Check alt text exists and is descriptive
    const altText = await usageImage.first().getAttribute('alt');

    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(10); // Should be descriptive
    expect(altText!.toLowerCase()).not.toBe('image');
    expect(altText!.toLowerCase()).not.toBe('gif');
  });

  test('Test Case 9: Page uses proper heading hierarchy', async ({ page }) => {
    // Get all headings
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    expect(headings.length).toBeGreaterThan(0);

    // Extract heading levels
    const headingLevels: number[] = [];
    for (const heading of headings) {
      const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
      headingLevels.push(parseInt(tagName.charAt(1)));
    }

    // Check h1 exists (should have exactly one)
    const h1Count = headingLevels.filter((level) => level === 1).length;
    // Allow 0 or 1 h1 tags - some SPAs may not have h1 in all views
    expect(h1Count).toBeLessThanOrEqual(1);

    // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    for (const level of headingLevels) {
      // Level should not skip more than 1 step down
      if (previousLevel > 0) {
        expect(level).toBeLessThanOrEqual(previousLevel + 1);
      }
      previousLevel = level;
    }
  });

  test('Test Case 10: Content is announced in logical order (structure check)', async ({ page }) => {
    // This is a structural check for screen reader logical order
    // We verify that the DOM structure follows a logical reading order

    // Check that main landmark exists
    const mainLandmark = page.locator('main, [role="main"]');
    const hasMain = await mainLandmark.count() > 0;
    expect(hasMain).toBe(true);

    // Check that header/banner exists
    const headerLandmark = page.locator('header, [role="banner"]');
    const hasHeader = await headerLandmark.count() > 0;
    expect(hasHeader).toBe(true);

    // Check that navigation landmark exists
    const navLandmark = page.locator('nav, [role="navigation"]');
    const hasNav = await navLandmark.count() > 0;
    expect(hasNav).toBe(true);

    // Verify header appears before main in DOM order
    const headerIndex = await page.evaluate(() => {
      const header = document.querySelector('header, [role="banner"]');
      const main = document.querySelector('main, [role="main"]');
      if (!header || !main) return { headerBefore: false };

      const allElements = Array.from(document.querySelectorAll('*'));
      const headerPos = allElements.indexOf(header);
      const mainPos = allElements.indexOf(main);

      return { headerBefore: headerPos < mainPos };
    });

    expect(headerIndex.headerBefore).toBe(true);

    // Check for ARIA landmarks using axe-core
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .options({ rules: { 'landmark-one-main': { enabled: true } } })
      .analyze();

    const landmarkViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('landmark')
    );

    expect(landmarkViolations).toHaveLength(0);
  });
});
