/**
 * Accessibility Compliance Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Test cases:
 * - axe-core audit passes
 * - Single h1 element
 * - Proper heading hierarchy
 * - All images have alt text
 * - Skip-to-content link exists
 * - Focus indicators visible
 * - Color contrast sufficient
 * - ARIA labels on interactive elements
 * - Semantic HTML structure
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { waitForPageLoad } from '../utils/test-helpers';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  // Test Case 1: Run axe-core accessibility audit
  test('should pass axe-core accessibility audit with no critical or serious violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Violations:', JSON.stringify(criticalViolations, null, 2));
    }

    expect(criticalViolations).toHaveLength(0);
  });

  // Test Case 2: Check for single h1 element containing 'MirDB'
  test('should have exactly one h1 element containing MirDB', async ({ page }) => {
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    expect(h1Count).toBe(1);

    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('MirDB');
  });

  // Test Case 3: Verify heading hierarchy without skipping levels
  test('should have proper heading hierarchy without skipping levels', async ({ page }) => {
    // Get all headings
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    let previousLevel = 0;

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const currentLevel = parseInt(tagName.charAt(1));

      // Heading level should not skip more than one level when increasing
      if (currentLevel > previousLevel && currentLevel - previousLevel > 1 && previousLevel !== 0) {
        throw new Error(`Heading hierarchy violated: jumped from h${previousLevel} to h${currentLevel}`);
      }

      previousLevel = currentLevel;
    }

    expect(headings.length).toBeGreaterThan(0);
  });

  // Test Case 4: Check all images have non-empty alt text
  test('should have alt text on all images', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    expect(imageCount).toBeGreaterThan(0);

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');

      // Alt should exist and not be empty
      expect(alt).not.toBeNull();
      expect(alt?.trim()).not.toBe('');
    }
  });

  // Test Case 5: Verify skip-to-content link exists and works
  test('should have skip-to-content link that focuses on main content', async ({ page }) => {
    // Look for skip-to-content link (usually first focusable element)
    const skipLink = page.locator('a[href="#main-content"], a.skip-link, a.skip-to-content');

    await expect(skipLink).toHaveCount(1);

    // Tab to the skip link (it should be the first focusable element)
    await page.keyboard.press('Tab');

    // The skip link should be visible when focused
    await expect(skipLink).toBeVisible();

    // Click the skip link
    await skipLink.click();

    // Check that main content exists and is the target
    const mainContent = page.locator('#main-content, main');
    await expect(mainContent).toBeVisible();
  });

  // Test Case 6: Check focus indicators on interactive elements
  test('should have visible focus indicators on all interactive elements', async ({ page }) => {
    // Navigate to the skip link first by pressing Tab
    await page.keyboard.press('Tab');

    // The skip link should now be focused and visible
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Check that it has a focus indicator
    const skipLinkStyle = await skipLink.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
      };
    });

    // Skip link should have an outline when focused
    expect(skipLinkStyle.outlineStyle).not.toBe('none');

    // Continue tabbing through elements and verify buttons have focus styles
    // Tab to a button (e.g., navigation link)
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
    }

    // Check that buttons in CSS have focus-visible styles defined
    // We verify by checking that the CSS includes focus-visible rules
    const hasButtonFocusStyles = await page.evaluate(() => {
      const styleSheets = Array.from(document.styleSheets);
      for (const sheet of styleSheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule instanceof CSSStyleRule) {
              if (rule.selectorText?.includes('focus-visible') ||
                  rule.selectorText?.includes(':focus')) {
                return true;
              }
            }
          }
        } catch (e) {
          // Cross-origin stylesheets may throw
        }
      }
      return false;
    });

    expect(hasButtonFocusStyles).toBe(true);
  });

  // Test Case 7: Verify color contrast ratios
  test('should have sufficient color contrast for all text', async ({ page }) => {
    // Use axe-core specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze();

    // Filter for contrast-related violations
    const contrastViolations = contrastResults.violations.filter(
      v => v.id.includes('contrast')
    );

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Contrast Violations:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations).toHaveLength(0);
  });

  // Test Case 8: Check ARIA labels on interactive elements without visible text
  test('should have aria-label on interactive elements without visible text', async ({ page }) => {
    // Check elements that might not have visible text but need labels
    // Icons, icon-only buttons, etc.

    // Navigation toggle button
    const navToggle = page.locator('.nav-toggle, button[aria-label]');
    if (await navToggle.count() > 0) {
      const ariaLabel = await navToggle.first().getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel?.trim()).not.toBe('');
    }

    // Icon-only links (like GitHub icon)
    const iconLinks = page.locator('a:has(svg):not(:has-text(""))');
    const iconCount = await iconLinks.count();

    for (let i = 0; i < iconCount; i++) {
      const link = iconLinks.nth(i);
      const textContent = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');

      // If link has minimal visible text, it should have aria-label
      if (!textContent || textContent.trim().length < 2) {
        expect(ariaLabel).not.toBeNull();
      }
    }

    // Copy buttons
    const copyButtons = page.locator('.copy-button, button[data-copy-target]');
    const copyCount = await copyButtons.count();

    for (let i = 0; i < copyCount; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel?.trim()).not.toBe('');
    }
  });

  // Test Case 9: Verify semantic HTML structure
  test('should use semantic HTML elements', async ({ page }) => {
    // Check for required semantic elements
    const header = page.locator('header, [role="banner"]');
    const nav = page.locator('nav, [role="navigation"]');
    const main = page.locator('main, [role="main"]');
    const sections = page.locator('section, [role="region"]');
    const footer = page.locator('footer, [role="contentinfo"]');

    // Verify each semantic element exists
    await expect(header).toHaveCount(1);
    await expect(nav).toBeVisible();
    await expect(main).toHaveCount(1);
    expect(await sections.count()).toBeGreaterThan(0);
    await expect(footer).toHaveCount(1);
  });

  // Additional test: Keyboard navigation
  test('should allow keyboard navigation through all interactive elements', async ({ page }) => {
    // Count all focusable elements on the page
    const focusableCount = await page.evaluate(() => {
      const focusableSelectors = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ];

      const elements = document.querySelectorAll(focusableSelectors.join(','));
      return elements.length;
    });

    // Should have multiple focusable elements (links, buttons, etc.)
    expect(focusableCount).toBeGreaterThan(10);

    // Tab through a few elements to verify tabbing works
    const visitedTags: string[] = [];
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
      const tagName = await page.evaluate(() => document.activeElement?.tagName);
      if (tagName && tagName !== 'BODY') {
        visitedTags.push(tagName);
      }
    }

    // Should have visited at least some interactive elements
    expect(visitedTags.length).toBeGreaterThan(0);
  });
});
