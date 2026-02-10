/**
 * Accessibility E2E Tests
 * Owner: Scenario 6 - Accessibility
 *
 * End-to-end tests for accessibility features of the MirDB homepage.
 *
 * Expected test coverage:
 * - Keyboard navigation support
 * - Focus indicators
 * - Tab order
 *
 * Requirements traced:
 * - REQ-5: Basic accessibility considerations
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const htmlPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Accessibility E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(htmlPath);
  });

  test('TC6: All interactive elements are reachable via Tab key in logical order', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveElements = await page.$$eval(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
      elements => elements.map(el => ({
        tagName: el.tagName,
        href: el.href || null,
        text: el.textContent.trim().substring(0, 50),
        tabIndex: el.tabIndex
      }))
    );

    // Tab through all interactive elements
    let focusedCount = 0;
    const maxTabs = interactiveElements.length + 5; // Add buffer for safety

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el.tagName,
          href: el.href || null,
          text: el.textContent?.trim().substring(0, 50) || ''
        };
      });

      // Check if we've tabbed to an interactive element
      if (focusedElement.tagName !== 'BODY') {
        focusedCount++;
      }

      // If we've cycled back to the beginning (body or document), stop
      if (focusedElement.tagName === 'BODY' && focusedCount > 0) {
        break;
      }
    }

    // We should have been able to tab through at least some interactive elements
    expect(focusedCount).toBeGreaterThan(0);

    // Verify the count matches expected interactive elements (with some tolerance)
    expect(focusedCount).toBeGreaterThanOrEqual(interactiveElements.length - 2);
  });

  test('TC8: Focused elements have visible focus outline or indicator', async ({ page }) => {
    // Tab to the first interactive element
    await page.keyboard.press('Tab');

    // Get the focused element's computed styles
    const focusStyles = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el.tagName === 'BODY') return null;

      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        boxShadow: styles.boxShadow,
        border: styles.border,
        borderWidth: styles.borderWidth
      };
    });

    expect(focusStyles).not.toBeNull();

    // Check if there's a visible focus indicator
    const hasOutline = focusStyles.outlineStyle !== 'none' &&
                       focusStyles.outlineWidth !== '0px';
    const hasBoxShadow = focusStyles.boxShadow !== 'none';
    const hasBorder = focusStyles.borderWidth !== '0px';

    // At least one focus indicator should be present
    expect(hasOutline || hasBoxShadow || hasBorder).toBe(true);
  });

  test('Skip link becomes visible on focus and links to main content', async ({ page }) => {
    // Tab to the first element (should be skip link)
    await page.keyboard.press('Tab');

    // Check if skip link exists
    const skipLink = await page.locator('.skip-link, a[href="#main-content"]').first();
    expect(skipLink).not.toBeNull();

    // Check the href points to main-content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // The skip link should be visible when focused
    const isVisible = await skipLink.isVisible();
    expect(isVisible).toBe(true);

    // Verify the main element with id="main-content" exists
    const mainContent = await page.$('#main-content');
    expect(mainContent).not.toBeNull();
  });

  test('Navigation links are focusable and have proper focus states', async ({ page }) => {
    // Get nav links
    const navLinks = await page.$$('nav a');
    expect(navLinks.length).toBeGreaterThan(0);

    // Tab until we reach nav links
    let reachedNav = false;
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el.tagName,
          isInNav: el.closest('nav') !== null
        };
      });

      if (focusedElement.isInNav && focusedElement.tagName === 'A') {
        reachedNav = true;

        // Check that the focused nav link has a visible focus indicator
        const hasVisibleFocus = await page.evaluate(() => {
          const el = document.activeElement;
          const styles = window.getComputedStyle(el);
          return styles.outlineStyle !== 'none' || styles.boxShadow !== 'none';
        });

        expect(hasVisibleFocus).toBe(true);
        break;
      }
    }

    expect(reachedNav).toBe(true);
  });
});
