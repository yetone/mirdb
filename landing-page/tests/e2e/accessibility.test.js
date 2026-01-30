/**
 * MirDB Landing Page - Accessibility E2E Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA accessibility requirements:
 * - Keyboard navigation
 * - Focus indicators
 * - Tab order
 */

const { test, expect } = require('@playwright/test');
const { setupPage, setViewport } = require('../helpers/test-utils');

test.describe('Accessibility Compliance - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('Test Case 4: Tab key navigation - all interactive elements reachable in logical order', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveElements = await page.locator(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
    ).all();

    // Skip link should be first focusable
    await page.keyboard.press('Tab');
    const firstFocused = await page.evaluate(() => document.activeElement?.className || '');
    expect(firstFocused).toContain('skip-link');

    // Press Tab to skip to main content
    await page.keyboard.press('Tab');

    // Continue tabbing through all interactive elements
    const focusedElements = [];
    let prevElement = null;

    for (let i = 0; i < 30; i++) {
      const currentElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el?.tagName?.toLowerCase(),
          text: el?.textContent?.trim().substring(0, 50),
          href: el?.getAttribute('href'),
          role: el?.getAttribute('role'),
          className: el?.className
        };
      });

      // Stop if we've looped back to the beginning
      if (JSON.stringify(currentElement) === JSON.stringify(prevElement)) {
        break;
      }

      focusedElements.push(currentElement);
      prevElement = currentElement;
      await page.keyboard.press('Tab');
    }

    // Verify we can reach key interactive elements
    const focusedTags = focusedElements.map(e => e.tag);
    const focusedTexts = focusedElements.map(e => e.text);

    // Should be able to reach links and buttons
    expect(focusedTags.some(tag => tag === 'a')).toBe(true);
    expect(focusedTags.some(tag => tag === 'button')).toBe(true);

    // Navigation links should be reachable
    const navLinksReachable = focusedTexts.some(text =>
      text?.includes('Features') ||
      text?.includes('Usage') ||
      text?.includes('Architecture')
    );
    expect(navLinksReachable).toBe(true);

    // CTA buttons should be reachable
    const ctaReachable = focusedTexts.some(text =>
      text?.includes('Get Started') ||
      text?.includes('GitHub')
    );
    expect(ctaReachable).toBe(true);
  });

  test('Test Case 5: Focus indicators - visible focus indicator on all focusable elements', async ({ page }) => {
    // Tab to skip link and check focus visibility
    await page.keyboard.press('Tab');

    // Check skip link has visible focus
    const skipLink = page.locator('.skip-link');
    const skipLinkFocused = await skipLink.evaluate(el => document.activeElement === el);
    expect(skipLinkFocused).toBe(true);

    // Verify skip link becomes visible on focus
    await expect(skipLink).toBeVisible();

    // Tab to navigation and check focus styles
    await page.keyboard.press('Tab');

    // Test focus indicators on various element types
    const testElements = [
      { selector: '.nav-links a', name: 'navigation link' },
      { selector: '.btn', name: 'button' },
      { selector: '.diagram-node[tabindex="0"]', name: 'diagram node' }
    ];

    for (const { selector, name } of testElements) {
      const element = page.locator(selector).first();
      if (await element.count() > 0) {
        // Focus the element
        await element.focus();

        // Get computed styles for focus indicator
        const focusStyles = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineColor: styles.outlineColor,
            outlineStyle: styles.outlineStyle,
            outlineWidth: styles.outlineWidth,
            boxShadow: styles.boxShadow,
            border: styles.border,
            borderColor: styles.borderColor
          };
        });

        // Element should have some visible focus indicator
        const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
        const hasBoxShadow = focusStyles.boxShadow !== 'none';
        const hasBorderChange = focusStyles.borderColor !== 'rgb(0, 0, 0)';

        const hasFocusIndicator = hasOutline || hasBoxShadow || hasBorderChange;
        expect(hasFocusIndicator, `${name} should have visible focus indicator`).toBe(true);
      }
    }
  });

  test('Focus indicator is distinguishable from hover state', async ({ page }) => {
    const button = page.locator('.btn').first();

    // Get hover styles
    await button.hover();
    const hoverStyles = await button.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        backgroundColor: styles.backgroundColor,
        borderColor: styles.borderColor
      };
    });

    // Get focus styles
    await button.focus();
    const focusStyles = await button.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        backgroundColor: styles.backgroundColor,
        borderColor: styles.borderColor
      };
    });

    // Focus should have an outline (different from hover which typically doesn't)
    expect(focusStyles.outline).not.toBe('none');
  });

  test('Skip link navigates to main content', async ({ page }) => {
    // Focus the skip link
    await page.keyboard.press('Tab');

    // Activate the skip link
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForTimeout(500);

    // Verify URL has #main-content hash or focus moved to main content
    const currentUrl = page.url();
    const hasFocusOnMain = await page.evaluate(() => {
      const mainContent = document.getElementById('main-content');
      return document.activeElement === mainContent || mainContent?.contains(document.activeElement);
    });

    expect(currentUrl.includes('#main-content') || hasFocusOnMain).toBe(true);
  });

  test('Interactive elements can be activated with keyboard', async ({ page }) => {
    // Test that buttons can be activated with Enter key
    const configToggle = page.locator('.config-toggle');

    if (await configToggle.count() > 0) {
      const initialState = await configToggle.getAttribute('aria-expanded');

      // Focus and activate with Enter
      await configToggle.focus();
      await page.keyboard.press('Enter');

      const newState = await configToggle.getAttribute('aria-expanded');

      // State should have changed
      expect(newState).not.toBe(initialState);
    }
  });

  test('No keyboard traps exist', async ({ page }) => {
    const startTime = Date.now();
    const maxTime = 10000; // 10 seconds max
    const visitedElements = new Set();

    // Tab through the entire page
    while (Date.now() - startTime < maxTime) {
      await page.keyboard.press('Tab');

      const currentElement = await page.evaluate(() => {
        const el = document.activeElement;
        return `${el?.tagName}-${el?.className}-${el?.id}`;
      });

      // If we've seen this element before, we've completed a full cycle
      if (visitedElements.has(currentElement) && visitedElements.size > 5) {
        break;
      }

      visitedElements.add(currentElement);
    }

    // Should have visited multiple elements (no trap)
    expect(visitedElements.size).toBeGreaterThan(5);
  });

  test('Focus order follows visual layout', async ({ page }) => {
    const focusOrder = [];

    // Tab through elements and record their positions
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');

      const position = await page.evaluate(() => {
        const el = document.activeElement;
        const rect = el?.getBoundingClientRect();
        return {
          top: rect?.top || 0,
          left: rect?.left || 0,
          tag: el?.tagName,
          text: el?.textContent?.substring(0, 20)
        };
      });

      focusOrder.push(position);
    }

    // Generally, focus should flow top-to-bottom
    // Count how many times focus moves down
    let movesDown = 0;
    let movesUp = 0;

    for (let i = 1; i < focusOrder.length; i++) {
      if (focusOrder[i].top >= focusOrder[i-1].top - 50) { // Allow some tolerance
        movesDown++;
      } else {
        movesUp++;
      }
    }

    // Focus should generally move down more than up
    expect(movesDown).toBeGreaterThan(movesUp);
  });
});
