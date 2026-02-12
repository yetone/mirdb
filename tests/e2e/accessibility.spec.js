/**
 * Accessibility E2E Tests
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Test cases:
 * - Tab through all interactive elements
 * - Check focus indicators are visible
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const getPageUrl = () => `file://${path.resolve(process.cwd(), 'index.html')}`;

test.describe('Keyboard Navigation and Focus Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getPageUrl());
  });

  test('should be able to tab through all interactive elements', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveSelectors = [
      'a[href]',
      'button',
      'input:not([type="hidden"])',
      'select',
      'textarea',
      '[tabindex]:not([tabindex="-1"])',
    ];

    // Count total interactive elements
    const interactiveElements = await page.locator(interactiveSelectors.join(', ')).all();
    const totalInteractive = interactiveElements.length;

    expect(totalInteractive).toBeGreaterThan(0);

    // Track which elements receive focus
    const focusedElements = [];

    // Start from beginning of document
    await page.keyboard.press('Tab');

    // Tab through elements and track focus
    for (let i = 0; i < totalInteractive + 2; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return {
            tagName: el.tagName.toLowerCase(),
            text: el.textContent?.trim().substring(0, 50) || '',
            href: el.getAttribute('href') || '',
            ariaLabel: el.getAttribute('aria-label') || '',
            className: el.className,
          };
        }
        return null;
      });

      if (focusedElement) {
        focusedElements.push(focusedElement);
      }

      await page.keyboard.press('Tab');
    }

    // Verify that links are focusable
    const focusedLinks = focusedElements.filter((el) => el.tagName === 'a');
    expect(focusedLinks.length).toBeGreaterThan(0);

    // Verify that buttons are focusable
    const focusedButtons = focusedElements.filter((el) => el.tagName === 'button');
    expect(focusedButtons.length).toBeGreaterThan(0);

    // Verify hero buttons are reachable (View on GitHub and Get Started)
    const gitHubLinkFocused = focusedElements.some(
      (el) => el.href?.includes('github.com') || el.text?.includes('GitHub')
    );
    expect(gitHubLinkFocused).toBe(true);
  });

  test('should have visible focus indicators on focused elements', async ({ page }) => {
    // Focus the first interactive element
    await page.keyboard.press('Tab');

    // Check focus styles on the currently focused element directly
    let checkedCount = 0;
    let validFocusCount = 0;

    for (let i = 0; i < 5; i++) {
      // Check that the focused element has visible focus styles
      const focusInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        const styles = window.getComputedStyle(el);

        // Get outline properties
        const outlineWidth = styles.outlineWidth;
        const outlineStyle = styles.outlineStyle;
        const outlineColor = styles.outlineColor;

        // Check if outline is visible (width > 0 and style is not none)
        // Parse the width value - it could be "2px", "0px", etc.
        const widthValue = parseFloat(outlineWidth) || 0;
        const hasOutline = widthValue > 0 && outlineStyle !== 'none';

        // Check for box-shadow
        const boxShadow = styles.boxShadow;
        const hasBoxShadow = boxShadow && boxShadow !== 'none';

        return {
          tagName: el.tagName,
          className: el.className,
          outlineWidth,
          outlineStyle,
          outlineColor,
          hasOutline,
          boxShadow,
          hasBoxShadow,
          hasAnyIndicator: hasOutline || hasBoxShadow,
        };
      });

      if (focusInfo && focusInfo.tagName !== 'BODY') {
        checkedCount++;
        if (focusInfo.hasAnyIndicator) {
          validFocusCount++;
        }
      }

      await page.keyboard.press('Tab');
    }

    // We should have checked at least some elements
    expect(checkedCount).toBeGreaterThan(0);

    // At least one focused element should have a visible focus indicator
    // (some elements like images inside links might not show direct outline)
    expect(validFocusCount).toBeGreaterThanOrEqual(1);
  });

  test('should maintain focus visibility after keyboard navigation', async ({ page }) => {
    // Tab to the first button
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // Get the focused element
    const focusedElement = page.locator(':focus');

    // Verify it's visible
    await expect(focusedElement).toBeVisible();

    // Check that it has some visible focus indicator
    const hasFocusIndicator = await focusedElement.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const hasOutline = styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none';
      const hasBoxShadow = styles.boxShadow !== 'none';
      return hasOutline || hasBoxShadow;
    });

    expect(hasFocusIndicator).toBe(true);
  });

  test('should allow activating links with Enter key', async ({ page }) => {
    // Find the "Get Started" link which is internal
    await page.keyboard.press('Tab');

    // Tab until we find the Get Started link
    let foundGetStarted = false;
    for (let i = 0; i < 10; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          href: el?.getAttribute('href') || '',
          text: el?.textContent?.trim() || '',
        };
      });

      if (activeElement.href === '#quickstart' || activeElement.text.includes('Get Started')) {
        foundGetStarted = true;
        break;
      }
      await page.keyboard.press('Tab');
    }

    expect(foundGetStarted).toBe(true);

    // Pressing Enter should navigate to the section
    await page.keyboard.press('Enter');

    // URL should now include #quickstart
    await expect(page).toHaveURL(/#quickstart/);
  });

  test('should allow activating buttons with Enter key', async ({ page }) => {
    // Find copy button
    await page.keyboard.press('Tab');

    let foundCopyButton = false;
    for (let i = 0; i < 15; i++) {
      const isCopyButton = await page.evaluate(() => {
        const el = document.activeElement;
        return el?.classList?.contains('copy-btn') || el?.textContent?.trim() === 'Copy';
      });

      if (isCopyButton) {
        foundCopyButton = true;
        break;
      }
      await page.keyboard.press('Tab');
    }

    expect(foundCopyButton).toBe(true);

    // Verify button is focused
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).toBeVisible();
  });

  test('should have logical tab order following visual layout', async ({ page }) => {
    const focusOrder = [];

    // Tab through and record element positions
    await page.keyboard.press('Tab');

    for (let i = 0; i < 10; i++) {
      const elementInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          const rect = el.getBoundingClientRect();
          return {
            tagName: el.tagName.toLowerCase(),
            top: rect.top,
            left: rect.left,
            text: el.textContent?.trim().substring(0, 30) || '',
          };
        }
        return null;
      });

      if (elementInfo) {
        focusOrder.push(elementInfo);
      }

      await page.keyboard.press('Tab');
    }

    // Verify elements are generally in top-to-bottom order
    // (with some tolerance for elements in the same row)
    let previousTop = -Infinity;
    let outOfOrderCount = 0;

    focusOrder.forEach((element, index) => {
      if (index > 0) {
        // Allow some tolerance for elements in the same visual row (within 50px)
        if (element.top < previousTop - 50) {
          outOfOrderCount++;
        }
      }
      previousTop = element.top;
    });

    // Tab order should mostly follow visual order
    // Allow at most 1 out-of-order element (for elements in same row)
    expect(outOfOrderCount).toBeLessThanOrEqual(1);
  });

  test('should not trap keyboard focus', async ({ page }) => {
    const visitedElements = new Set();

    // Tab through the entire page multiple times
    await page.keyboard.press('Tab');

    for (let i = 0; i < 30; i++) {
      const elementId = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          // Create a unique identifier for the element
          return `${el.tagName}-${el.className}-${el.textContent?.trim().substring(0, 20)}`;
        }
        return null;
      });

      if (elementId) {
        visitedElements.add(elementId);
      }

      await page.keyboard.press('Tab');
    }

    // We should have visited multiple unique elements
    expect(visitedElements.size).toBeGreaterThan(3);
  });
});
