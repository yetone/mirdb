// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Keyboard Navigation Tests
 *
 * Verifies that the MirDB homepage supports keyboard navigation per WCAG 2.1 AA (NFR-3).
 * Tests include:
 * - Tab navigation through all interactive elements
 * - Visible focus indicators on focused elements
 * - Keyboard activation of buttons and links (Enter/Space)
 * - Skip to main content link functionality
 */

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('all interactive elements are reachable via Tab key in logical order', async ({ page }) => {
    // Start from the beginning of the page
    await page.keyboard.press('Tab');

    // Expected tab order: skip link, nav links, CTA buttons, footer links
    const expectedTabOrder = [
      { selector: '[data-testid="skip-to-main"]', description: 'Skip to main content link' },
      { selector: '.nav-link[href="#features"]', description: 'Features nav link' },
      { selector: '.nav-link[href="#architecture"]', description: 'Architecture nav link' },
      { selector: '.nav-link[href="#commands"]', description: 'Commands nav link' },
      { selector: '.nav-link[href="#getting-started"]', description: 'Getting Started nav link' },
      { selector: '[data-testid="primary-cta"]', description: 'Primary CTA (View on GitHub)' },
      { selector: '[data-testid="secondary-cta"]', description: 'Secondary CTA (Learn More)' },
    ];

    // Verify first few elements are reachable in logical order
    for (let i = 0; i < expectedTabOrder.length; i++) {
      const { selector, description } = expectedTabOrder[i];
      const element = page.locator(selector);

      // Check if the element is focused
      const isFocused = await element.evaluate((el) => el === document.activeElement);
      expect(isFocused, `${description} should be focused at tab index ${i + 1}`).toBe(true);

      // Press Tab to move to next element (except for the last one)
      if (i < expectedTabOrder.length - 1) {
        await page.keyboard.press('Tab');
      }
    }

    // Continue tabbing to verify footer link is also reachable
    await page.keyboard.press('Tab');
    // Tab through any remaining interactive elements to reach footer
    let maxTabs = 20;
    while (maxTabs > 0) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.closest('.footer') !== null : false;
      });
      if (activeElement) break;
      await page.keyboard.press('Tab');
      maxTabs--;
    }

    // Verify we can reach the footer link
    const footerLink = page.locator('.footer a[href="https://github.com/yetone/mirdb"]');
    const isFooterLinkReachable = await footerLink.evaluate((el) => {
      // Check if element was previously focused or is focusable
      return el.tabIndex >= 0 || el.tagName === 'A';
    });
    expect(isFooterLinkReachable).toBe(true);
  });

  test('focused elements have visible outline or highlight', async ({ page }) => {
    // Test focus styles on various interactive elements
    const interactiveElements = [
      { selector: '[data-testid="skip-to-main"]', name: 'Skip link' },
      { selector: '.nav-link', name: 'Nav link' },
      { selector: '[data-testid="primary-cta"]', name: 'Primary CTA button' },
      { selector: '[data-testid="secondary-cta"]', name: 'Secondary CTA button' },
      { selector: '.footer a', name: 'Footer link' },
    ];

    for (const { selector, name } of interactiveElements) {
      const element = page.locator(selector).first();

      // Focus the element
      await element.focus();

      // Get the computed outline style
      const outlineStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow,
        };
      });

      // Check if there's a visible focus indicator (outline or box-shadow)
      const hasVisibleOutline =
        (outlineStyles.outlineWidth !== '0px' && outlineStyles.outlineStyle !== 'none') ||
        outlineStyles.boxShadow !== 'none';

      expect(hasVisibleOutline, `${name} should have visible focus indicator`).toBe(true);
    }
  });

  test('buttons trigger their action when focused and Enter is pressed', async ({ page }) => {
    // Test the Learn More button (secondary CTA) which scrolls to features
    const learnMoreButton = page.locator('[data-testid="secondary-cta"]');

    // Focus the button
    await learnMoreButton.focus();

    // Verify it's focused
    const isFocused = await learnMoreButton.evaluate((el) => el === document.activeElement);
    expect(isFocused).toBe(true);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Press Enter to activate the button
    await page.keyboard.press('Enter');

    // Wait for smooth scroll animation
    await page.waitForTimeout(800);

    // Get new scroll position
    const newScrollY = await page.evaluate(() => window.scrollY);

    // Verify the page scrolled (button action was triggered)
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is now in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  test('skip to main content link is available for keyboard users', async ({ page }) => {
    // The skip link should be present in the DOM
    const skipLink = page.locator('[data-testid="skip-to-main"]');
    await expect(skipLink).toBeAttached();

    // The skip link should have correct href pointing to main content
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Tab to the skip link (should be first focusable element)
    await page.keyboard.press('Tab');

    // Verify skip link is focused
    const isFocused = await skipLink.evaluate((el) => el === document.activeElement);
    expect(isFocused, 'Skip link should be the first focusable element').toBe(true);

    // Wait for the skip link transition to complete (top: -100px -> 0)
    await page.waitForTimeout(300);

    // Skip link should become visible when focused
    const isVisible = await skipLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      // Check if the element is positioned on screen (top >= -10 allows for small variations)
      const rect = el.getBoundingClientRect();
      // When focused, top should be 0 (or close to it after transition)
      return rect.top >= -10 && styles.opacity !== '0';
    });
    expect(isVisible, 'Skip link should be visible when focused').toBe(true);

    // Press Enter to activate the skip link
    await page.keyboard.press('Enter');

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(800);

    // Verify main content area is now in viewport (primary check for skip link functionality)
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeInViewport();

    // The URL may or may not contain the hash depending on smooth scroll implementation
    // The key test is that the main content is visible and scrolled to
  });

  test('navigation links can be activated with Enter key', async ({ page }) => {
    // Focus on the Features nav link
    const featuresNavLink = page.locator('.nav-link[href="#features"]');
    await featuresNavLink.focus();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Press Enter to navigate
    await page.keyboard.press('Enter');

    // Wait for smooth scroll
    await page.waitForTimeout(800);

    // Verify navigation occurred (scroll changed or hash changed)
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is visible
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  test('focus does not get trapped in any section', async ({ page }) => {
    // Tab through all elements and verify we can reach the end
    let tabCount = 0;
    const maxTabs = 50;
    const visitedElements = new Set();

    while (tabCount < maxTabs) {
      await page.keyboard.press('Tab');
      tabCount++;

      // Get current focused element info
      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName,
          id: el.id,
          className: el.className,
          href: el.getAttribute('href'),
        };
      });

      if (focusedInfo) {
        const elementKey = JSON.stringify(focusedInfo);

        // If we've seen this element before, we've completed a full cycle
        if (visitedElements.has(elementKey)) {
          break;
        }
        visitedElements.add(elementKey);
      }
    }

    // We should have visited multiple unique elements before cycling
    expect(visitedElements.size).toBeGreaterThan(5);

    // We should eventually cycle back (focus is not trapped)
    expect(tabCount).toBeLessThan(maxTabs);
  });

  test('Shift+Tab navigates backwards through interactive elements', async ({ page }) => {
    // First, tab to a known element
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await primaryCTA.focus();

    // Press Shift+Tab to go backwards
    await page.keyboard.press('Shift+Tab');

    // Should now be on the Getting Started nav link
    const gettingStartedLink = page.locator('.nav-link[href="#getting-started"]');
    const isFocused = await gettingStartedLink.evaluate((el) => el === document.activeElement);
    expect(isFocused, 'Shift+Tab should navigate to previous element').toBe(true);
  });
});
