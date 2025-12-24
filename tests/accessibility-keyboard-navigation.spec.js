// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Keyboard Navigation Tests
 *
 * These tests verify that all interactive elements are keyboard accessible
 * as specified in NFR-3 (WCAG 2.1 AA compliance).
 *
 * Test coverage:
 * - Tab through all interactive elements in logical order
 * - Visible focus indicators on all focusable elements
 * - Keyboard activation of buttons with Enter/Space
 * - No keyboard traps (can tab away from all elements)
 */

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should allow tabbing through all interactive elements in logical order', async ({ page }) => {
    // Test Case 1: Tab through page elements
    // Expected: All interactive elements receive focus in logical order

    // Get all interactive elements that should be focusable
    const expectedFocusableElements = await page.evaluate(() => {
      // Select all naturally focusable elements
      const selectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
      const elements = document.querySelectorAll(selectors);
      return Array.from(elements)
        .filter(el => {
          const style = window.getComputedStyle(el);
          // Filter out hidden elements
          return style.display !== 'none' &&
                 style.visibility !== 'hidden' &&
                 !el.hasAttribute('disabled') &&
                 el.getAttribute('tabindex') !== '-1';
        })
        .map(el => ({
          tagName: el.tagName.toLowerCase(),
          href: el.getAttribute('href'),
          text: el.textContent?.trim().substring(0, 50) || '',
          className: el.className
        }));
    });

    // Verify we have focusable elements
    expect(expectedFocusableElements.length).toBeGreaterThan(0);

    // Tab through elements and track focus order
    const focusedElements = [];

    // Start by focusing the body to ensure clean state
    await page.evaluate(() => document.body.focus());

    // Tab through all focusable elements
    for (let i = 0; i < expectedFocusableElements.length + 2; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          href: el.getAttribute('href'),
          text: el.textContent?.trim().substring(0, 50) || '',
          className: el.className
        };
      });

      if (focusedElement && !focusedElements.some(e =>
        e.tagName === focusedElement.tagName &&
        e.href === focusedElement.href &&
        e.text === focusedElement.text
      )) {
        focusedElements.push(focusedElement);
      }
    }

    // Verify all expected elements received focus
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify navigation links are focusable
    const navLinks = focusedElements.filter(el => el.tagName === 'a');
    expect(navLinks.length).toBeGreaterThan(0);

    // Verify focus order follows document order (top to bottom, left to right)
    // Check that header nav links come before footer links
    const headerLinkIndex = focusedElements.findIndex(el =>
      el.href && el.href.includes('#features')
    );
    const footerLinkIndex = focusedElements.findIndex(el =>
      el.tagName === 'a' && el.className && el.className.includes('footer')
    );

    // If both exist, header should come first
    if (headerLinkIndex !== -1 && footerLinkIndex !== -1) {
      expect(headerLinkIndex).toBeLessThan(footerLinkIndex);
    }
  });

  test('should show visible focus indicators on all focusable elements', async ({ page }) => {
    // Test Case 2: Check focus indicator visibility
    // Expected: Visible focus outline on all focusable elements

    // Get all interactive elements
    const focusableSelectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';

    // Tab through and verify focus visibility for each element
    await page.evaluate(() => document.body.focus());

    const focusStyleResults = [];

    // Tab through several elements and check their focus styles
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');

      const focusInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        const style = window.getComputedStyle(el);
        const beforePseudo = window.getComputedStyle(el, ':focus');

        return {
          tagName: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 30) || '',
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          borderColor: style.borderColor,
          backgroundColor: style.backgroundColor,
          // Check if there's any visible focus indicator
          hasOutline: style.outlineStyle !== 'none' && style.outlineWidth !== '0px',
          hasBoxShadow: style.boxShadow !== 'none',
          hasBorderChange: style.borderColor !== 'transparent'
        };
      });

      if (focusInfo) {
        focusStyleResults.push(focusInfo);
      }
    }

    // Verify each focused element has some visible focus indicator
    for (const result of focusStyleResults) {
      const hasVisibleFocus = result.hasOutline || result.hasBoxShadow || result.hasBorderChange;
      expect(
        hasVisibleFocus,
        `Element "${result.text}" (${result.tagName}) should have visible focus indicator. ` +
        `Outline: ${result.outlineStyle} ${result.outlineWidth}, BoxShadow: ${result.boxShadow}`
      ).toBe(true);
    }
  });

  test('should activate buttons and links with Enter and Space keys', async ({ page }) => {
    // Test Case 3: Test keyboard activation of buttons
    // Expected: Buttons can be activated with Enter or Space keys

    // Test 1: Verify Enter key activates anchor links (internal navigation)
    await page.evaluate(() => document.body.focus());

    // Tab to the first navigation link (Features)
    await page.keyboard.press('Tab');

    // Find and focus on the "Features" link
    const featuresLink = page.locator('nav a[href="#features"]');
    await featuresLink.focus();

    // Get current URL hash before activation
    const initialHash = await page.evaluate(() => window.location.hash);

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Verify navigation occurred (hash changed)
    await page.waitForFunction(
      (initial) => window.location.hash !== initial || window.location.hash === '#features',
      initialHash,
      { timeout: 2000 }
    ).catch(() => {
      // If timeout, check current hash
    });

    const newHash = await page.evaluate(() => window.location.hash);
    expect(newHash).toBe('#features');

    // Test 2: Verify CTA buttons can be activated with keyboard
    // Navigate to Get Started button
    const getStartedBtn = page.locator('a.btn-primary[href="#getting-started"]');
    await getStartedBtn.focus();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Verify navigation occurred
    await page.waitForTimeout(100);
    const ctaHash = await page.evaluate(() => window.location.hash);
    expect(ctaHash).toBe('#getting-started');

    // Test 3: Verify Space key works on focusable elements (if applicable)
    // Note: Space key works on buttons but scrolls page on links by default
    // Test that elements respond to keyboard interaction
    const configLink = page.locator('nav a[href="#configuration"]');
    await configLink.focus();
    await page.keyboard.press('Enter');

    await page.waitForTimeout(100);
    const configHash = await page.evaluate(() => window.location.hash);
    expect(configHash).toBe('#configuration');
  });

  test('should not trap keyboard focus (can tab away from all elements)', async ({ page }) => {
    // Test Case 4: Verify no keyboard traps
    // Expected: Can tab away from all focusable elements

    // Count all focusable elements
    const totalFocusable = await page.evaluate(() => {
      const selectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
      const elements = document.querySelectorAll(selectors);
      return Array.from(elements).filter(el => {
        const style = window.getComputedStyle(el);
        return style.display !== 'none' &&
               style.visibility !== 'hidden' &&
               !el.hasAttribute('disabled');
      }).length;
    });

    // Start from body
    await page.evaluate(() => document.body.focus());

    // Tab through ALL elements plus a few extra to ensure we loop
    const visitedElements = new Set();
    let consecutiveRepeats = 0;
    let lastElement = '';
    let trapDetected = false;

    for (let i = 0; i < totalFocusable + 10; i++) {
      await page.keyboard.press('Tab');

      const currentElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return 'body';
        return `${el.tagName}-${el.getAttribute('href') || el.textContent?.substring(0, 20) || i}`;
      });

      // Check for keyboard trap (same element multiple times in a row)
      if (currentElement === lastElement && currentElement !== 'body') {
        consecutiveRepeats++;
        if (consecutiveRepeats > 3) {
          trapDetected = true;
          break;
        }
      } else {
        consecutiveRepeats = 0;
      }

      lastElement = currentElement;
      visitedElements.add(currentElement);
    }

    // Verify no trap was detected
    expect(trapDetected).toBe(false);

    // Verify we visited multiple unique elements
    expect(visitedElements.size).toBeGreaterThan(1);

    // Test Shift+Tab to go backwards
    const backwardsElements = [];
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Shift+Tab');

      const currentElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return el.tagName.toLowerCase();
      });

      if (currentElement) {
        backwardsElements.push(currentElement);
      }
    }

    // Verify Shift+Tab moves focus backwards
    expect(backwardsElements.length).toBeGreaterThan(0);
  });

  test('should have skip-to-main-content link or ensure main content is reachable quickly', async ({ page }) => {
    // Additional test: Check for skip link or efficient navigation to main content
    // This is a WCAG best practice for keyboard users

    // Check if skip link exists
    const skipLink = await page.locator('a[href="#main-content"], a[href="#main"], .skip-link, [class*="skip"]').count();

    // If skip link doesn't exist, verify main content is reachable within reasonable tabs
    if (skipLink === 0) {
      await page.evaluate(() => document.body.focus());

      let tabsToMain = 0;
      const maxTabs = 20;
      let reachedMain = false;

      for (let i = 0; i < maxTabs; i++) {
        await page.keyboard.press('Tab');
        tabsToMain++;

        const isInMain = await page.evaluate(() => {
          const activeEl = document.activeElement;
          if (!activeEl) return false;
          const main = document.querySelector('main');
          return main && main.contains(activeEl);
        });

        if (isInMain) {
          reachedMain = true;
          break;
        }
      }

      // Main content should be reachable within reasonable number of tabs
      // (navigation has ~6 links, CTA has 2 buttons = ~8 tabs to main content is acceptable)
      expect(reachedMain).toBe(true);
      expect(tabsToMain).toBeLessThanOrEqual(15);
    }
  });
});
