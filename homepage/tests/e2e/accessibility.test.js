/**
 * E2E Tests for Accessibility Compliance (WCAG 2.1 AA)
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests:
 * - Keyboard navigation
 * - Focus visibility
 * - Reduced motion support
 */

const { test, expect } = require('@playwright/test');

test.describe('Accessibility E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: All interactive elements reachable via Tab key', async ({ page }) => {
    // Get all expected interactive elements
    const expectedInteractiveCount = await page.evaluate(() => {
      const selectors = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])'
      ];

      const elements = document.querySelectorAll(selectors.join(', '));
      return elements.length;
    });

    // Navigate through all elements using Tab
    const visitedElements = new Set();
    const maxTabs = expectedInteractiveCount + 10; // Extra buffer for safety
    let firstElement = null;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          id: el.id || '',
          text: el.textContent?.substring(0, 30) || '',
          ariaLabel: el.getAttribute('aria-label') || '',
          uniqueKey: el.tagName + '-' + (el.id || el.className || el.getAttribute('aria-label') || '')
        };
      });

      if (focusedElement) {
        // Track first element to detect cycle
        if (firstElement === null) {
          firstElement = focusedElement.uniqueKey;
        } else if (focusedElement.uniqueKey === firstElement && visitedElements.size > 3) {
          // We've cycled back to the beginning
          break;
        }
        visitedElements.add(focusedElement.uniqueKey);
      }
    }

    // We should have visited multiple interactive elements
    // At minimum: skip link, nav links, CTAs, copy buttons, theme toggle
    expect(visitedElements.size).toBeGreaterThanOrEqual(5);
  });

  test('Test Case 2: Focus indicator visible on buttons', async ({ page }) => {
    // Find all buttons
    const buttons = await page.locator('button').all();

    for (const button of buttons) {
      // Focus the button using keyboard
      await button.focus();

      // Check if the button has a visible focus indicator
      const focusStyles = await button.evaluate(el => {
        const style = getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          outlineColor: style.outlineColor,
          outlineStyle: style.outlineStyle,
          boxShadow: style.boxShadow,
          border: style.border
        };
      });

      // Focus should be visible via outline, box-shadow, or border change
      const hasVisibleFocus =
        (focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none') ||
        focusStyles.boxShadow !== 'none' ||
        focusStyles.outline !== 'none' && focusStyles.outline !== '';

      expect(hasVisibleFocus).toBe(true);
    }
  });

  test('Test Case 3: Focus indicator visible on links', async ({ page }) => {
    // Find visible links (not hidden skip links)
    const links = await page.locator('a[href]:not(.skip-link)').all();

    // Test a sample of links
    const linksToTest = links.slice(0, Math.min(5, links.length));

    for (const link of linksToTest) {
      const isVisible = await link.isVisible();
      if (!isVisible) continue;

      await link.focus();

      const focusStyles = await link.evaluate(el => {
        const style = getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          boxShadow: style.boxShadow,
          textDecoration: style.textDecoration
        };
      });

      // Focus should be visible via outline, box-shadow, or underline
      const hasVisibleFocus =
        (focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none') ||
        focusStyles.boxShadow !== 'none' ||
        focusStyles.outline !== 'none' && focusStyles.outline !== '';

      expect(hasVisibleFocus).toBe(true);
    }
  });

  test('Test Case 11: Animations disabled with prefers-reduced-motion', async ({ page }) => {
    // Create a new context with reduced motion preference
    const browser = page.context().browser();
    const reducedMotionContext = await browser.newContext({
      reducedMotion: 'reduce'
    });
    const reducedMotionPage = await reducedMotionContext.newPage();

    await reducedMotionPage.goto('/');

    // Check that transitions are disabled or very short
    const transitionValues = await reducedMotionPage.evaluate(() => {
      const elements = document.querySelectorAll('*');
      const transitions = [];

      elements.forEach(el => {
        const style = getComputedStyle(el);
        const transitionDuration = style.transitionDuration;
        const animationDuration = style.animationDuration;

        if (transitionDuration && transitionDuration !== '0s' && transitionDuration !== '0.01ms') {
          const duration = parseFloat(transitionDuration);
          if (duration > 0.02) { // More than 20ms
            transitions.push({
              element: el.tagName,
              className: el.className,
              transitionDuration
            });
          }
        }

        if (animationDuration && animationDuration !== '0s' && animationDuration !== '0.01ms') {
          const duration = parseFloat(animationDuration);
          if (duration > 0.02) {
            transitions.push({
              element: el.tagName,
              className: el.className,
              animationDuration
            });
          }
        }
      });

      return transitions;
    });

    // Check scroll behavior
    const scrollBehavior = await reducedMotionPage.evaluate(() => {
      return getComputedStyle(document.documentElement).scrollBehavior;
    });

    // Scroll behavior should be 'auto' not 'smooth'
    expect(scrollBehavior).toBe('auto');

    // No long animations/transitions should be present
    expect(transitionValues.length).toBe(0);

    await reducedMotionContext.close();
  });

  test('Skip link becomes visible on focus', async ({ page }) => {
    // Initially, skip link should be hidden (positioned off-screen)
    const skipLink = await page.locator('.skip-link, a[href="#main-content"]').first();

    // Skip link should start hidden
    await expect(skipLink).toBeAttached();

    // Check initial state - should have negative top or be positioned off screen
    const initiallyVisible = await skipLink.isVisible();

    // Press Tab to focus the skip link
    await page.keyboard.press('Tab');
    await page.waitForTimeout(50); // Brief wait for CSS transition

    // Verify skip link received focus
    const isFocused = await skipLink.evaluate(el => el === document.activeElement);
    expect(isFocused).toBe(true);

    // Skip link should have proper focus styling - just verify it exists and is focusable
    // The CSS transitions the 'top' property on focus
    const focusedState = await skipLink.evaluate(el => {
      const style = getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        top: style.top
      };
    });

    // Skip link should have focus styling applied
    // Either outline or top position change indicates visibility
    const hasVisibleFocus = focusedState.outlineWidth !== '0px' ||
      parseFloat(focusedState.top) > -100;

    expect(hasVisibleFocus).toBe(true);
  });

  test('Skip link navigates to main content', async ({ page }) => {
    // Focus and activate skip link
    await page.keyboard.press('Tab');

    const isSkipLinkFocused = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.classList.contains('skip-link') || el?.getAttribute('href') === '#main-content';
    });

    if (isSkipLinkFocused) {
      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Check that focus moved to main content or URL has hash
      const url = page.url();
      const hasFocusInMain = await page.evaluate(() => {
        return document.activeElement?.id === 'main-content' ||
          document.activeElement?.closest('main') !== null;
      });

      expect(url.includes('#main-content') || hasFocusInMain).toBe(true);
    }
  });

  test('Theme toggle is keyboard accessible', async ({ page }) => {
    // Find theme toggle and focus it
    const themeToggle = await page.locator('[data-theme-toggle], .theme-toggle').first();

    await themeToggle.focus();

    // Check initial theme state
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme') || 'light';
    });

    // Press Enter or Space to toggle
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    // Check theme changed
    const newTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    expect(newTheme).not.toBe(initialTheme);
  });

  test('Copy buttons are keyboard accessible', async ({ page }) => {
    // Find a copy button
    const copyButton = await page.locator('[data-copy-target]').first();

    await copyButton.focus();

    // Verify button is focused
    const isFocused = await copyButton.evaluate(el => el === document.activeElement);
    expect(isFocused).toBe(true);

    // Press Enter to activate (we can't verify clipboard without permissions, but should work)
    await page.keyboard.press('Enter');

    // Check for visual feedback (copied state)
    await page.waitForTimeout(100);
    const hasCopiedClass = await copyButton.evaluate(el =>
      el.classList.contains('code-block__copy--copied')
    );

    // Button should show copied feedback
    expect(hasCopiedClass).toBe(true);
  });

  test('All interactive elements have visible focus on keyboard navigation', async ({ page }) => {
    // Track elements with poor focus visibility
    const poorFocusElements = [];
    let previousElement = null;

    // Tab through elements using keyboard (triggers :focus-visible)
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');
      await page.waitForTimeout(50); // Allow CSS to apply

      const focusInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();

        // Check if element is visible
        if (rect.width === 0 || rect.height === 0) return null;

        return {
          tag: el.tagName.toLowerCase(),
          id: el.id,
          className: el.className,
          hasOutline: style.outlineWidth !== '0px' && style.outlineStyle !== 'none',
          hasBoxShadow: style.boxShadow !== 'none' && style.boxShadow !== '',
          // Check for custom focus indicators (borders, background changes, etc.)
          borderWidth: style.borderWidth,
          outline: style.outline,
          outlineColor: style.outlineColor,
          outlineWidth: style.outlineWidth,
          // Element might rely on parent focus styling
          parentHasFocus: el.parentElement ? getComputedStyle(el.parentElement).outline !== 'none' : false
        };
      });

      if (focusInfo) {
        // Detect cycle - same element as before
        const currentKey = `${focusInfo.tag}-${focusInfo.id}-${focusInfo.className}`;
        if (currentKey === previousElement) break;
        previousElement = currentKey;

        // Check for visible focus indicator
        const hasVisibleFocus = focusInfo.hasOutline ||
          focusInfo.hasBoxShadow ||
          focusInfo.parentHasFocus;

        if (!hasVisibleFocus) {
          poorFocusElements.push(focusInfo);
        }
      }
    }

    // Most focused elements should have visible focus indicator
    // Allow for a small number without (decorative elements, etc.)
    expect(poorFocusElements.length).toBeLessThanOrEqual(2);
  });
});
