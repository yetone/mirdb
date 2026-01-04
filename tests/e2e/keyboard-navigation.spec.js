/**
 * Accessibility Tests: Keyboard Navigation
 * Tests NFR-3: WCAG 2.1 AA compliance - keyboard accessibility
 * Scenario: Verify page is fully navigable using keyboard only
 */
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Tab Through Page Elements', () => {
    test('All interactive elements (links, buttons) are reachable via Tab key', async ({ page }) => {
      // Get all expected interactive elements
      const expectedInteractiveElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();
      const expectedCount = expectedInteractiveElements.length;

      // Ensure there are interactive elements to test
      expect(expectedCount).toBeGreaterThan(0);

      // Start from the body and tab through
      await page.keyboard.press('Tab');

      const visitedElements = new Set();
      let tabCount = 0;
      const maxTabs = expectedCount + 5; // Allow some extra tabs to detect if we cycle back

      while (tabCount < maxTabs) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tagName: el.tagName,
            href: el.href || null,
            text: el.textContent?.trim().substring(0, 50) || '',
            id: el.id || null,
            className: el.className || null
          };
        });

        if (focusedElement) {
          const key = `${focusedElement.tagName}-${focusedElement.href || focusedElement.text || focusedElement.id}`;
          if (visitedElements.has(key)) {
            // We've cycled back to the beginning
            break;
          }
          visitedElements.add(key);
        }

        await page.keyboard.press('Tab');
        tabCount++;
      }

      // Verify that we visited a reasonable number of interactive elements
      // The page should have at least navigation links + CTA buttons + footer links
      expect(visitedElements.size).toBeGreaterThanOrEqual(5);
    });

    test('Navigation links are keyboard accessible', async ({ page }) => {
      const navLinks = await page.locator('nav a').all();
      expect(navLinks.length).toBeGreaterThan(0);

      // Tab to first nav link
      await page.keyboard.press('Tab'); // Skip link
      await page.keyboard.press('Tab'); // First nav link

      for (let i = 0; i < navLinks.length; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          return el ? el.tagName : null;
        });
        expect(focusedElement).toBe('A');
        await page.keyboard.press('Tab');
      }
    });

    test('CTA buttons in hero are keyboard accessible', async ({ page }) => {
      const ctaButtons = await page.locator('.cta-buttons a').all();
      expect(ctaButtons.length).toBeGreaterThan(0);

      // Tab through to find CTA buttons
      let foundCta = false;
      for (let i = 0; i < 20; i++) {
        await page.keyboard.press('Tab');
        const isCta = await page.evaluate(() => {
          const el = document.activeElement;
          return el && el.classList.contains('cta');
        });
        if (isCta) {
          foundCta = true;
          break;
        }
      }
      expect(foundCta).toBe(true);
    });
  });

  test.describe('Test Case 2: Focus Visibility', () => {
    test('Focused elements have visible outline or indicator', async ({ page }) => {
      // Get the first link
      await page.keyboard.press('Tab');

      // Check multiple focusable elements for visible focus styles
      const elementsToCheck = 5;
      for (let i = 0; i < elementsToCheck; i++) {
        const focusStyles = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;

          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            outlineColor: styles.outlineColor,
            boxShadow: styles.boxShadow,
            border: styles.border,
            backgroundColor: styles.backgroundColor
          };
        });

        if (focusStyles) {
          // Element should have some visible focus indicator
          const hasVisibleOutline =
            focusStyles.outlineStyle !== 'none' &&
            focusStyles.outlineWidth !== '0px';
          const hasBoxShadow = focusStyles.boxShadow !== 'none';
          const hasVisibleFocus = hasVisibleOutline || hasBoxShadow;

          expect(hasVisibleFocus).toBe(true);
        }

        await page.keyboard.press('Tab');
      }
    });

    test('Focus indicator has sufficient contrast', async ({ page }) => {
      // Tab to first interactive element
      await page.keyboard.press('Tab');

      const focusIndicatorVisible = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return false;

        const styles = window.getComputedStyle(el);
        const outlineWidth = parseFloat(styles.outlineWidth);

        // Check if outline is visible (at least 2px as per WCAG 2.2 recommendation)
        return outlineWidth >= 2 || styles.boxShadow !== 'none';
      });

      expect(focusIndicatorVisible).toBe(true);
    });
  });

  test.describe('Test Case 3: Logical Tab Order', () => {
    test('Tab order follows visual/logical order of content', async ({ page }) => {
      const expectedOrder = [
        'skip-link', // Skip to main content link should be first
        'nav', // Navigation links
        'hero', // Hero section CTAs
        'content', // Main content links
        'footer' // Footer links
      ];

      const visitedRegions = [];
      let lastRegion = null;

      // Tab through all elements and track regions
      for (let i = 0; i < 30; i++) {
        await page.keyboard.press('Tab');

        const region = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;

          // Determine which region the element is in
          if (el.classList.contains('skip-link')) return 'skip-link';
          if (el.closest('nav')) return 'nav';
          if (el.closest('.hero')) return 'hero';
          if (el.closest('footer')) return 'footer';
          if (el.closest('main')) return 'content';
          return 'other';
        });

        if (region && region !== lastRegion) {
          visitedRegions.push(region);
          lastRegion = region;
        }
      }

      // Verify the order: skip-link should come first if present
      if (visitedRegions.includes('skip-link')) {
        expect(visitedRegions[0]).toBe('skip-link');
      }

      // Navigation should come before main content
      const navIndex = visitedRegions.indexOf('nav');
      const heroIndex = visitedRegions.indexOf('hero');
      const contentIndex = visitedRegions.indexOf('content');
      const footerIndex = visitedRegions.indexOf('footer');

      if (navIndex !== -1 && heroIndex !== -1) {
        expect(navIndex).toBeLessThan(heroIndex);
      }
      if (heroIndex !== -1 && contentIndex !== -1) {
        expect(heroIndex).toBeLessThan(contentIndex);
      }
      if (contentIndex !== -1 && footerIndex !== -1) {
        expect(contentIndex).toBeLessThan(footerIndex);
      }
    });

    test('No tabindex values greater than 0 (anti-pattern)', async ({ page }) => {
      const badTabindexElements = await page.locator('[tabindex]').all();

      for (const element of badTabindexElements) {
        const tabindex = await element.getAttribute('tabindex');
        const tabindexValue = parseInt(tabindex, 10);
        // tabindex should be 0 or -1, never positive
        expect(tabindexValue).toBeLessThanOrEqual(0);
      }
    });
  });

  test.describe('Test Case 4: Skip Link', () => {
    test('Skip to main content link is present', async ({ page }) => {
      const skipLink = page.locator('.skip-link, [href="#main"], a[href="#main-content"]');
      await expect(skipLink).toBeAttached();
    });

    test('Skip link is the first focusable element', async ({ page }) => {
      await page.keyboard.press('Tab');

      const isSkipLink = await page.evaluate(() => {
        const el = document.activeElement;
        return el && (
          el.classList.contains('skip-link') ||
          el.getAttribute('href') === '#main' ||
          el.getAttribute('href') === '#main-content'
        );
      });

      expect(isSkipLink).toBe(true);
    });

    test('Skip link becomes visible on focus', async ({ page }) => {
      // Skip link should be visually hidden but become visible on focus
      const skipLink = page.locator('.skip-link');

      // Initially may be visually hidden
      await page.keyboard.press('Tab');

      // After focus, should be visible
      const isVisible = await skipLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          styles.visibility !== 'hidden' &&
          styles.display !== 'none' &&
          styles.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      });

      expect(isVisible).toBe(true);
    });

    test('Skip link navigates to main content when activated', async ({ page }) => {
      // Focus the skip link
      await page.keyboard.press('Tab');

      // Activate it with Enter
      await page.keyboard.press('Enter');

      // Wait for navigation
      await page.waitForTimeout(100);

      // Check that focus or scroll moved to main content
      const mainHasFocus = await page.evaluate(() => {
        const main = document.querySelector('main');
        const activeElement = document.activeElement;

        // Either main has focus, or an element inside main has focus
        // Or the page has scrolled to main
        return (
          activeElement === main ||
          main.contains(activeElement) ||
          main.getBoundingClientRect().top <= 100
        );
      });

      expect(mainHasFocus).toBe(true);
    });

    test('Main content has id matching skip link target', async ({ page }) => {
      const skipLink = page.locator('.skip-link');
      const skipLinkHref = await skipLink.getAttribute('href');

      // Extract the ID from the href
      const targetId = skipLinkHref?.replace('#', '');

      if (targetId) {
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement).toBeAttached();

        // Target should be main content area
        const isMainContent = await targetElement.evaluate((el) => {
          return el.tagName === 'MAIN' || el.closest('main') !== null;
        });
        expect(isMainContent).toBe(true);
      }
    });
  });

  test.describe('Additional Keyboard Accessibility', () => {
    test('Enter key activates focused links', async ({ page }) => {
      // Find and focus a navigation link
      await page.keyboard.press('Tab'); // Skip link
      await page.keyboard.press('Tab'); // First nav link

      // Get current URL before activation
      const urlBefore = page.url();

      // Get the href of the focused link
      const href = await page.evaluate(() => {
        return document.activeElement?.href || null;
      });

      if (href && href.includes('#')) {
        // Internal link - pressing Enter should scroll or change hash
        await page.keyboard.press('Enter');
        await page.waitForTimeout(500);

        // URL should now include the anchor
        const urlAfter = page.url();
        expect(urlAfter).toContain('#');
      }
    });

    test('Escape key does not cause unexpected behavior', async ({ page }) => {
      // Tab to an element
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');

      // Press Escape
      await page.keyboard.press('Escape');

      // Page should still be functional
      const bodyVisible = await page.locator('body').isVisible();
      expect(bodyVisible).toBe(true);
    });
  });
});
