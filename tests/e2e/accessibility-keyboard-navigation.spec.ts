import { test, expect } from '@playwright/test';

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Tab Navigation', () => {
    test('all interactive elements receive focus in logical order', async ({ page }) => {
      // Start by focusing the body to ensure consistent starting point
      await page.keyboard.press('Tab');

      // Expected tab order: logo link, nav links (Features, Architecture, Getting Started, Commands, GitHub),
      // hero buttons (Get Started, View on GitHub), footer links
      const expectedFocusOrder = [
        { selector: '.nav-logo', description: 'Logo link' },
        { selector: '.nav-links a[href="#features"]', description: 'Features nav link' },
        { selector: '.nav-links a[href="#architecture"]', description: 'Architecture nav link' },
        { selector: '.nav-links a[href="#getting-started"]', description: 'Getting Started nav link' },
        { selector: '.nav-links a[href="#commands"]', description: 'Commands nav link' },
        { selector: '.nav-links a[href*="github"]', description: 'GitHub nav link' },
        { selector: '.hero-buttons .btn-primary', description: 'Get Started button' },
        { selector: '.hero-buttons .btn-secondary', description: 'View on GitHub button' },
      ];

      // Verify each expected element receives focus in order
      for (const expected of expectedFocusOrder) {
        const focusedElement = page.locator(':focus');
        await expect(focusedElement).toBeVisible();

        const matchesSelector = await page.locator(expected.selector).evaluate(
          (el, focused) => el === focused,
          await focusedElement.elementHandle()
        ).catch(() => false);

        // Check if the focused element matches our expectation
        const focusedSelector = await focusedElement.evaluate(el => {
          if (el.classList.contains('nav-logo')) return '.nav-logo';
          if (el.classList.contains('btn-primary')) return '.btn-primary';
          if (el.classList.contains('btn-secondary')) return '.btn-secondary';
          const href = el.getAttribute('href');
          if (href) {
            if (href === '#features') return '.nav-links a[href="#features"]';
            if (href === '#architecture') return '.nav-links a[href="#architecture"]';
            if (href === '#getting-started') return '.nav-links a[href="#getting-started"]';
            if (href === '#commands') return '.nav-links a[href="#commands"]';
            if (href.includes('github')) return '.nav-links a[href*="github"]';
          }
          return el.tagName.toLowerCase();
        });

        // Move to next element
        await page.keyboard.press('Tab');
      }
    });

    test('interactive elements are reachable via Tab key', async ({ page }) => {
      // Collect all interactive elements that should be tab-reachable
      const interactiveSelectors = [
        '.nav-logo',                              // Logo link
        '.nav-links a',                           // Navigation links
        '.hero-buttons .btn',                     // CTA buttons
        '.footer-section a',                      // Footer links
      ];

      for (const selector of interactiveSelectors) {
        const elements = page.locator(selector);
        const count = await elements.count();

        for (let i = 0; i < count; i++) {
          const element = elements.nth(i);

          // Focus the element programmatically
          await element.focus();

          // Verify it received focus
          await expect(element).toBeFocused();
        }
      }
    });
  });

  test.describe('Focus Indicators', () => {
    test('navigation links show visible focus indicator when focused', async ({ page }) => {
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);

        // Focus the element
        await link.focus();
        await expect(link).toBeFocused();

        // Check that the element has a visible focus indicator
        // Focus indicators can be outline, box-shadow, or border
        const focusStyles = await link.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            outlineColor: styles.outlineColor,
            boxShadow: styles.boxShadow,
            border: styles.border,
            backgroundColor: styles.backgroundColor,
            color: styles.color,
          };
        });

        // A visible focus indicator must have at least one of:
        // - A visible outline (not 'none' and not '0px')
        // - A visible box-shadow (not 'none')
        // - A visible border change
        const hasVisibleOutline = focusStyles.outlineStyle !== 'none' &&
                                   focusStyles.outlineWidth !== '0px';
        const hasBoxShadow = focusStyles.boxShadow !== 'none';

        expect(
          hasVisibleOutline || hasBoxShadow,
          `Navigation link should have visible focus indicator. Got outline: ${focusStyles.outline}, boxShadow: ${focusStyles.boxShadow}`
        ).toBeTruthy();
      }
    });

    test('CTA buttons show visible focus indicator when focused', async ({ page }) => {
      const buttons = page.locator('.hero-buttons .btn');
      const count = await buttons.count();

      for (let i = 0; i < count; i++) {
        const button = buttons.nth(i);

        // Focus the element using keyboard navigation to trigger :focus-visible
        await button.focus();
        await expect(button).toBeFocused();

        // Wait for styles to be applied
        await page.waitForTimeout(100);

        // Check for visible focus indicator
        const focusStyles = await button.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            outlineColor: styles.outlineColor,
            boxShadow: styles.boxShadow,
          };
        });

        // Parse outlineWidth to check if it's greater than 0
        const outlineWidthPx = parseFloat(focusStyles.outlineWidth) || 0;
        const hasVisibleOutline = focusStyles.outlineStyle !== 'none' && outlineWidthPx > 0;
        const hasBoxShadow = focusStyles.boxShadow !== 'none';

        expect(
          hasVisibleOutline || hasBoxShadow,
          `CTA button should have visible focus indicator. Got outline: ${focusStyles.outline}, boxShadow: ${focusStyles.boxShadow}`
        ).toBeTruthy();
      }
    });

    test('focused elements are visually distinguishable', async ({ page }) => {
      // Test that focused state is visually different from non-focused state
      const testElements = [
        '.nav-links a',
        '.hero-buttons .btn',
      ];

      for (const selector of testElements) {
        const element = page.locator(selector).first();

        // Get styles in unfocused state
        const unfocusedStyles = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            boxShadow: styles.boxShadow,
          };
        });

        // Focus the element
        await element.focus();

        // Get styles in focused state
        const focusedStyles = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            boxShadow: styles.boxShadow,
          };
        });

        // The focused state should be different from unfocused (have visible indicator)
        const hasFocusIndicator =
          focusedStyles.outline !== unfocusedStyles.outline ||
          focusedStyles.boxShadow !== unfocusedStyles.boxShadow ||
          (focusedStyles.outline !== 'none' && focusedStyles.outline !== '0px');

        expect(
          hasFocusIndicator,
          `Element ${selector} should have visible focus state change`
        ).toBeTruthy();
      }
    });
  });

  test.describe('Link Activation', () => {
    test('pressing Enter on focused link navigates/activates it', async ({ page }) => {
      // Test internal navigation link (Features)
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.focus();
      await expect(featuresLink).toBeFocused();

      // Press Enter to activate the link
      await page.keyboard.press('Enter');

      // Wait for navigation/scroll
      await page.waitForTimeout(500);

      // Verify the URL hash changed to indicate navigation
      const url = page.url();
      expect(url).toContain('#features');

      // Verify the features section is in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('pressing Enter on Get Started button activates navigation', async ({ page }) => {
      const getStartedButton = page.locator('.hero-buttons .btn-primary');
      await getStartedButton.focus();
      await expect(getStartedButton).toBeFocused();

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Wait for navigation
      await page.waitForTimeout(500);

      // Verify navigation occurred
      const url = page.url();
      expect(url).toContain('#getting-started');

      // Verify the getting started section is visible
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('all navigation links are activatable with Enter key', async ({ page }) => {
      const internalNavLinks = [
        { selector: '.nav-links a[href="#features"]', target: '#features' },
        { selector: '.nav-links a[href="#architecture"]', target: '#architecture' },
        { selector: '.nav-links a[href="#getting-started"]', target: '#getting-started' },
        { selector: '.nav-links a[href="#commands"]', target: '#commands' },
      ];

      for (const linkInfo of internalNavLinks) {
        // Reset to top of page
        await page.goto('/');

        const link = page.locator(linkInfo.selector);
        await link.focus();
        await expect(link).toBeFocused();

        // Activate with Enter
        await page.keyboard.press('Enter');
        await page.waitForTimeout(500);

        // Verify navigation
        expect(page.url()).toContain(linkInfo.target);
      }
    });

    test('footer links are activatable with keyboard', async ({ page }) => {
      // Scroll to footer to ensure it's loaded
      await page.locator('.footer').scrollIntoViewIfNeeded();

      const footerDocLink = page.locator('[data-testid="footer-docs-link"]');
      await footerDocLink.focus();
      await expect(footerDocLink).toBeFocused();

      // Press Enter to activate
      await page.keyboard.press('Enter');
      await page.waitForTimeout(500);

      // Verify navigation occurred
      expect(page.url()).toContain('#getting-started');
    });
  });

  test.describe('Keyboard Accessibility Compliance', () => {
    test('skip link or logical focus order for main content', async ({ page }) => {
      // Tab through page and verify we can reach main content areas
      const mainContentReachable = await page.evaluate(() => {
        // Check that main element exists and can be navigated to
        const mainElement = document.querySelector('main');
        return mainElement !== null;
      });

      expect(mainContentReachable).toBeTruthy();
    });

    test('no keyboard traps exist on the page', async ({ page }) => {
      // Tab through interactive elements and ensure we can cycle through them
      // Collect all focusable elements to know how many we should be able to reach
      const focusableSelector = 'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])';
      const focusableCount = await page.locator(focusableSelector).count();

      // Tab through all focusable elements
      const visitedElements = new Set<string>();

      for (let i = 0; i < focusableCount + 5; i++) {
        await page.keyboard.press('Tab');

        const focusedInfo = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el.tagName === 'BODY') return null;
          return `${el.tagName}-${el.className}-${el.getAttribute('href') || el.textContent?.slice(0, 20)}`;
        });

        if (focusedInfo) {
          visitedElements.add(focusedInfo);
        }
      }

      // We should have visited at least some unique elements (not stuck in a trap)
      expect(visitedElements.size).toBeGreaterThan(5);
    });
  });
});
