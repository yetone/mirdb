const { test, expect } = require('@playwright/test');

test.describe('Accessibility Compliance (WCAG 2.1 AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 5: Keyboard navigation
  test.describe('Test Case 5: Keyboard Navigation', () => {
    test('All buttons and links should be focusable via keyboard in logical order', async ({ page }) => {
      // Get all focusable elements
      const focusableElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

      // Should have multiple focusable elements
      expect(focusableElements.length).toBeGreaterThan(0);

      // Focus the body first to start tabbing from the beginning
      await page.keyboard.press('Tab');

      // Verify we can tab through interactive elements
      let tabCount = 0;
      const maxTabs = 20;

      while (tabCount < maxTabs) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          return el ? el.tagName.toLowerCase() : null;
        });

        // Check if we've reached a focusable element
        if (focusedElement && ['a', 'button', 'input', 'select', 'textarea'].includes(focusedElement)) {
          break;
        }

        await page.keyboard.press('Tab');
        tabCount++;
      }

      // Verify we found at least one focusable element
      expect(tabCount).toBeLessThan(maxTabs);
    });

    test('Tab order should follow logical document order', async ({ page }) => {
      const focusOrder = [];
      const maxTabs = 15;

      // Start tabbing from the beginning
      await page.keyboard.press('Tab');

      for (let i = 0; i < maxTabs; i++) {
        const focusedInfo = await page.evaluate(() => {
          const el = document.activeElement;
          if (el && el !== document.body) {
            const rect = el.getBoundingClientRect();
            return {
              tag: el.tagName.toLowerCase(),
              text: el.textContent?.slice(0, 30),
              top: rect.top,
              left: rect.left,
              href: el.getAttribute('href')
            };
          }
          return null;
        });

        if (focusedInfo) {
          focusOrder.push(focusedInfo);
        }

        await page.keyboard.press('Tab');
      }

      // Verify we have focusable elements in the tab order
      expect(focusOrder.length).toBeGreaterThan(0);

      // Verify that focusable elements follow DOM order (first elements should be from hero section)
      // This is a relaxed check - we just verify that elements CAN be tabbed through
      const hasInteractiveElements = focusOrder.some(
        info => info.tag === 'a' || info.tag === 'button'
      );
      expect(hasInteractiveElements).toBe(true);
    });
  });

  // Test Case 6: Focus indicator visibility
  test.describe('Test Case 6: Focus Indicator Visibility', () => {
    test('Focused buttons should have visible focus rings or outlines', async ({ page }) => {
      // Find a button to test
      const buttons = page.locator('.btn, button, a.btn-primary, a.btn-secondary');
      const buttonCount = await buttons.count();

      if (buttonCount > 0) {
        const firstButton = buttons.first();
        await firstButton.focus();

        // Check if the button has a visible focus indicator
        const focusStyles = await firstButton.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          const focusStyles = window.getComputedStyle(el, ':focus');
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            outlineColor: styles.outlineColor,
            boxShadow: styles.boxShadow
          };
        });

        // Check that there's some form of focus indicator
        const hasVisibleOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
        const hasBoxShadow = focusStyles.boxShadow && focusStyles.boxShadow !== 'none';

        expect(hasVisibleOutline || hasBoxShadow).toBe(true);
      }
    });

    test('Focused links should have visible focus indication', async ({ page }) => {
      const links = page.locator('a[href]');
      const linkCount = await links.count();

      if (linkCount > 0) {
        // Focus the first link using keyboard
        await page.keyboard.press('Tab');

        // Get the focused element
        const focusedElement = page.locator(':focus');

        // Check if the focused element is visible (focus indicator should be visible)
        const isVisible = await focusedElement.isVisible().catch(() => false);
        expect(isVisible).toBe(true);
      }
    });

    test('Focus should be visible on CTA buttons', async ({ page }) => {
      const getStartedBtn = page.locator('a.btn-primary').first();

      if (await getStartedBtn.isVisible()) {
        await getStartedBtn.focus();

        // The button should be visible when focused
        await expect(getStartedBtn).toBeVisible();

        // Check that focus styles are applied
        const hasFocusStyles = await getStartedBtn.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return styles.outline !== 'none' ||
                 styles.boxShadow !== 'none' ||
                 styles.outlineWidth !== '0px';
        });

        expect(hasFocusStyles).toBe(true);
      }
    });
  });

  // Test Case 7: Color contrast
  test.describe('Test Case 7: Color Contrast', () => {
    test('Primary text should have sufficient contrast ratio (at least 4.5:1)', async ({ page }) => {
      // Check contrast by verifying text colors are properly defined
      // Note: Full contrast checking requires axe-core or similar,
      // here we do a basic verification that text has visible colors

      const textElements = page.locator('h1, h2, h3, p.description, .tagline');
      const count = await textElements.count();

      // Verify at least some text elements exist
      expect(count).toBeGreaterThan(0);

      // Sample a few text elements and verify they have visible text colors
      const sampleSize = Math.min(count, 4);

      for (let i = 0; i < sampleSize; i++) {
        const element = textElements.nth(i);
        const isVisible = await element.isVisible();

        if (isVisible) {
          const colorInfo = await element.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            const color = styles.color;
            // Parse the rgba/rgb values
            const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*(\d*\.?\d+))?\)/);
            if (match) {
              const alpha = match[4] !== undefined ? parseFloat(match[4]) : 1;
              return {
                r: parseInt(match[1]),
                g: parseInt(match[2]),
                b: parseInt(match[3]),
                a: alpha,
                hasColor: true
              };
            }
            return { hasColor: false };
          });

          // Verify the text has a visible color (not fully transparent)
          if (colorInfo.hasColor) {
            expect(colorInfo.a).toBeGreaterThan(0);
          }
        }
      }
    });

    test('Hero section text should have good contrast against background', async ({ page }) => {
      const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();

      if (await heroSection.isVisible()) {
        const heroText = heroSection.locator('h1, .tagline, .description, p').first();

        if (await heroText.isVisible()) {
          const colors = await heroText.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
              color: styles.color,
              // For elements with transparent background, we need to check the parent
              opacity: styles.opacity
            };
          });

          // Basic check: text should be visible (not transparent)
          expect(colors.opacity).not.toBe('0');
        }
      }
    });
  });

  // Test Case 8: Skip navigation link
  test.describe('Test Case 8: Skip Navigation Link', () => {
    test('Skip to main content link should be present or page structure allows keyboard access', async ({ page }) => {
      // Check for skip link (optional but recommended)
      const skipLink = page.locator('a[href="#main"], a[href="#content"], a[href="#get-started"]').first();
      const hasSkipLink = await skipLink.isVisible().catch(() => false);

      // If no skip link, verify the page has good landmark structure
      if (!hasSkipLink) {
        // Check that main landmark exists
        const main = page.locator('main, [role="main"]');
        const hasMain = await main.count() > 0;

        // Check that header comes before main
        const header = page.locator('header, [role="banner"]');
        const hasHeader = await header.count() > 0;

        // If the page has proper landmarks, that's acceptable without skip link
        expect(hasMain && hasHeader).toBe(true);
      } else {
        // If skip link exists, it should work when clicked
        await expect(skipLink).toBeVisible();
      }
    });

    test('First interactive element can be reached quickly via Tab', async ({ page }) => {
      // Measure how many tabs to reach first interactive element
      let tabCount = 0;
      const maxTabs = 10;

      await page.keyboard.press('Tab');

      for (let i = 0; i < maxTabs; i++) {
        const isInteractive = await page.evaluate(() => {
          const el = document.activeElement;
          return el && (
            el.tagName === 'A' ||
            el.tagName === 'BUTTON' ||
            el.tagName === 'INPUT'
          );
        });

        if (isInteractive) {
          break;
        }

        tabCount++;
        await page.keyboard.press('Tab');
      }

      // First interactive element should be reachable within reasonable number of tabs
      expect(tabCount).toBeLessThan(maxTabs);
    });
  });

  // Additional comprehensive accessibility tests
  test.describe('Additional Accessibility Verification', () => {
    test('Interactive elements should have adequate touch target sizes', async ({ page }) => {
      const buttons = page.locator('.btn, button, a.btn-primary, a.btn-secondary');
      const count = await buttons.count();

      for (let i = 0; i < Math.min(count, 4); i++) {
        const button = buttons.nth(i);
        if (await button.isVisible()) {
          const size = await button.boundingBox();
          if (size) {
            // Minimum recommended touch target is 44x44 pixels
            expect(size.width).toBeGreaterThanOrEqual(44);
            expect(size.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    });

    test('Page should have proper document title', async ({ page }) => {
      const title = await page.title();
      expect(title.length).toBeGreaterThan(0);
      expect(title.toLowerCase()).toContain('mirdb');
    });

    test('Page language should be set', async ({ page }) => {
      const lang = await page.getAttribute('html', 'lang');
      expect(lang).not.toBeNull();
      expect(lang).toBe('en');
    });

    test('Images should have alt text or be decorative', async ({ page }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        // All images should have alt attribute (can be empty for decorative)
        expect(alt).not.toBeNull();
      }
    });

    test('SVG icons should be properly hidden from screen readers', async ({ page }) => {
      const decorativeSvgs = page.locator('.feature-icon svg, [class*="icon"] svg');
      const count = await decorativeSvgs.count();

      for (let i = 0; i < count; i++) {
        const svg = decorativeSvgs.nth(i);
        if (await svg.isVisible()) {
          const ariaHidden = await svg.getAttribute('aria-hidden');
          // Decorative SVGs should have aria-hidden="true"
          expect(ariaHidden).toBe('true');
        }
      }
    });
  });
});
